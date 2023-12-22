use positioned_io::{ReadAt, ReadBytesAtExt};
use rkyv::bytecheck;
use smol_str::SmolStr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::{
    archive::chunk::ArchiveChunk,
    codec::{decode::DecodeContext, encode::EncodeContext},
    io::AsZstdWriteBuf,
};

#[cfg_attr(feature = "debug", derive(Debug))]
#[derive(rkyv::Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, rkyv::CheckBytes))]
pub struct ChonkerArchiveMeta {
    pub(crate) application_version: SmolStr,
    pub(crate) source_size: u64,
    pub(crate) source_checksum: [u8; 32],
    pub(crate) source_chunks: Vec<ArchiveChunk>,
}

impl Default for ChonkerArchiveMeta {
    fn default() -> Self {
        Self {
            application_version: env!("CARGO_PKG_VERSION").into(),
            source_checksum: [0; 32],
            source_size: 0,
            source_chunks: Vec::default(),
        }
    }
}

impl ChonkerArchiveMeta {
    //     pub(crate) fn read<'meta, R>(
    //         context: &mut DecodeContext,
    //         meta_frame: &'meta mut rkyv::AlignedVec,
    //         reader: &mut R,
    //         reader_size: u64,
    //     ) -> crate::BoxResult<(u64, &'meta rkyv::Archived<ChonkerArchiveMeta>)>
    //     where
    //         R: positioned_io::ReadAt,
    //     {
    //         let meta_size = reader.read_u64_at(reader_size - 40)?;
    //         let meta_len = usize::try_from(meta_size)?;
    //         let meta_off = i64::try_from(meta_size)?;

    //         let expected_checksum = &mut [0u8; 32];
    //         reader.read_exact_at(reader_size - 32, expected_checksum)?;

    //         // reader.seek(std::io::SeekFrom::End(-40 - meta_off)).await?;

    //         assert_eq!(meta_frame.len(), 0);
    //         meta_frame.reserve_exact(meta_len + 8 - meta_frame.len());
    //         let mut meta_frame_reader = tokio_util::io::SyncIoBridge::new(reader.take(meta_size + 8));
    //         let meta_frame_len =
    //             tokio::task::block_in_place(|| -> crate::BoxResult<usize> {
    //                 meta_frame
    //                     .extend_from_reader(&mut meta_frame_reader)
    //                     .map_err(Into::into)
    //             })?;
    //         debug_assert_eq!(meta_frame_len, meta_len + 8);

    //         let actual_checksum = blake3::hash(meta_frame.as_slice());
    //         assert_eq!(actual_checksum.as_bytes(), expected_checksum);

    //         // FIXME: decompress first
    //         let meta = rkyv::check_archived_root::<ChonkerArchiveMeta>(&meta_frame[.. meta_frame.len() - 8])
    //             .map_err(|err| err.to_string())?;

    //         Ok((meta_size, meta))
    //     }

    pub(crate) async fn write<W>(&self, context: &EncodeContext, mut writer: W) -> crate::BoxResult<()>
    where
        W: AsyncWrite + Unpin,
    {
        let mut hasher = blake3::Hasher::new();

        let metadata = rkyv::to_bytes::<Self, 0>(self)?;
        let mut compressed =
            rkyv::AlignedVec::with_capacity(std::cmp::max(64, metadata.len() + 16 - metadata.len() % 16));
        let size = {
            let n_workers = u32::try_from(usize::from(std::thread::available_parallelism()?))?;
            let mut compressor = zstd::bulk::Compressor::default();
            context.configure_zstd_compressor(&mut compressor)?;
            compressor
                .context_mut()
                .set_pledged_src_size(Some(u64::try_from(metadata.len())?))
                .unwrap();
            compressor.multithread(n_workers)?;
            let mut buffer = compressed.as_zstd_write_buf();
            let size = compressor.compress_to_buffer(&metadata, &mut buffer)?;
            let size = u64::try_from(size)?;
            let size = rkyv::rend::u64_le::from(size);
            rkyv::to_bytes::<rkyv::rend::u64_le, 0>(&size)?
        };
        debug_assert_eq!(size.len(), core::mem::size_of::<u64>());

        writer.write_all(compressed.as_slice()).await?;
        hasher.update(compressed.as_slice());

        writer.write_all(size.as_slice()).await?;
        hasher.update(size.as_slice());

        let checksum = hasher.finalize();
        writer.write_all(checksum.as_bytes()).await?;

        Ok(())
    }
}
