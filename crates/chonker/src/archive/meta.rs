use std::io::Read;

use positioned_io::{ReadAt, ReadBytesAtExt};
use rkyv::bytecheck;
use smol_str::SmolStr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::{
    archive::{chunk::ArchiveChunk, meta},
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
    pub(crate) fn read<'meta, R>(
        context: &mut DecodeContext,
        mut meta_frame: &'meta mut rkyv::AlignedVec,
        mut reader: R,
        reader_size: u64,
    ) -> crate::BoxResult<(std::io::Take<R>, u64, &'meta rkyv::Archived<ChonkerArchiveMeta>)>
    where
        R: positioned_io::ReadAt + std::io::Read + std::io::Seek + Unpin,
    {
        let meta_size = reader.read_u64_at::<byteorder::LittleEndian>(reader_size - 40)?;
        let meta_len = usize::try_from(meta_size)?;
        let meta_off = i64::try_from(meta_size)?;

        let expected_checksum = &mut [0u8; 32];
        reader.read_exact_at(reader_size - 32, expected_checksum)?;

        let meta_frame_compressed = &mut Vec::with_capacity(meta_len + 8);
        let mut meta_frame_reader =
            {
                reader.seek(std::io::SeekFrom::End(-40 - meta_off))?;
                reader.take(meta_size + 8)
            };
        let meta_frame_size = std::io::copy(&mut meta_frame_reader, meta_frame_compressed)?;
        debug_assert_eq!(meta_frame_size, meta_size + 8);

        let actual_checksum = blake3::hash(meta_frame.as_slice());
        debug_assert_eq!(actual_checksum.as_bytes(), expected_checksum);

        let content_size = zstd_safe::get_frame_content_size(meta_frame_compressed)
            .map_err(|err| err.to_string())?
            .ok_or("Failed to get compressed size of meta frame")?;
        meta_frame.reserve_exact(usize::try_from(content_size)? - meta_frame.len());

        let mut decompressor = zstd::stream::read::Decoder::new(&meta_frame_compressed[..])?;
        context.configure_zstd_stream_decompressor(&mut decompressor)?;

        let decompressed_size = std::io::copy(&mut decompressor, &mut meta_frame)?;
        debug_assert_eq!(decompressed_size, content_size);

        let meta = rkyv::check_archived_root::<ChonkerArchiveMeta>(&meta_frame.as_slice()[.. meta_frame.len() - 8])
            .map_err(|err| err.to_string())?;

        let mut reader = meta_frame_reader.into_inner();
        reader.seek(std::io::SeekFrom::Start(32))?;
        let reader = reader.take(reader_size - 16 - 2 - 14 - meta_size - 8 - 32);

        Ok((reader, meta_size, meta))
    }

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
            context.configure_zstd_bulk_compressor(&mut compressor)?;
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
