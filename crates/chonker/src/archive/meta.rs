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
    pub(crate) app_version: SmolStr,
    pub(crate) src_size: u64,
    pub(crate) src_checksum: [u8; 32],
    pub(crate) src_chunks: Vec<ArchiveChunk>,
}

impl Default for ChonkerArchiveMeta {
    fn default() -> Self {
        Self {
            app_version: env!("CARGO_PKG_VERSION").into(),
            src_checksum: [0; 32],
            src_size: 0,
            src_chunks: Vec::default(),
        }
    }
}

impl ChonkerArchiveMeta {
    pub(crate) fn read<'meta, R>(
        context: &mut DecodeContext,
        mut meta_bytes: &'meta mut rkyv::AlignedVec,
        mut reader: R,
        reader_size: u64,
    ) -> crate::BoxResult<(R, &'meta rkyv::Archived<ChonkerArchiveMeta>, u64)>
    where
        R: positioned_io::ReadAt + std::io::Read + std::io::Seek + Unpin,
    {
        // Read [-40 .. -32] to get the meta frame size.
        let meta_size = reader.read_u64_at::<byteorder::LittleEndian>(reader_size - 40)?;
        let meta_len = usize::try_from(meta_size)?;
        let meta_off = i64::try_from(meta_size)?;

        // Read [-32 ..] to get the meta frame checksum.
        let expected_checksum = &mut [0u8; 32];
        reader.read_exact_at(reader_size - 32, expected_checksum)?;

        // NOTE: We initially read the meta frame size as part of the frame data since this must be
        // included in the checksum. After we verify the checksum, we truncate the meta frame to
        // just the rkyv serialized bytes.
        //
        // Read [-40 - meta .. -32] to get the meta frame.
        let meta_frame = &mut rkyv::AlignedVec::with_capacity(meta_len + 8);

        let mut meta_frame_reader =
            {
                reader.seek(std::io::SeekFrom::End(-40 - meta_off))?;
                reader.take(meta_size + 8)
            };
        let meta_frame_read = std::io::copy(&mut meta_frame_reader, meta_frame)?;
        assert_eq!(meta_frame_read, u64::try_from(meta_frame.len())?);

        // Verify the checksum of the meta frame.
        let actual_checksum = blake3::hash(meta_frame.as_slice());
        assert_eq!(actual_checksum.as_bytes(), expected_checksum);

        // NOTE: Now that we have verified the checksum, truncate the frame (dropping the size u64),
        // leaving just the rkyv serialized bytes.
        meta_frame.resize(meta_len, u8::default());
        assert_eq!(meta_size, u64::try_from(meta_frame.len())?);

        // Extract the meta frame (zstd) content size.
        let meta_frame_content_size = zstd_safe::get_frame_content_size(meta_frame)
            .map_err(|err| err.to_string())?
            .ok_or("Failed to get compressed size of meta frame")?;
        meta_bytes.reserve_exact(usize::try_from(meta_frame_content_size)? - meta_bytes.len());

        // Decompress the meta frame.
        let mut decompressor = zstd::stream::read::Decoder::new(meta_frame.as_slice())?;
        context.configure_zstd_stream_decompressor(&mut decompressor)?;

        // Read the decompressed meta frame into the meta bytes buffer.
        let meta_bytes_read = std::io::copy(&mut decompressor, &mut meta_bytes)?;
        assert_eq!(meta_bytes_read, meta_frame_content_size);

        // Zero-copy convert the meta bytes into the rkyv::Archived<ChonkerArchiveMeta>.
        let meta =
            rkyv::check_archived_root::<ChonkerArchiveMeta>(meta_bytes.as_slice()).map_err(|err| err.to_string())?;

        // // Prepare the reader for reading the archive source chunks.
        // let reader = meta_frame_reader.into_inner();
        // reader.seek(std::io::SeekFrom::Start(32))?;
        // let reader = reader.take(reader_size - 16 - 2 - 14 - meta_size - 8 - 32);

        Ok((meta_frame_reader.into_inner(), meta, meta_size))
    }

    pub(crate) async fn write<W>(&self, context: &EncodeContext, mut writer: W) -> crate::BoxResult<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
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
            u64::try_from(size)?
        };
        let mut hasher = blake3::Hasher::new();

        writer.write_all(compressed.as_slice()).await?;
        hasher.update(compressed.as_slice());

        writer.write_all(&size.to_le_bytes()).await?;
        hasher.update(&size.to_le_bytes());

        let checksum = hasher.finalize();
        writer.write_all(checksum.as_bytes()).await?;

        Ok(())
    }
}
