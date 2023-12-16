use core::pin::Pin;
use std::io::{prelude::Write, Read};

use positioned_io::{ReadAt, ReadBytesAtExt};
use rkyv::bytecheck;
use smol_str::SmolStr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::{
    archive::{chunk::ArchiveChunk, meta},
    codec::{decode::DecodeContext, encode::context::EncodeContext},
    util::io::AsZstdWriteBuf,
    BoxResult,
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
    pub(crate) async fn read<'meta, MR>(
        context: &DecodeContext,
        mut meta_reader: MR,
    ) -> crate::BoxResult<OwningChonkerArchiveMeta>
    where
        MR: tokio::io::AsyncRead + Unpin,
    {
        // Read the meta frame checksum.
        let expected_checksum = &mut [0u8; 32];
        meta_reader.read_exact(expected_checksum).await?;

        // Read the meta frame.
        let mut meta_frame = Vec::new();
        meta_reader.read_to_end(&mut meta_frame).await?;

        // Verify the checksum of the meta frame.
        let actual_checksum = blake3::hash(meta_frame.as_slice());
        assert_eq!(actual_checksum.as_bytes(), expected_checksum);

        // Extract the meta frame (zstd) content size.
        let meta_frame_content_size = zstd_safe::get_frame_content_size(&meta_frame)
            .map_err(|err| err.to_string())?
            .ok_or("Failed to get compressed size of meta frame")?;

        // Resize the meta_bytes buffer to fit the decompressed meta frame.
        let mut meta_bytes = rkyv::AlignedVec::new();
        meta_bytes.reserve_exact(usize::try_from(meta_frame_content_size)? - meta_bytes.capacity());

        // Decompress the meta frame.
        let mut decompressor = zstd::stream::read::Decoder::new(meta_frame.as_slice())?;
        context.configure_zstd_stream_decompressor(&mut decompressor)?;

        // Read the decompressed meta frame into the meta bytes buffer.
        let meta_bytes_read = std::io::copy(&mut decompressor, &mut meta_bytes)?;
        assert_eq!(meta_bytes_read, meta_frame_content_size);

        // Zero-copy convert the meta bytes into the rkyv::Archived<ChonkerArchiveMeta>.
        let meta = OwningChonkerArchiveMeta::new(meta_bytes)?;

        Ok(meta)
    }

    pub(crate) async fn write<W>(&self, context: &EncodeContext, mut meta_writer: W) -> crate::BoxResult<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let meta = rkyv::to_bytes::<Self, 0>(self)?;
        let mut meta_frame = rkyv::AlignedVec::with_capacity(std::cmp::max(64, meta.len() + 16 - meta.len() % 16));

        let mut compressor = {
            let mut compressor = zstd::bulk::Compressor::default();
            context.configure_zstd_bulk_compressor(&mut compressor)?;
            compressor
                .context_mut()
                .set_pledged_src_size(Some(u64::try_from(meta.len())?))
                .unwrap();
            compressor.multithread(u32::try_from(usize::from(std::thread::available_parallelism()?))?)?;
            compressor
        };
        let mut buffer = meta_frame.as_zstd_write_buf();
        compressor.compress_to_buffer(&meta, &mut buffer)?;

        let checksum = blake3::hash(meta_frame.as_slice());
        meta_writer.write_all(checksum.as_bytes()).await?;
        meta_writer.write_all(meta_frame.as_slice()).await?;

        Ok(())
    }
}

pub struct OwningChonkerArchiveMeta {
    inner: Pin<Box<OwningChonkerArchiveMetaInner>>,
}
// NOTE: Safe because the backing bytes for the inner pointer are Send.
unsafe impl Send for OwningChonkerArchiveMeta {
}
// NOTE: Safe because the backing bytes for the inner pointer are Sync.
unsafe impl Sync for OwningChonkerArchiveMeta {
}

impl AsRef<rkyv::Archived<ChonkerArchiveMeta>> for OwningChonkerArchiveMeta {
    // NOTE: Safe because the inner pointer is always non-null by construction.
    fn as_ref(&self) -> &rkyv::Archived<ChonkerArchiveMeta> {
        unsafe { &*self.inner.inner }
    }
}

impl core::ops::Deref for OwningChonkerArchiveMeta {
    type Target = rkyv::Archived<ChonkerArchiveMeta>;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl OwningChonkerArchiveMeta {
    pub fn new(bytes: rkyv::AlignedVec) -> BoxResult<Self> {
        let mut inner = Box::pin(OwningChonkerArchiveMetaInner {
            bytes,
            inner: core::ptr::null(),
        });
        let repr = rkyv::check_archived_root::<ChonkerArchiveMeta>(&inner.bytes).map_err(|err| err.to_string())?;
        inner.as_mut().inner = repr;
        Ok(Self { inner })
    }
}

pub struct OwningChonkerArchiveMetaInner {
    bytes: rkyv::AlignedVec,
    inner: *const rkyv::Archived<ChonkerArchiveMeta>,
}

impl core::ops::Deref for OwningChonkerArchiveMetaInner {
    type Target = rkyv::Archived<ChonkerArchiveMeta>;

    fn deref(&self) -> &Self::Target {
        // NOTE: Safe because the inner pointer is always non-null by construction.
        unsafe { &*self.inner }
    }
}
