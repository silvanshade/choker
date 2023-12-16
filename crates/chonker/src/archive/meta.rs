use rkyv::bytecheck;
use smol_str::SmolStr;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{archive::chunk::ArchiveChunk, codec::encode::EncodeContext, io::AsZstdWriteBuf};

#[cfg_attr(feature = "debug", derive(Debug))]
#[derive(rkyv::Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, rkyv::CheckBytes))]
pub(crate) struct ArchiveMeta {
    pub(crate) application_version: SmolStr,
    pub(crate) source_size: u64,
    pub(crate) source_checksum: [u8; 32],
    pub(crate) source_orders: Vec<u64>,
    pub(crate) source_chunks: Vec<ArchiveChunk>,
}

impl Default for ArchiveMeta {
    fn default() -> Self {
        Self {
            application_version: env!("CARGO_PKG_VERSION").into(),
            source_checksum: [0; 32],
            source_size: 0,
            source_orders: Vec::default(),
            source_chunks: Vec::default(),
        }
    }
}

impl ArchiveMeta {
    pub(crate) async fn write<W>(&self, writer: &mut W) -> crate::BoxResult<()>
    where
        W: AsyncWrite + Unpin,
    {
        let context = EncodeContext::default();
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
            compressor.set_compression_level(i32::MAX)?;
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
