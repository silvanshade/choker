use bytes::Bytes;
use positioned_io::ReadAt;
use std::io::prelude::Write;

use crate::archive::{chunk::ArchiveChunk, meta::ChonkerArchiveMeta};

#[derive(Default)]
pub struct DecodeContext {}

impl DecodeContext {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    pub(crate) fn configure_zstd_bulk_decompressor(
        &self,
        _decompressor: &mut zstd::bulk::Decompressor,
    ) -> crate::BoxResult<()> {
        Ok(())
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    pub(crate) fn configure_zstd_stream_decompressor<R>(
        &self,
        _decompressor: &mut zstd::stream::read::Decoder<R>,
    ) -> crate::BoxResult<()>
    where
        R: std::io::BufRead,
    {
        Ok(())
    }
}

#[allow(clippy::needless_pass_by_value, clippy::unused_async)]
pub(crate) async fn decode_chunks<R, W>(
    context: DecodeContext,
    meta: &rkyv::Archived<ChonkerArchiveMeta>,
    meta_size: u64,
    reader: R,
    writer: &mut W,
) -> crate::BoxResult<()>
where
    R: positioned_io::ReadAt + std::io::Read + std::io::Seek,
    W: std::io::Write,
{
    let mut decompressor = zstd::bulk::Decompressor::new()?;
    let mut src_data = Vec::new();
    let mut arc_data = Vec::new();
    context.configure_zstd_bulk_decompressor(&mut decompressor)?;

    let mut chunks = meta.src_chunks.iter();
    let mut dupes: Vec<&rkyv::Archived<ArchiveChunk>> = vec![];

    let (tx_hasher_update, mut rx_hasher_update) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    let hasher = tokio::task::spawn(async move {
        let mut hasher = blake3::Hasher::new();
        while let Some(data) = rx_hasher_update.recv().await {
            hasher.update(&data);
        }
        hasher.finalize()
    });

    while let Some(chunk) = dupes.pop().or_else(|| chunks.next()) {
        match chunk {
            rkyv::Archived::<ArchiveChunk>::Data {
                checksum,
                src_offset,
                src_length,
                arc_offset,
                arc_length,
            } => {
                let arc_len = usize::try_from(u32::from(arc_length))?;
                if arc_len > arc_data.len() {
                    arc_data.resize(arc_len, 0);
                }
                reader.read_exact_at(u64::from(arc_offset), &mut arc_data[.. arc_len])?;

                let src_len = usize::try_from(u32::from(src_length))?;
                if src_len > src_data.len() {
                    src_data.resize(src_len, 0);
                }
                decompressor.decompress_to_buffer(&arc_data[.. arc_len], &mut src_data[.. src_len])?;

                assert_eq!(checksum, blake3::hash(&src_data[.. src_len]).as_bytes());
                tx_hasher_update.send(Bytes::copy_from_slice(&src_data[.. src_len]))?;

                writer.write_all(&src_data[.. src_len])?;
            },
            rkyv::Archived::<ArchiveChunk>::Dupe { index } => {
                let index = usize::try_from(u64::from(*index))?;
                dupes.push(&meta.src_chunks[index]);
            },
        }
    }
    drop(tx_hasher_update);

    assert_eq!(&meta.src_checksum, hasher.await?.as_bytes());

    Ok(())
}
