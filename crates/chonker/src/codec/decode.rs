use crate::archive::{chunk::ArchiveChunk, meta::ChonkerArchiveMeta};
use positioned_io::ReadAt;

pub struct DecodeContext {
    meta_frame: rkyv::AlignedVec,
}

impl DecodeContext {
    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    pub(crate) fn configure_zstd_bulk_decompressor(
        &self,
        decompressor: &mut zstd::bulk::Decompressor,
    ) -> crate::BoxResult<()> {
        Ok(())
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    pub(crate) fn configure_zstd_stream_decompressor<R>(
        &self,
        decompressor: &mut zstd::stream::read::Decoder<R>,
    ) -> crate::BoxResult<()>
    where
        R: std::io::BufRead,
    {
        Ok(())
    }
}

#[allow(clippy::unused_async)]
pub(crate) async fn decode_chunks<R, W>(
    context: &mut DecodeContext,
    meta: &rkyv::Archived<ChonkerArchiveMeta>,
    reader: R,
    writer: &mut W,
) -> crate::BoxResult<()> {
    for chunk in meta.source_chunks.iter() {
        match chunk {
            rkyv::Archived::<ArchiveChunk>::Data { hash, length, offset } => {
                //
            },
            rkyv::Archived::<ArchiveChunk>::Dupe { pointer } => {
                //
            },
        }
    }

    Ok(())
}
