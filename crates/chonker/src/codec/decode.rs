use crate::archive::{chunk::ArchiveChunk, meta::ChonkerArchiveMeta};
use positioned_io::ReadAt;

pub struct DecodeContext {
    meta_frame: rkyv::AlignedVec,
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
