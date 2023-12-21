use rkyv::AlignedVec;

use crate::archive::meta::ChonkerArchiveMeta;

pub struct DecodeContext {
    meta_frame: AlignedVec,
}

#[allow(clippy::unused_async)]
pub(crate) async fn decode_chunks<R, W>(
    context: &mut DecodeContext,
    meta: &rkyv::Archived<ChonkerArchiveMeta>,
    reader: R,
    writer: &mut W,
) -> crate::BoxResult<()> {
    // meta.
    Ok(())
}
