pub struct DecodeContext {
    pub(crate) meta_frame: rkyv::AlignedVec,
}

#[allow(clippy::unused_async)]
pub(crate) async fn decode_chunks<R, W>(
    context: &mut DecodeContext,
    reader: R,
    writer: &mut W,
) -> crate::BoxResult<()> {
    Ok(())
}
