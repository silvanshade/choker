use std::path::Path;

use chonker::DecodeContext;

pub(crate) async fn decode(reader_path: &Path) -> crate::BoxResult<()> {
    let Some(writer_path) = reader_path.parent().and_then(|parent| {
        reader_path
            .file_stem()
            .map(|file_stem| parent.join(file_stem).with_extension("tmp"))
    }) else {
        return Err("invalid file name".into());
    };
    let reader = &mut tokio::task::block_in_place(|| chonker::io::std_fast_seq_open(reader_path))?;
    let reader_size = tokio::task::block_in_place(|| reader.metadata().map(|md| md.len()))?;
    let context = DecodeContext::new();
    let meta_frame = &mut rkyv::AlignedVec::new();
    let writer = &mut std::fs::File::create(writer_path)?;
    chonker::ChonkerArchive::decode(context, meta_frame, reader, reader_size, writer).await?;
    Ok(())
}
