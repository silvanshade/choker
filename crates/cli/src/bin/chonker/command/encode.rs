use std::path::Path;

use chonker::EncodeContext;

pub(crate) async fn encode(path: &Path) -> crate::BoxResult<()> {
    let reader = chonker::io::tokio_fast_seq_open(path).await?;
    let reader_size = reader.metadata().await?.len();
    let context = EncodeContext::new(reader_size, true)?.into();
    let prev_ext = path
        .extension()
        .and_then(|ext| ext.to_str().map(|ext| format!("{ext}.")))
        .unwrap_or_default();
    let writer = &mut tokio::fs::File::create(path.with_extension(format!("{prev_ext}chonk"))).await?;
    chonker::ChonkerArchive::encode(context, reader, Some(reader_size), writer).await?;
    Ok(())
}
