use std::{path::Path, sync::Arc};

pub(crate) async fn encode(path: &Path) -> crate::BoxResult<()> {
    let reader = chonker::io::fast_seq_open(path).await?;
    let reader_size = reader.metadata().await?.len();
    let context = Arc::new(chonker::EncodeContext {
        multi_progress: indicatif::MultiProgress::new().into(),
        ..Default::default()
    });
    let writer = &mut tokio::io::sink();
    chonker::ChonkerArchive::create(context, reader, Some(reader_size), writer).await?;
    Ok(())
}
