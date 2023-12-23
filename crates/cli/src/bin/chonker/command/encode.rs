use std::{path::Path, sync::Arc};

use chonker::EncodeContext;

pub(crate) async fn encode(path: &Path) -> crate::BoxResult<()> {
    let reader = chonker::io::fast_seq_open(path).await?;
    let reader_size = reader.metadata().await?.len();
    let mut context = EncodeContext::new(reader_size);
    context.multi_progress = indicatif::MultiProgress::new().into();
    let context = Arc::new(context);
    let writer = &mut tokio::fs::File::create(path.with_extension("chonker")).await?;
    chonker::ChonkerArchive::encode(context, reader, Some(reader_size), writer).await?;
    Ok(())
}
