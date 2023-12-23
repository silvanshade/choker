use std::path::Path;

use chonker::EncodeContext;

pub(crate) async fn encode(path: &Path) -> crate::BoxResult<()> {
    let reader = chonker::io::tokio_fast_seq_open(path).await?;
    let reader_size = reader.metadata().await?.len();
    let context = EncodeContext::new(reader_size, true)?.into();
    let writer = &mut tokio::fs::File::create(path.with_extension("chonk")).await?;
    chonker::ChonkerArchive::encode(context, reader, Some(reader_size), writer).await?;
    Ok(())
}
