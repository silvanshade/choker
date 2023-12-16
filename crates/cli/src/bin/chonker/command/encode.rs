use std::path::Path;

use chonker::EncodeContext;

pub(crate) async fn encode(path: &Path) -> crate::BoxResult<()> {
    let file = chonker::util::io::fast_seq_open(path).await?;
    let reader_size = file.metadata().await?.len().into();
    let reader = tokio::io::BufReader::new(file);

    let prev_ext = path
        .extension()
        .and_then(|ext| ext.to_str().map(|ext| format!("{ext}.")))
        .unwrap_or_default();

    let meta_file_path = path.with_extension(format!("{prev_ext}chonk.meta"));
    let data_file_path = path.with_extension(format!("{prev_ext}chonk.data"));

    let meta_file = tokio::fs::File::create(&meta_file_path).await?;
    let meta_writer = tokio::io::BufWriter::new(meta_file);
    let data_file = tokio::fs::File::create(&data_file_path).await?;
    let data_writer = tokio::io::BufWriter::new(data_file);

    let report_progress = false;
    let context = EncodeContext::new(reader_size, report_progress)?.into();

    chonker::ChonkerArchive::encode(context, reader, reader_size, meta_writer, data_writer).await?;

    Ok(())
}
