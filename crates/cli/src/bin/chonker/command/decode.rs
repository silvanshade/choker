use std::{path::Path, sync::Arc};

use chonker::DecodeContext;

pub(crate) async fn decode(meta_reader_path: &Path) -> crate::BoxResult<()> {
    // FIXME: fix path calculation
    let Some(data_reader_path) = meta_reader_path.parent().and_then(|parent| {
        meta_reader_path
            .file_stem()
            .map(|file_stem| parent.join(file_stem).with_extension("chonk.data"))
    }) else {
        return Err("invalid file name".into());
    };

    let Some(file_writer_path) = meta_reader_path.parent().and_then(|parent| {
        meta_reader_path
            .file_stem()
            .map(|file_stem| parent.join(file_stem).with_extension("tmp"))
    }) else {
        return Err("invalid file name".into());
    };

    let meta_file = chonker::util::io::fast_seq_open(&meta_reader_path).await?;
    let data_file = chonker::util::io::fast_seq_open(&data_reader_path).await?;
    let data_reader_size = data_file.metadata().await?.len().into();
    let meta_reader = tokio::io::BufReader::new(meta_file);
    let data_reader = tokio::io::BufReader::new(data_file);

    let report_progress = true;
    let context = Arc::new(DecodeContext::new(data_reader_size, report_progress)?);
    context
        .progress()
        .map(|progress| {
            let msg_cache = "\n    cache: N/A";
            let msg = format!(
                "decoding ::\n     meta: {}\n     data: {}\n   target: {}{msg_cache}\n  threads: {}",
                meta_reader_path.display(),
                data_reader_path.display(),
                file_writer_path.display(),
                std::thread::available_parallelism()?,
            );
            progress.println(msg)
        })
        .transpose()?;

    let file_writer = tokio::fs::File::create(file_writer_path).await?;
    let file_writer = tokio::io::BufWriter::new(file_writer);

    chonker::ChonkerArchive::decode(context, meta_reader, data_reader, file_writer).await?;

    Ok(())
}
