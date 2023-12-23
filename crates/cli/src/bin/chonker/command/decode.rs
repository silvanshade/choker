use std::path::Path;

use chonker::DecodeContext;

pub(crate) fn decode(reader_path: &Path) -> crate::BoxResult<()> {
    let Some(writer_path) = reader_path.file_stem() else {
        return Err("invalid file name".into());
    };
    let reader = &mut chonker::io::std_fast_seq_open(reader_path)?;
    let reader_size = reader.metadata()?.len();
    let context = DecodeContext::new();
    let meta_frame = &mut rkyv::AlignedVec::new();
    let writer = &mut std::fs::File::create(writer_path)?;
    chonker::ChonkerArchive::decode(context, meta_frame, reader, reader_size, writer)?;
    Ok(())
}
