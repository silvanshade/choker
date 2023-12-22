use dashmap::DashMap;
use indicatif::MultiProgress;
use std::{cell::RefCell, sync::Arc};
use thread_local::ThreadLocal;
use tokio::io::AsyncWriteExt;

use crate::{
    archive::{chunk::ArchiveChunk, meta::ChonkerArchiveMeta},
    cdc::AsyncStreamChunker,
};

pub struct EncodeContext {
    pub cdc_min_chunk_size: u32,
    pub cdc_avg_chunk_size: u32,
    pub cdc_max_chunk_size: u32,
    pub zstd_compression_level: i32,
    pub multi_progress: Option<MultiProgress>,
}

impl Default for EncodeContext {
    fn default() -> Self {
        Self {
            cdc_min_chunk_size: Self::FASTCDC_MIN_CHUNK_SIZE,
            cdc_avg_chunk_size: Self::FASTCDC_AVG_CHUNK_SIZE,
            cdc_max_chunk_size: Self::FASTCDC_MAX_CHUNK_SIZE,
            zstd_compression_level: Self::ZSTD_COMPRESSION_LEVEL,
            multi_progress: None,
        }
    }
}

impl EncodeContext {
    const FASTCDC_MIN_CHUNK_SIZE: u32 = 1024;
    const FASTCDC_AVG_CHUNK_SIZE: u32 = 131_072;
    const FASTCDC_MAX_CHUNK_SIZE: u32 = 524_288;

    const ZSTD_COMPRESSION_LEVEL: i32 = -5;
    const ZSTD_INCLUDE_CHECKSUM: bool = false;
    const ZSTD_INCLUDE_DICTID: bool = false;

    pub(crate) fn configure_zstd_bulk_compressor(
        &self,
        compressor: &mut zstd::bulk::Compressor,
    ) -> crate::BoxResult<()> {
        compressor.set_compression_level(self.zstd_compression_level)?;
        compressor.include_checksum(Self::ZSTD_INCLUDE_CHECKSUM)?;
        compressor.include_dictid(Self::ZSTD_INCLUDE_DICTID)?;
        Ok(())
    }

    // #[allow(clippy::unused_self)]
    // fn configure_zstd_decompressor(&self, decompressor: &mut zstd::bulk::Decompressor) -> crate::BoxResult<()> {
    //     decompressor.include_magicbytes(Self::ZSTD_INCLUDE_MAGICBYTES)?;
    //     Ok(())
    // }

    // fn chunker_from_slice<'data>(&self, slice: &'data [u8]) -> Chunker<'data> {
    //     Chunker::new(self, slice)
    // }

    fn async_stream_chunker<R>(&self, source: R) -> AsyncStreamChunker<R>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        AsyncStreamChunker::new(self, source)
    }

    fn estimated_chunk_count(&self, source_size: u64) -> crate::BoxResult<usize> {
        let source_size = usize::try_from(source_size)?;
        let average = usize::try_from(self.cdc_avg_chunk_size)?;
        let estimated_count = source_size / average;
        // NOTE: better to overallocate and truncate than to underallocate and reallocate
        Ok(2 * estimated_count)
    }
}

struct ThreadLocalState {
    compressor: zstd::bulk::Compressor<'static>,
    chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, EncodedChunkResult)>,
}

impl ThreadLocalState {
    fn new(
        context: &EncodeContext,
        chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, EncodedChunkResult)>,
    ) -> crate::BoxResult<Self> {
        let mut compressor = zstd::bulk::Compressor::default();
        context.configure_zstd_bulk_compressor(&mut compressor)?;
        Ok(Self { compressor, chunks_tx })
    }
}

pub(crate) enum EncodedChunkResult {
    Data {
        checksum: [u8; 32],
        src_length: u64,
        src_offset: u64,
        compressed: Vec<u8>,
    },
    Dupe {
        index: u64,
    },
}

fn process_one_chunk(
    context: Arc<EncodeContext>,
    thread_local: Arc<ThreadLocal<RefCell<ThreadLocalState>>>,
    chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, EncodedChunkResult)>,
    processed: Arc<DashMap<[u8; 32], usize>>,
    ordinal: usize,
    chunk: fastcdc::v2020::ChunkData,
) -> impl FnOnce(&rayon::Scope) -> crate::BoxResult<()> {
    move |_scope| {
        let state = thread_local.get_or(|| {
            let state = ThreadLocalState::new(&context, chunks_tx.clone()).unwrap();
            RefCell::new(state)
        });
        state
            .borrow_mut()
            .compressor
            .context_mut()
            .set_pledged_src_size(Some(u64::try_from(chunk.length)?))
            .map_err(|err| format!("set_pleged_src_size failed: {err}"))?;

        let data = chunk.data;
        let checksum = <[u8; 32]>::from(blake3::hash(data.as_slice()));

        if let Some(unique) = processed.get(&checksum) {
            state.borrow_mut().chunks_tx.send((ordinal, EncodedChunkResult::Dupe {
                index: u64::try_from(*unique)?,
            }))?;
        } else {
            processed.insert(checksum, ordinal);
            let src_offset = chunk.offset;
            let src_length = u64::try_from(chunk.length)?;
            let compressed = state.borrow_mut().compressor.compress(data.as_slice())?;
            state.borrow_mut().chunks_tx.send((ordinal, EncodedChunkResult::Data {
                checksum,
                src_length,
                src_offset,
                compressed,
            }))?;
        }

        Ok(())
    }
}

fn process_all_chunks<R>(
    context: Arc<EncodeContext>,
    reader: R,
    thread_local: Arc<ThreadLocal<RefCell<ThreadLocalState>>>,
    encoded_chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, EncodedChunkResult)>,
) -> impl FnOnce(&rayon::Scope) -> crate::BoxResult<blake3::Hash>
where
    R: tokio::io::AsyncRead + Unpin,
{
    move |scope| {
        let processed = Arc::new(DashMap::<[u8; 32], usize>::new());

        let mut chunker = context.async_stream_chunker(reader);
        let stream = chunker.as_stream();
        let stream = core::pin::pin!(stream);
        let chunker = futures::executor::block_on_stream(stream);

        let mut hasher = blake3::Hasher::new();

        for (ordinal, result) in chunker.enumerate() {
            let chunk = result?;
            // NOTE: We could use `update_rayon` here, but since we're already maxing out the
            // ThreadPool with other chunk processing jobs, it's actually less efficient due to
            // resource contention.
            //
            // NOTE: Theoretically, we could avoid some work by reusing the BLAKE3 chunk states:
            //
            // - First, create a chunk state to calculate the individual hash for the CDC chunk.
            // - Next, reuse the same chunk state to calculate the CV and update the CV stack.
            // - Finally, once all individual CDC chunks are processed, finalize the root.
            //
            // However, in practice this seems to be slower (around 25% on M3 Max). Should revisit.
            hasher.update(chunk.data.as_slice());
            let context = context.clone();
            let thread_local = thread_local.clone();
            let chunks_tx = encoded_chunks_tx.clone();
            let processed = processed.clone();
            scope.spawn(move |scope| {
                process_one_chunk(context, thread_local, chunks_tx, processed, ordinal, chunk)(scope).unwrap();
            });
        }

        Ok(hasher.finalize())
    }
}

pub(crate) async fn encode_chunks<R, W>(
    context: Arc<EncodeContext>,
    reader: R,
    reader_size: Option<u64>,
    writer: &mut W,
) -> crate::BoxResult<ChonkerArchiveMeta>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin,
{
    let (encoded_chunks_tx, mut encoded_chunks_rx) =
        tokio::sync::mpsc::unbounded_channel::<(usize, EncodedChunkResult)>();

    let src_checksum = tokio::task::spawn_blocking({
        let context = context.clone();
        move || {
            let thread_local = Arc::new(thread_local::ThreadLocal::new());
            let pool = rayon::ThreadPoolBuilder::new().build()?;
            let checksum = pool.in_place_scope(process_all_chunks(context, reader, thread_local, encoded_chunks_tx))?;
            Ok::<_, crate::BoxError>(checksum)
        }
    });

    // Try to estimate a reasonable count for the cdc chunks.
    let estimated_chunk_count = reader_size
        .map(|source_size| context.estimated_chunk_count(source_size))
        .transpose()?
        .unwrap_or(2048);

    // Remember the actual chunk count.
    let mut actual_chunk_count = 0;

    let mut src_pos = 0;
    let mut arc_pos = 0;

    // Allocate the source chunks vec with the estimated length to avoid reallocations.
    let mut src_chunks = vec![ArchiveChunk::default(); estimated_chunk_count];

    let pb = match (reader_size, context.multi_progress.as_ref()) {
        (Some(size), Some(mp)) => {
            let pb = mp.add(indicatif::ProgressBar::new(size));
            let style = indicatif::ProgressStyle::with_template(
                    "{prefix:.bold.dim} {spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})",
                )?
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ");
            pb.set_style(style);
            pb.set_prefix("encoding");
            Some(pb)
        },
        _ => None,
    };

    // Process the encoded chunks as they arrive from the worker threads.
    while let Some((ordinal, result)) = encoded_chunks_rx.recv().await {
        actual_chunk_count += 1;
        if ordinal >= src_chunks.len() {
            src_chunks.resize_with(2 * src_chunks.len(), Default::default);
        }
        let src_pos_delta =
            match result {
                EncodedChunkResult::Data {
                    checksum,
                    src_length,
                    src_offset,
                    compressed,
                } => {
                    src_chunks[ordinal] = ArchiveChunk::Data {
                        checksum,
                        src_length,
                        src_offset,
                        arc_offset: arc_pos,
                    };
                    writer.write_all(compressed.as_slice()).await?;
                    arc_pos += u64::try_from(compressed.len())?;
                    src_length
                },
                EncodedChunkResult::Dupe { index } => {
                    src_chunks[ordinal] = ArchiveChunk::Dupe { index };
                    let Some(ArchiveChunk::Data { src_length: length, .. }) = src_chunks.get(usize::try_from(index)?)
                    else {
                        return Err("invalid dupe index".into());
                    };
                    *length
                },
            };
        src_pos += src_pos_delta;
        if let Some(pb) = pb.as_ref() {
            pb.inc(src_pos_delta);
        }
    }

    // Truncate the source chunks vec to the actual chunk count.
    src_chunks.truncate(actual_chunk_count);

    if let Some(pb) = pb.as_ref() {
        pb.finish();
    }

    Ok(ChonkerArchiveMeta {
        src_checksum: src_checksum.await??.into(),
        src_size: src_pos,
        src_chunks,
        ..ChonkerArchiveMeta::default()
    })
}
