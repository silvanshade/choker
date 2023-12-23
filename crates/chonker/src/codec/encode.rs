use dashmap::DashMap;
use indicatif::{MultiProgress, ProgressBar};
use std::{cell::RefCell, sync::Arc};
use thread_local::ThreadLocal;
use tokio::io::AsyncWriteExt;

use crate::{
    archive::{chunk::ArchiveChunk, meta::ChonkerArchiveMeta},
    cdc::AsyncStreamChunker,
    BoxResult,
};

pub struct EncodeContext {
    pub cdc_min_chunk_size: u32,
    pub cdc_avg_chunk_size: u32,
    pub cdc_max_chunk_size: u32,
    pub zstd_compression_level: i32,
    input_size: u64,
    pub progress: Option<EncodeContextProgress>,
}

pub struct EncodeContextProgress {
    pub multi_progress: MultiProgress,
    pub progress_chunking: ProgressBar,
    pub progress_analyzing: ProgressBar,
    pub progress_deduping: ProgressBar,
    pub progress_archiving: ProgressBar,
}

impl EncodeContext {
    const FASTCDC_MIN_CHUNK_SIZE: u32 = 1024;
    const FASTCDC_AVG_CHUNK_SIZE: u32 = 131_072;
    const FASTCDC_MAX_CHUNK_SIZE: u32 = 524_288;

    const ZSTD_COMPRESSION_LEVEL: i32 = 15;
    const ZSTD_INCLUDE_CHECKSUM: bool = false;
    const ZSTD_INCLUDE_DICTID: bool = false;

    pub fn new(input_size: u64, report_progress: bool) -> BoxResult<Self> {
        let progress =
            if report_progress {
                Some(EncodeContextProgress::new(input_size)?)
            } else {
                None
            };
        Ok(Self {
            cdc_min_chunk_size: Self::FASTCDC_MIN_CHUNK_SIZE,
            cdc_avg_chunk_size: Self::FASTCDC_AVG_CHUNK_SIZE,
            cdc_max_chunk_size: Self::FASTCDC_MAX_CHUNK_SIZE,
            zstd_compression_level: Self::ZSTD_COMPRESSION_LEVEL,
            input_size,
            progress,
        })
    }

    pub(crate) fn configure_zstd_bulk_compressor(&self, compressor: &mut zstd::bulk::Compressor) -> BoxResult<()> {
        compressor.set_compression_level(self.zstd_compression_level)?;
        // compressor.include_checksum(Self::ZSTD_INCLUDE_CHECKSUM)?;
        // compressor.include_dictid(Self::ZSTD_INCLUDE_DICTID)?;
        Ok(())
    }

    fn async_stream_chunker<R>(&self, source: R) -> AsyncStreamChunker<R>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        AsyncStreamChunker::new(self, source)
    }

    fn estimated_chunk_count(&self, source_size: u64) -> BoxResult<usize> {
        let source_size = usize::try_from(source_size)?;
        let average = usize::try_from(self.cdc_avg_chunk_size)?;
        let estimated_count = source_size / average;
        // NOTE: better to overallocate and truncate than to underallocate and reallocate
        Ok(2 * estimated_count)
    }

    fn progress_update(
        &self,
        cdc_data_size: Option<u64>,
        src_data_size: Option<u64>,
        arc_data_size: Option<u64>,
        arc_dupe_size: Option<u64>,
    ) {
        if let Some(progress) = self.progress.as_ref() {
            progress.update(cdc_data_size, src_data_size, arc_data_size, arc_dupe_size);
        }
    }

    fn progress_finish(&self) {
        if let Some(progress) = self.progress.as_ref() {
            progress.finish();
        }
    }
}

impl EncodeContextProgress {
    pub fn new(report_size: u64) -> BoxResult<Self> {
        let multi_progress = MultiProgress::new();

        let progress_analyzing = multi_progress.add(ProgressBar::new(report_size));
        progress_analyzing.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("{prefix:>12.bold.dim}  {spinner:.green}{spinner:.yellow}{spinner:.red}  [{elapsed_precise:8}] [{wide_bar:.blue}] {bytes:>10} / {total_bytes:>10}")?
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .progress_chars("#>-"),
        );
        progress_analyzing.enable_steady_tick(std::time::Duration::from_millis(100));
        progress_analyzing.set_prefix("analyzing 🔬");

        let progress_chunking = multi_progress.add(ProgressBar::new(report_size));
        progress_chunking.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("{prefix:>12.bold.dim}                  [{wide_bar:.green}] {bytes:>10}             ")?,
        );
        progress_chunking.set_prefix("chunking 🧱");

        let progress_deduping = multi_progress.add(ProgressBar::new(report_size));
        progress_deduping.set_style(indicatif::ProgressStyle::default_bar().template(
            "{prefix:>11.bold.dim} {percent:>5}% (of input) [{wide_bar:.yellow}] {bytes:>10} (dupe bytes)",
        )?);
        progress_deduping.set_prefix("deduping ♻️");

        let progress_archiving = multi_progress.add(ProgressBar::new(report_size));
        progress_archiving.set_style(
            indicatif::ProgressStyle::default_bar().template(
                "{prefix:>11.bold.dim} {percent:>5}% (of input) [{wide_bar:.red}] {bytes:>10} (data bytes)",
            )?,
        );
        progress_archiving.set_prefix("archiving 🗄️");

        Ok(Self {
            multi_progress,
            progress_chunking,
            progress_analyzing,
            progress_deduping,
            progress_archiving,
        })
    }

    fn update(
        &self,
        cdc_data_size: Option<u64>,
        src_data_size: Option<u64>,
        arc_data_size: Option<u64>,
        arc_dupe_size: Option<u64>,
    ) {
        if let Some(cdc_data_size) = cdc_data_size {
            self.progress_chunking.inc(cdc_data_size);
        }
        if let Some(src_data_size) = src_data_size {
            self.progress_analyzing.set_position(src_data_size);
        }
        if let Some(arc_data_size) = arc_data_size {
            self.progress_archiving.set_position(arc_data_size);
        }
        if let Some(arc_dupe_size) = arc_dupe_size {
            self.progress_deduping.set_position(arc_dupe_size);
        }
    }

    fn finish(&self) {
        self.progress_analyzing.finish();
        self.progress_chunking.abandon();
        self.progress_deduping.abandon();
        self.progress_archiving.abandon();
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
    ) -> BoxResult<Self> {
        let mut compressor = zstd::bulk::Compressor::default();
        context.configure_zstd_bulk_compressor(&mut compressor)?;
        Ok(Self { compressor, chunks_tx })
    }
}

pub(crate) enum EncodedChunkResult {
    Data {
        checksum: [u8; 32],
        src_length: u32,
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
) -> impl FnOnce(&rayon::Scope) -> BoxResult<()> {
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
            let src_length = u32::try_from(chunk.length)?;
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
    chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, EncodedChunkResult)>,
) -> impl FnOnce(&rayon::Scope) -> BoxResult<blake3::Hash>
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
            context.progress_update(u64::try_from(chunk.data.len())?.into(), None, None, None);
            hasher.update(chunk.data.as_slice());
            let context = context.clone();
            let thread_local = thread_local.clone();
            let chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, EncodedChunkResult)> = chunks_tx.clone();
            let processed = processed.clone();
            scope.spawn(move |scope| {
                process_one_chunk(context, thread_local, chunks_tx, processed, ordinal, chunk)(scope).unwrap();
            });
        }

        Ok(hasher.finalize())
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn encode_chunks<R, W>(
    context: Arc<EncodeContext>,
    reader: R,
    reader_size: Option<u64>,
    writer: &mut W,
) -> BoxResult<ChonkerArchiveMeta>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin,
{
    let (chunks_tx, mut encoded_chunks_rx) = tokio::sync::mpsc::unbounded_channel::<(usize, EncodedChunkResult)>();

    let src_checksum = tokio::task::spawn_blocking({
        let context = context.clone();
        move || {
            let thread_local = Arc::new(thread_local::ThreadLocal::new());
            let pool = rayon::ThreadPoolBuilder::new().build()?;
            let checksum = pool.in_place_scope(process_all_chunks(context, reader, thread_local, chunks_tx))?;
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

    let mut src_data_size = 0u64;
    let mut arc_data_size = 0u64;
    let mut arc_dupe_size = 0u64;
    let arc_header_offset = u64::try_from(crate::archive::header::ChonkerArchiveHeader::CHONKER_FILE_HEADER_OFFSET)?;

    // Allocate the source chunks vec with the estimated length to avoid reallocations.
    let mut src_chunks = vec![ArchiveChunk::default(); estimated_chunk_count];

    // Process the encoded chunks as they arrive from the worker threads.
    while let Some((ordinal, result)) = encoded_chunks_rx.recv().await {
        actual_chunk_count += 1;
        if ordinal >= src_chunks.len() {
            src_chunks.resize_with(2 * src_chunks.len(), Default::default);
        }
        match result {
            EncodedChunkResult::Data {
                checksum,
                src_length,
                src_offset,
                compressed,
            } => {
                let arc_offset = arc_header_offset + arc_data_size;
                let arc_length = u32::try_from(compressed.len())?;
                src_chunks[ordinal] = ArchiveChunk::Data {
                    checksum,
                    src_offset,
                    src_length,
                    arc_offset,
                    arc_length,
                };
                writer.write_all(compressed.as_slice()).await?;
                src_data_size += u64::from(src_length);
                arc_data_size += u64::from(arc_length);
            },
            EncodedChunkResult::Dupe { index } => {
                src_chunks[ordinal] = ArchiveChunk::Dupe { index };
                let Some(ArchiveChunk::Data {
                    src_length, arc_length, ..
                }) = src_chunks.get(usize::try_from(index)?)
                else {
                    return Err("invalid dupe index".into());
                };
                src_data_size += u64::from(*src_length);
                arc_dupe_size += u64::from(*arc_length);
            },
        };
        context.progress_update(None, src_data_size.into(), arc_data_size.into(), arc_dupe_size.into());
    }

    // Truncate the source chunks vec to the actual chunk count.
    src_chunks.truncate(actual_chunk_count);

    // Finish the progress bars.
    context.progress_finish();

    Ok(ChonkerArchiveMeta {
        src_checksum: src_checksum.await??.into(),
        src_size: src_data_size,
        src_chunks,
        ..ChonkerArchiveMeta::default()
    })
}
