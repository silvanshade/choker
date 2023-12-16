pub(crate) mod context;
mod state;

use bytes::Bytes;
use dashmap::DashMap;
use indicatif::{MultiProgress, ProgressBar};
use rayon::prelude::*;
use std::{cell::RefCell, collections::BTreeMap, sync::Arc};
use thread_local::ThreadLocal;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

use crate::{
    archive::{chunk::ArchiveChunk, meta::ChonkerArchiveMeta},
    cdc::AsyncStreamChunker,
    codec::encode::{context::EncodeContext, state::ThreadLocalState},
    BoxResult,
};

pub(crate) struct EncodedChunkResult {
    pub(crate) checksum: [u8; 32],
    pub(crate) src_length: u32,
    pub(crate) src_offset: u64,
    pub(crate) compressed: Bytes,
}

#[inline]
async fn encode_chunks_cdc_chunker<R>(
    context: Arc<EncodeContext>,
    reader: R,
    cdc_chunks_zstd_tx: tokio::sync::mpsc::UnboundedSender<(usize, fastcdc::v2020::ChunkData)>,
    cdc_chunks_hash_tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
) -> BoxResult<u64>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut src_size = 0;
    let mut chunker = context.async_stream_chunker(reader);
    let stream = chunker.as_stream();
    let mut stream = core::pin::pin!(stream);
    let mut ordinal = 0;
    while let Some(chunk) = stream.try_next().await? {
        src_size += chunk.length;
        cdc_chunks_hash_tx.send(chunk.data.clone())?;
        cdc_chunks_zstd_tx.send((ordinal, chunk))?;
        ordinal += 1;
        tokio::task::yield_now().await;
    }
    u64::try_from(src_size).map_err(Into::into)
}

#[inline]
fn encode_chunks_compressor(
    context: &EncodeContext,
    thread_local: &thread_local::ThreadLocal<RefCell<ThreadLocalState>>,
    cdc_chunks_zstd_rx: tokio::sync::mpsc::UnboundedReceiver<(usize, fastcdc::v2020::ChunkData)>,
    arc_chunks_tx: &tokio::sync::mpsc::UnboundedSender<(usize, EncodedChunkResult)>,
) -> BoxResult<()> {
    let chunker = crate::util::sync::BlockingReceiver::new(cdc_chunks_zstd_rx);
    chunker.par_bridge().try_for_each(|(ordinal, chunk)| -> BoxResult<()> {
        let state = thread_local.get_or(|| {
            let state = ThreadLocalState::new(context, arc_chunks_tx.clone()).unwrap();
            RefCell::new(state)
        });
        let ThreadLocalState {
            compressor,
            arc_chunks_tx: chunks_tx,
            compression_buffer,
            ..
        } = &mut *state.borrow_mut();

        compressor
            .context_mut()
            .set_pledged_src_size(Some(u64::try_from(chunk.length)?))
            .map_err(|err| format!("set_pleged_src_size failed: {err}"))?;

        let data = chunk.data;
        let checksum = {
            let mut hasher = blake3::Hasher::new();
            hasher.update_rayon(data.as_slice());
            <[u8; 32]>::from(hasher.finalize())
        };

        let src_offset = chunk.offset;
        let src_length = u32::try_from(chunk.length)?;

        let compression_bound = zstd_safe::compress_bound(chunk.length);
        if compression_buffer.len() < compression_bound {
            compression_buffer.resize(compression_bound, u8::default());
        }
        let arc_length = compressor.compress_to_buffer(data.as_slice(), compression_buffer.as_mut_slice())?;

        chunks_tx.send((ordinal, EncodedChunkResult {
            checksum,
            src_length,
            src_offset,
            compressed: Bytes::copy_from_slice(&compression_buffer[.. arc_length]),
        }))?;

        Ok(())
    })?;

    Ok(())
}

#[inline]
async fn encode_chunks_emitter<DW>(
    context: &EncodeContext,
    reader_size: Option<u64>,
    mut data_writer: DW,
    mut arc_chunks_rx: tokio::sync::mpsc::UnboundedReceiver<(usize, EncodedChunkResult)>,
) -> BoxResult<Vec<ArchiveChunk>>
where
    DW: tokio::io::AsyncWrite + Unpin,
{
    // Try to estimate a reasonable count for the cdc chunks.
    let estimated_chunk_count = reader_size
        .map(|source_size| context.estimated_chunk_count(source_size))
        .transpose()?
        .unwrap_or(2048);

    let mut arc_size = 0u64;
    let arc_header_offset = u64::try_from(crate::archive::header::ChonkerArchiveHeader::CHONKER_META_HEADER_OFFSET)?;

    // Allocate the source chunks vec with the estimated length to avoid reallocations.
    let mut src_chunks = vec![ArchiveChunk::default(); estimated_chunk_count];

    let mut cursor = 0usize;
    let mut collator = std::collections::BTreeMap::<usize, EncodedChunkResult>::new();

    'outer: loop {
        let result = match collator.remove(&cursor) {
            Some(result) => result,
            None => 'inner: loop {
                match arc_chunks_rx.recv().await {
                    Some((ordinal, result)) if ordinal == cursor => break 'inner result,
                    Some((ordinal, result)) => {
                        collator.insert(ordinal, result);
                        continue;
                    },
                    None => break 'outer,
                }
                tokio::task::yield_now().await;
            },
        };

        if cursor >= src_chunks.len() {
            src_chunks.resize_with(2 * src_chunks.len(), Default::default);
        }

        let EncodedChunkResult {
            checksum,
            src_length,
            src_offset,
            compressed,
        } = result;

        let arc_offset = arc_header_offset + arc_size;
        let arc_length = u32::try_from(compressed.len())?;
        src_chunks[cursor] = ArchiveChunk {
            checksum,
            src_offset,
            src_length,
            arc_offset,
            arc_length,
        };

        data_writer.write_all(&compressed).await?;
        arc_size += u64::from(arc_length);

        context.progress_update(u64::from(src_length), u64::from(arc_length));
        cursor += 1;
    }

    // Truncate the source chunks vec to the actual chunk count.
    src_chunks.truncate(cursor);

    // Finish the progress bars.
    context.progress_finish();

    Ok(src_chunks)
}

#[inline]
fn encode_chunks_hasher(mut cdc_chunks_hash_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    while let Some(data) = cdc_chunks_hash_rx.blocking_recv() {
        hasher.update_rayon(data.as_slice());
        rayon::yield_local();
    }
    hasher.finalize()
}

pub(crate) async fn encode_chunks<R, DW>(
    context: Arc<EncodeContext>,
    reader: R,
    reader_size: Option<u64>,
    mut data_writer: DW,
) -> BoxResult<ChonkerArchiveMeta>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    DW: tokio::io::AsyncWrite + Unpin,
{
    let (cdc_chunks_zstd_tx, mut cdc_chunks_zstd_rx) =
        tokio::sync::mpsc::unbounded_channel::<(usize, fastcdc::v2020::ChunkData)>();
    let (cdc_chunks_hash_tx, mut cdc_chunks_hash_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let (arc_chunks_tx, mut arc_chunks_rx) = tokio::sync::mpsc::unbounded_channel::<(usize, EncodedChunkResult)>();

    let cdc_chunker = tokio::task::spawn({
        let context = context.clone();
        encode_chunks_cdc_chunker(context, reader, cdc_chunks_zstd_tx, cdc_chunks_hash_tx)
    });

    let arc_chunker = tokio::task::spawn_blocking({
        let context = context.clone();
        move || -> BoxResult<blake3::Hash> {
            let thread_local = Arc::new(thread_local::ThreadLocal::new());
            let pool = rayon::ThreadPoolBuilder::new().build()?;
            let checksum = pool.install(|| -> BoxResult<blake3::Hash> {
                let compressor =
                    || encode_chunks_compressor(&context, &thread_local, cdc_chunks_zstd_rx, &arc_chunks_tx);
                let hasher = || encode_chunks_hasher(cdc_chunks_hash_rx);
                let (compressor, checksum) = rayon::join(compressor, hasher);
                compressor?;
                Ok(checksum)
            })?;
            Ok(checksum)
        }
    });

    let src_size = cdc_chunker.await??;
    let src_checksum = arc_chunker.await??.into();
    let src_chunks = encode_chunks_emitter(&context, reader_size, data_writer, arc_chunks_rx).await?;

    Ok(ChonkerArchiveMeta {
        src_size,
        src_checksum,
        src_chunks,
        ..ChonkerArchiveMeta::default()
    })
}
