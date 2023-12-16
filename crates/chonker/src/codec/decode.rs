use bytes::Bytes;
use core::cell::RefCell;
use indicatif::{MultiProgress, ProgressBar};
use positioned_io::ReadAt;
use std::{collections::BTreeMap, io::Read, os::unix::process, sync::Arc};
use thread_local::ThreadLocal;

use crate::{
    archive::{
        chunk::{self, ArchiveChunk},
        header::ChonkerArchiveHeader,
        meta::{ChonkerArchiveMeta, OwningChonkerArchiveMeta},
    },
    BoxResult,
};

#[derive(Default)]
pub struct DecodeContext {
    pub(crate) progress: Option<DecodeContextProgress>,
}

impl DecodeContext {
    pub fn new(data_reader_size: Option<u64>, report_progress: bool) -> BoxResult<Self> {
        let progress =
            data_reader_size
                .and_then(|size| {
                    if report_progress {
                        Some(DecodeContextProgress::new(size))
                    } else {
                        None
                    }
                })
                .transpose()?;
        Ok(Self { progress })
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    pub(crate) fn configure_zstd_bulk_decompressor(
        &self,
        _decompressor: &mut zstd::bulk::Decompressor,
    ) -> crate::BoxResult<()> {
        Ok(())
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    pub(crate) fn configure_zstd_stream_decompressor<R>(
        &self,
        _decompressor: &mut zstd::stream::read::Decoder<R>,
    ) -> crate::BoxResult<()>
    where
        R: std::io::BufRead,
    {
        Ok(())
    }

    #[must_use]
    pub fn progress(&self) -> Option<&indicatif::MultiProgress> {
        self.progress.as_ref().map(|progress| &progress.multi_progress)
    }

    fn progress_set_src_length(&self, src_size: u64) {
        if let Some(progress) = self.progress.as_ref() {
            progress.set_src_length(src_size);
        }
    }

    fn progress_update(&self, arc_data_size: u64, src_data_size: u64) {
        if let Some(progress) = self.progress.as_ref() {
            progress.update(arc_data_size, src_data_size);
        }
    }

    fn progress_finish(&self) {
        if let Some(progress) = self.progress.as_ref() {
            progress.finish();
        }
    }
}

pub struct DecodeContextProgress {
    pub multi_progress: MultiProgress,
    pub progress_decoding: ProgressBar,
    pub progress_emitting: ProgressBar,
}

impl DecodeContextProgress {
    pub fn new(arc_size: u64) -> BoxResult<Self> {
        let multi_progress = MultiProgress::new();

        let progress_decoding = multi_progress.add(ProgressBar::new(arc_size));
        progress_decoding.set_style(indicatif::ProgressStyle::default_bar().template(
            "{prefix:>10.bold.dim} {bytes_per_sec:>15} [{wide_bar:.green}]{bytes:>12} / {total_bytes:>12}",
        )?);
        progress_decoding.set_prefix("decoding 🗜️");

        let progress_emitting = multi_progress.add(ProgressBar::new(0));
        progress_emitting
            .set_style(indicatif::ProgressStyle::default_bar().template(
                "{prefix:>10.bold.dim} {percent:>5}% (of src) [{wide_bar:.red}]{bytes:>12} (output bytes)",
            )?);
        progress_emitting.set_prefix("emitting 🗄️");

        Ok(Self {
            multi_progress,
            progress_decoding,
            progress_emitting,
        })
    }

    fn set_src_length(&self, src_size: u64) {
        self.progress_emitting.set_length(src_size);
    }

    fn update(&self, arc_data_size: u64, src_data_size: u64) {
        self.progress_decoding.set_position(arc_data_size);
        self.progress_emitting.set_position(src_data_size);
    }

    fn finish(&self) {
        self.progress_decoding.finish();
        self.progress_emitting.finish();
    }
}

struct ThreadLocalState {
    decompressor: zstd::bulk::Decompressor<'static>,
    chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, Bytes)>,
    src_data: Vec<u8>,
}

impl ThreadLocalState {
    fn new(context: &DecodeContext, chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, Bytes)>) -> BoxResult<Self> {
        let mut decompressor = zstd::bulk::Decompressor::new()?;
        context.configure_zstd_bulk_decompressor(&mut decompressor)?;
        Ok(Self {
            decompressor,
            chunks_tx,
            src_data: Vec::default(),
        })
    }
}

#[allow(clippy::inline_always, clippy::needless_pass_by_value, clippy::too_many_arguments)]
#[inline(always)]
fn process_one_chunk_inner(
    context: Arc<DecodeContext>,
    thread_local: Arc<ThreadLocal<RefCell<ThreadLocalState>>>,
    chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, Bytes)>,
    hasher_tx: tokio::sync::mpsc::UnboundedSender<(usize, Bytes)>,
    ordinal: usize,
    checksum: [u8; 32],
    arc_data: Vec<u8>,
    arc_pos: u64,
    src_len: u64,
    src_pos: u64,
    scope: &rayon::Scope,
) -> BoxResult<()> {
    let state = thread_local.get_or(|| {
        let state = ThreadLocalState::new(&context, chunks_tx.clone()).unwrap();
        RefCell::new(state)
    });
    let ThreadLocalState {
        decompressor, src_data, ..
    } = &mut *state.borrow_mut();

    let src_len = usize::try_from(src_len)?;
    if src_len > src_data.len() {
        src_data.resize(src_len, u8::default());
    }

    decompressor
        .decompress_to_buffer(&arc_data, &mut src_data[.. src_len])
        .unwrap();

    assert_eq!(&checksum, blake3::hash(&src_data[.. src_len]).as_bytes());
    hasher_tx
        .send((ordinal, Bytes::copy_from_slice(&src_data[.. src_len])))
        .unwrap();

    context.progress_update(arc_pos, src_pos);

    Ok(())
}

#[allow(clippy::inline_always, clippy::needless_pass_by_value, clippy::too_many_arguments)]
#[inline(always)]
fn process_one_chunk(
    context: Arc<DecodeContext>,
    thread_local: Arc<ThreadLocal<RefCell<ThreadLocalState>>>,
    chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, Bytes)>,
    hasher_tx: tokio::sync::mpsc::UnboundedSender<(usize, Bytes)>,
    ordinal: usize,
    checksum: [u8; 32],
    arc_data: Vec<u8>,
    arc_pos: u64,
    src_len: u64,
    src_pos: u64,
) -> impl FnOnce(&rayon::Scope) {
    move |scope| {
        scope.spawn(move |scope| {
            process_one_chunk_inner(
                context,
                thread_local,
                chunks_tx,
                hasher_tx,
                ordinal,
                checksum,
                arc_data,
                arc_pos,
                src_len,
                src_pos,
                scope,
            )
            .unwrap();
        });
    }
}

#[allow(clippy::inline_always)]
#[inline(always)]
fn process_all_chunks<DR>(
    context: Arc<DecodeContext>,
    meta: OwningChonkerArchiveMeta,
    mut data_reader: DR,
    thread_local: Arc<ThreadLocal<RefCell<ThreadLocalState>>>,
    chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, Bytes)>,
    hasher_tx: tokio::sync::mpsc::UnboundedSender<(usize, Bytes)>,
) -> impl FnOnce(&rayon::Scope) -> BoxResult<OwningChonkerArchiveMeta>
where
    DR: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    move |scope| {
        let mut data_reader = tokio_util::io::SyncIoBridge::new(&mut data_reader);
        let mut arc_pos = u64::try_from(ChonkerArchiveHeader::CHONKER_DATA_HEADER_OFFSET)?;
        let mut src_pos = u64::default();

        context.progress_set_src_length(u64::from(meta.src_size));

        for (ordinal, chunk) in meta.src_chunks.iter().enumerate() {
            let &rkyv::Archived::<ArchiveChunk> {
                checksum,
                src_length,
                arc_offset,
                arc_length,
                ..
            } = chunk;
            assert_eq!(arc_pos, u64::from(arc_offset));

            let arc_length = u32::from(arc_length);
            let arc_len = usize::try_from(arc_length)?;
            let arc_len_u64 = u64::from(arc_length);

            let mut arc_data = Vec::with_capacity(arc_len);
            data_reader = {
                let mut data_reader_take = data_reader.take(arc_len_u64);
                std::io::copy(&mut data_reader_take, &mut arc_data)?;
                data_reader_take.into_inner()
            };

            let src_len_u64 = u64::from(u32::from(src_length));

            arc_pos += arc_len_u64;
            src_pos += src_len_u64;

            scope.spawn({
                let context = context.clone();
                let thread_local = thread_local.clone();
                let chunks_tx = chunks_tx.clone();
                let hasher_tx = hasher_tx.clone();
                process_one_chunk(
                    context,
                    thread_local,
                    chunks_tx,
                    hasher_tx,
                    ordinal,
                    checksum,
                    arc_data,
                    arc_pos,
                    src_len_u64,
                    src_pos,
                )
            });
        }

        Ok(meta)
    }
}

pub(crate) async fn decode_chunks<DR, W>(
    context: Arc<DecodeContext>,
    meta: OwningChonkerArchiveMeta,
    mut data_reader: DR,
    mut writer: W,
) -> crate::BoxResult<OwningChonkerArchiveMeta>
where
    DR: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite,
{
    let (chunks_tx, mut chunks_rx) = tokio::sync::mpsc::unbounded_channel::<(usize, Bytes)>();
    let (hasher_tx, mut hasher_rx) = tokio::sync::mpsc::unbounded_channel::<(usize, Bytes)>();

    let mut pos = u64::try_from(ChonkerArchiveHeader::CHONKER_DATA_HEADER_OFFSET)?;

    let hasher =
        tokio::task::spawn(async move {
            let mut collator = BTreeMap::<usize, Bytes>::new();
            let mut hasher = blake3::Hasher::new();
            let mut cursor = 0;
            'outer: loop {
                let data = match collator.remove(&cursor) {
                    Some(data) => data,
                    None => 'inner: loop {
                        match hasher_rx.recv().await {
                            Some((ordinal, data)) if ordinal == cursor => break 'inner data,
                            Some((ordinal, data)) => {
                                collator.insert(ordinal, data);
                            },
                            None if collator.is_empty() => break 'outer,
                            None => continue 'outer,
                        }
                    },
                };
                hasher.update(&data);
                cursor += 1;
            }
            hasher.finalize()
        });

    let meta = tokio::task::spawn_blocking({
        let context = context.clone();
        move || -> BoxResult<OwningChonkerArchiveMeta> {
            let thread_local = Arc::new(thread_local::ThreadLocal::new());
            let pool = rayon::ThreadPoolBuilder::new().build()?;
            pool.in_place_scope(process_all_chunks(
                context,
                meta,
                data_reader,
                thread_local,
                chunks_tx,
                hasher_tx,
            ))
        }
    })
    .await??;

    assert_eq!(&meta.src_checksum, hasher.await?.as_bytes());

    context.progress_finish();

    Ok(meta)
}
