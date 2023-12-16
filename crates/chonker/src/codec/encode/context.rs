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
    BoxResult,
};

pub struct EncodeContext {
    pub cdc_min_chunk_size: u32,
    pub cdc_avg_chunk_size: u32,
    pub cdc_max_chunk_size: u32,
    pub zstd_compression_level: i32,
    reader_size: Option<u64>,
    pub(crate) progress: Option<EncodeContextProgress>,
}

impl EncodeContext {
    const FASTCDC_MIN_CHUNK_SIZE: u32 = 1024;
    const FASTCDC_AVG_CHUNK_SIZE: u32 = 131_072;
    const FASTCDC_MAX_CHUNK_SIZE: u32 = 524_288;

    const ZSTD_COMPRESSION_LEVEL: i32 = -5;
    const ZSTD_INCLUDE_CHECKSUM: bool = false;
    const ZSTD_INCLUDE_DICTID: bool = false;

    pub fn new(reader_size: Option<u64>, report_progress: bool) -> BoxResult<Self> {
        let progress =
            reader_size
                .and_then(|size| {
                    if report_progress {
                        Some(EncodeContextProgress::new(size))
                    } else {
                        None
                    }
                })
                .transpose()?;
        Ok(Self {
            cdc_min_chunk_size: Self::FASTCDC_MIN_CHUNK_SIZE,
            cdc_avg_chunk_size: Self::FASTCDC_AVG_CHUNK_SIZE,
            cdc_max_chunk_size: Self::FASTCDC_MAX_CHUNK_SIZE,
            zstd_compression_level: Self::ZSTD_COMPRESSION_LEVEL,
            reader_size,
            progress,
        })
    }

    pub(crate) fn configure_zstd_bulk_compressor(&self, compressor: &mut zstd::bulk::Compressor) -> BoxResult<()> {
        compressor.set_compression_level(self.zstd_compression_level)?;
        compressor.include_checksum(Self::ZSTD_INCLUDE_CHECKSUM)?;
        compressor.include_dictid(Self::ZSTD_INCLUDE_DICTID)?;
        Ok(())
    }

    pub(super) fn async_stream_chunker<R>(&self, source: R) -> AsyncStreamChunker<R>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        AsyncStreamChunker::new(self, source)
    }

    pub(super) fn estimated_chunk_count(&self, source_size: u64) -> BoxResult<usize> {
        let source_size = usize::try_from(source_size)?;
        let average = usize::try_from(self.cdc_avg_chunk_size)?;
        let estimated_count = source_size / average;
        // NOTE: better to overallocate and truncate than to underallocate and reallocate
        Ok(2 * estimated_count)
    }

    #[must_use]
    pub fn progress(&self) -> Option<&indicatif::MultiProgress> {
        self.progress.as_ref().map(|progress| &progress.multi_progress)
    }

    #[inline]
    pub(super) fn progress_update(&self, src_delta: u64, arc_delta: u64) {
        if let Some(progress) = self.progress.as_ref() {
            progress.update(src_delta, arc_delta);
        }
    }

    pub(super) fn progress_finish(&self) {
        if let Some(progress) = self.progress.as_ref() {
            progress.finish();
        }
    }
}

pub struct EncodeContextProgress {
    pub multi_progress: MultiProgress,
    pub progress_encoding: ProgressBar,
    pub progress_emitting: ProgressBar,
}

impl EncodeContextProgress {
    pub fn new(reader_size: u64) -> BoxResult<Self> {
        let multi_progress = MultiProgress::new();

        let progress_encoding = multi_progress.add(ProgressBar::new(reader_size));
        progress_encoding.set_style(indicatif::ProgressStyle::default_bar().template(
            "{prefix:>10.bold.dim} {bytes_per_sec:>15} [{wide_bar:.green}]{bytes:>12} / {total_bytes:>12}",
        )?);
        progress_encoding.set_prefix("encoding 🗜️");

        let progress_emitting = multi_progress.add(ProgressBar::new(reader_size));
        progress_emitting
            .set_style(indicatif::ProgressStyle::default_bar().template(
                "{prefix:>10.bold.dim} {percent:>5}% (of src) [{wide_bar:.red}]{bytes:>12} (output bytes)",
            )?);
        progress_emitting.set_prefix("emitting 🗄️");

        Ok(Self {
            multi_progress,
            progress_encoding,
            progress_emitting,
        })
    }

    #[inline]
    fn update(&self, src_delta: u64, arc_delta: u64) {
        self.progress_encoding.inc(src_delta);
        self.progress_emitting.inc(arc_delta);
    }

    fn finish(&self) {
        self.progress_encoding.abandon();
        self.progress_emitting.abandon();
    }
}
