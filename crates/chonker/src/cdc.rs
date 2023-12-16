use tokio::io::AsyncRead;
use tokio_stream::Stream;

use crate::codec::encode::EncodeContext;

pub(crate) struct Chunker<'source> {
    inner: fastcdc::v2020::FastCDC<'source>,
}

impl<'source> Chunker<'source> {
    pub(crate) fn new(context: &EncodeContext, source: &'source [u8]) -> Self {
        let level = fastcdc::v2020::Normalization::Level0;
        let inner = fastcdc::v2020::FastCDC::with_level(
            source,
            context.cdc_min_chunk_size,
            context.cdc_avg_chunk_size,
            context.cdc_max_chunk_size,
            level,
        );
        Self { inner }
    }
}

impl Iterator for Chunker<'_> {
    type Item = fastcdc::v2020::Chunk;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub(crate) struct AsyncStreamChunker<R> {
    inner: fastcdc::v2020::AsyncStreamCDC<R>,
}

impl<R> AsyncStreamChunker<R> {
    pub(crate) fn new(context: &EncodeContext, source: R) -> Self
    where
        R: AsyncRead + Unpin,
    {
        let level = fastcdc::v2020::Normalization::Level0;
        let inner = fastcdc::v2020::AsyncStreamCDC::with_level(
            source,
            context.cdc_min_chunk_size,
            context.cdc_avg_chunk_size,
            context.cdc_max_chunk_size,
            level,
        );
        Self { inner }
    }

    pub(crate) fn as_stream(
        &mut self,
    ) -> impl Stream<Item = Result<fastcdc::v2020::ChunkData, fastcdc::v2020::Error>> + '_
    where
        R: AsyncRead + Unpin,
    {
        self.inner.as_stream()
    }
}
