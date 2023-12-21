use tokio::io::AsyncRead;
use tokio_stream::Stream;

use crate::codec::encode::EncodeContext;

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
