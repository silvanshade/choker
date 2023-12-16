use crate::{
    codec::encode::{context::EncodeContext, EncodedChunkResult},
    BoxResult,
};

pub(super) struct ThreadLocalState {
    pub(super) compressor: zstd::bulk::Compressor<'static>,
    pub(super) compression_buffer: Vec<u8>,
    pub(super) arc_chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, EncodedChunkResult)>,
}

impl ThreadLocalState {
    pub(super) fn new(
        context: &EncodeContext,
        arc_chunks_tx: tokio::sync::mpsc::UnboundedSender<(usize, EncodedChunkResult)>,
    ) -> BoxResult<Self> {
        let mut compressor = zstd::bulk::Compressor::default();
        context.configure_zstd_bulk_compressor(&mut compressor)?;
        let capacity =
            {
                let chunk_size = usize::try_from(context.cdc_max_chunk_size)?;
                chunk_size + 2048
            };
        let compression_buffer = vec![u8::default(); capacity];
        Ok(Self {
            compressor,
            compression_buffer,
            arc_chunks_tx,
        })
    }
}
