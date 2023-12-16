pub(crate) mod chunk;
pub(crate) mod header;
pub(crate) mod meta;

use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::{
    archive::{
        header::ChonkerArchiveHeader,
        meta::{ChonkerArchiveMeta, OwningChonkerArchiveMeta},
    },
    codec::{decode::DecodeContext, encode::context::EncodeContext},
    BoxResult,
};

pub struct ChonkerArchive;

// Archive Meta byte layout:
//
// | offset |      size | description                                           |
// |--------|-----------|-------------------------------------------------------|
// |      0 |        16 | Archive file magic ("CHONKERMETA\xF0\x9F\x98\xBC\0"). |
// |     16 |         8 | Archive format version (u64 little-endian).           |
// |     24 |         8 | Padding.                                              |
// |     32 |        32 | BLAKE3 Checksum of [64 ..].                           |
// |     64 | size - 64 | Compressed meta.                                      |

// Archive Data byte layout:
//
// | offset |      size | description                                           |
// |--------|-----------|-------------------------------------------------------|
// |      0 |        16 | Archive file magic ("CHONKERDATA\xF0\x9F\x98\xBC\0"). |
// |     16 |         8 | Archive format version (u64 little-endian).           |
// |     24 |         8 | Padding.                                              |
// |     32 | size - 32 | Compressed data.                                      |

impl ChonkerArchive {
    pub async fn encode<R, MW, DW>(
        context: Arc<EncodeContext>,
        reader: R,
        reader_size: Option<u64>,
        mut meta_writer: MW,
        mut data_writer: DW,
    ) -> crate::BoxResult<ChonkerArchiveMeta>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        DW: tokio::io::AsyncWrite + Unpin + Send + 'static,
        MW: tokio::io::AsyncWrite + Unpin,
    {
        let header = ChonkerArchiveHeader::default();
        let emit_data = tokio::task::spawn({
            let context = context.clone();
            let header = header.clone();
            async move {
                header.write_data(&context, &mut data_writer).await?;
                let meta =
                    crate::codec::encode::encode_chunks(context.clone(), reader, reader_size, &mut data_writer).await?;
                data_writer.shutdown().await?;
                Ok::<_, crate::BoxError>(meta)
            }
        });
        header.write_meta(&context, &mut meta_writer).await?;
        let meta = emit_data.await??;
        meta.write(&context, &mut meta_writer).await?;
        meta_writer.shutdown().await?;
        Ok(meta)
    }

    #[allow(clippy::all, clippy::pedantic)]
    pub async fn decode<MR, DR, W>(
        mut context: Arc<DecodeContext>,
        mut meta_reader: MR,
        mut data_reader: DR,
        mut file_writer: W,
    ) -> crate::BoxResult<OwningChonkerArchiveMeta>
    where
        MR: tokio::io::AsyncRead + Unpin + Send,
        DR: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Unpin,
    {
        ChonkerArchiveHeader::read_meta(&context, &mut meta_reader).await?;
        let meta = ChonkerArchiveMeta::read(&context, meta_reader).await?;
        ChonkerArchiveHeader::read_data(&context, &mut data_reader).await?;
        let meta = crate::codec::decode::decode_chunks(context, meta, data_reader, &mut file_writer).await?;
        Ok(meta)
    }
}
