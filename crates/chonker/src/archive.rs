pub(crate) mod chunk;
pub(crate) mod header;
pub(crate) mod meta;

use core::pin::pin;
use futures::stream::StreamExt;
use std::{collections::hash_map::Entry, sync::Arc};
use tinyvec::tiny_vec;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::{
    archive::{chunk::ArchiveChunk, header::ArchiveHeader, meta::ArchiveMeta},
    codec::encode::EncodeContext,
};

pub struct Archive {
    pub meta: ArchiveMeta,
}

// Archive bytes layout:
//
// | offset           |                  size | description                                           |
// |------------------|-----------------------|-------------------------------------------------------|
// |         0        |                    16 | Archive file magic ("CHONKERFILE\xF0\x9F\x98\xB8\0"). |
// |        16        |                     8 | Archive format version (u64 le).                      |
// |        24        |                     8 | Padding.                                              |
// |        32        | size - 40 - meta - 32 | Compressed data.                                      |
// | size - 40 - meta |             meta      | Compressed meta.                                      |
// | size - 40        |         8             | Compressed meta size (u64 le).                        |
// | size - 32        |        32             | Checksum of [size - 40 - meta .. size - 32].          |

// NOTE: Currently we construct the archive in post-order, where the meta is at the end of the file.
// This allows for more efficient encoding (since we can stream out encoded bytes as they are
// created without buffering) but is not suitable for streaming decoding. We might consider allowing
// this to be configured by the encoder in the future. In this case, we can use the padding bytes to
// specify some additional data describing the layout strategy.

impl Archive {
    #[allow(clippy::unused_async)]
    pub async fn create<R, W>(
        context: Arc<EncodeContext>,
        reader: R,
        reader_size: Option<u64>,
        writer: &mut W,
    ) -> crate::BoxResult<Self>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        ArchiveHeader::write(&context, writer).await?;
        let meta = crate::codec::encode::emit_chunks(context.clone(), reader, reader_size, writer).await?;
        meta.write(&context, writer).await?;
        Ok(Archive { meta })
    }
}
