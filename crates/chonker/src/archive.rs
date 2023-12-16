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
// |        16        | size - 40 - meta - 16 | Compressed data.                                      |
// | size - 40 - meta |             meta      | Compressed meta.                                      |
// | size - 40        |         8             | Compressed meta size (u64 le).                        |
// | size - 32        |        32             | Checksum of [size - 40 - meta .. size - 32].          |

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
        let meta = crate::codec::encode::emit_chunks(context, reader, reader_size, writer).await?;
        meta.write(writer).await?;
        Ok(Archive { meta })
    }
}
