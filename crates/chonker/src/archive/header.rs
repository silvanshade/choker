use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::codec::encode::EncodeContext;

pub(crate) struct ArchiveHeader;

impl ArchiveHeader {
    pub(crate) const FILE_MAGIC: [u8; 16] = *b"CHONKERFILE\xF0\x9F\x98\xB8\0";

    pub(crate) async fn write<W>(context: &EncodeContext, writer: &mut W) -> crate::BoxResult<()>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_all(&Self::FILE_MAGIC).await?;
        Ok(())
    }
}
