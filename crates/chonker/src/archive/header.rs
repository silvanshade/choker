use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::codec::encode::EncodeContext;

pub(crate) struct ArchiveHeader {
    format_version: u16,
}

impl Default for ArchiveHeader {
    fn default() -> Self {
        Self {
            format_version: CHONKER_FILE_FORMAT_V0,
        }
    }
}

const CHONKER_FILE_FORMAT_V0: u16 = 0u16;

impl ArchiveHeader {
    pub(crate) const FILE_MAGIC: [u8; 16] = *b"CHONKERFILE\xF0\x9F\x98\xB8\0";

    pub(crate) async fn write<W>(&self, context: &EncodeContext, writer: &mut W) -> crate::BoxResult<()>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_all(&Self::FILE_MAGIC).await?;
        writer.write_u16_le(self.format_version).await?;
        writer.write_all(&[0u8; 14]).await?;
        Ok(())
    }
}
