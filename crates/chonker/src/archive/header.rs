use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::codec::{decode::DecodeContext, encode::EncodeContext};

pub(crate) struct ChonkerArchiveHeader {
    format_version: u16,
}

impl Default for ChonkerArchiveHeader {
    fn default() -> Self {
        Self {
            format_version: CHONKER_FILE_FORMAT_V0,
        }
    }
}

const CHONKER_FILE_FORMAT_V0: u16 = 0u16;

impl ChonkerArchiveHeader {
    pub(crate) const FILE_MAGIC: [u8; 16] = *b"CHONKERFILE\xF0\x9F\x98\xB8\0";

    pub(crate) async fn read<R>(_context: &DecodeContext, reader: &mut R) -> crate::BoxResult<ChonkerArchiveHeader>
    where
        R: AsyncRead + Unpin,
    {
        let buf = &mut [0u8; Self::FILE_MAGIC.len()];
        reader.read_exact(buf).await?;
        if buf != &Self::FILE_MAGIC {
            return Err(crate::BoxError::from("invalid file magic"));
        }

        let format_version = reader.read_u16_le().await?;

        let buf = &mut [0u8; 14];
        reader.read_exact(buf).await?;
        if buf != &[0u8; 14] {
            return Err(crate::BoxError::from("invalid padding"));
        }

        Ok(ChonkerArchiveHeader { format_version })
    }

    pub(crate) async fn write<W>(&self, _context: &EncodeContext, writer: &mut W) -> crate::BoxResult<()>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_all(&Self::FILE_MAGIC).await?;
        writer.write_u16_le(self.format_version).await?;
        writer.write_all(&[0u8; 14]).await?;
        Ok(())
    }
}
