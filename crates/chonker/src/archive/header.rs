use byteorder::ReadBytesExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::codec::{decode::DecodeContext, encode::EncodeContext};

pub(crate) struct ChonkerArchiveHeader {
    format_version: u16,
}

impl Default for ChonkerArchiveHeader {
    fn default() -> Self {
        Self {
            format_version: Self::CHONKER_FILE_FORMAT_V0,
        }
    }
}

impl ChonkerArchiveHeader {
    const CHONKER_FILE_MAGIC: [u8; 16] = *b"CHONKERFILE\xF0\x9F\x98\xB8\0";
    const CHONKER_FILE_FORMAT_V0: u16 = 0u16;
    const CHONKER_FILE_HEADER_PADDING: &'static [u8] = &[0u8; 8];
    pub(crate) const CHONKER_FILE_HEADER_OFFSET: usize =
        Self::CHONKER_FILE_MAGIC.len() + core::mem::size_of::<u64>() + Self::CHONKER_FILE_HEADER_PADDING.len();

    pub(crate) fn read<R>(_context: &DecodeContext, reader: &mut R) -> crate::BoxResult<ChonkerArchiveHeader>
    where
        R: std::io::Read,
    {
        let buf = &mut [0u8; Self::CHONKER_FILE_MAGIC.len()];
        reader.read_exact(buf)?;
        if buf != &Self::CHONKER_FILE_MAGIC {
            return Err(crate::BoxError::from("invalid file magic"));
        }

        let format_version = reader.read_u16::<byteorder::LittleEndian>()?;

        let buf = &mut [0u8; Self::CHONKER_FILE_HEADER_PADDING.len()];
        reader.read_exact(buf)?;
        if buf != Self::CHONKER_FILE_HEADER_PADDING {
            return Err(crate::BoxError::from("invalid padding"));
        }

        Ok(ChonkerArchiveHeader { format_version })
    }

    pub(crate) async fn write<W>(&self, _context: &EncodeContext, writer: &mut W) -> crate::BoxResult<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        writer.write_all(&Self::CHONKER_FILE_MAGIC).await?;
        writer.write_u16_le(self.format_version).await?;
        writer.write_all(&[0u8; 14]).await?;
        Ok(())
    }
}
