use byteorder::ReadBytesExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::codec::{decode::DecodeContext, encode::context::EncodeContext};

#[derive(Clone)]
pub(crate) struct ChonkerArchiveHeader {
    format_version: [u16; 4],
}

impl Default for ChonkerArchiveHeader {
    fn default() -> Self {
        Self {
            format_version: Self::CHONKER_FILE_FORMAT_V0,
        }
    }
}

impl ChonkerArchiveHeader {
    const CHONKER_FILE_FORMAT_V0: [u16; 4] = [0u16, 0u16, 0u16, 0u16];
    const CHONKER_FILE_HEADER_PADDING: &'static [u8] = &[0u8; 8];

    const CHONKER_META_MAGIC: [u8; 16] = *b"CHONKERMETA\xF0\x9F\x98\xB8\0";
    pub(crate) const CHONKER_META_HEADER_OFFSET: usize =
        Self::CHONKER_META_MAGIC.len() + core::mem::size_of::<u64>() + Self::CHONKER_FILE_HEADER_PADDING.len();

    const CHONKER_DATA_MAGIC: [u8; 16] = *b"CHONKERDATA\xF0\x9F\x98\xB8\0";
    pub(crate) const CHONKER_DATA_HEADER_OFFSET: usize =
        Self::CHONKER_DATA_MAGIC.len() + core::mem::size_of::<u64>() + Self::CHONKER_FILE_HEADER_PADDING.len();

    pub(crate) async fn read_meta<R>(
        _context: &DecodeContext,
        meta_reader: &mut R,
    ) -> crate::BoxResult<ChonkerArchiveHeader>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let buf = &mut [0u8; Self::CHONKER_META_MAGIC.len()];
        meta_reader.read_exact(buf).await?;
        if buf != &Self::CHONKER_META_MAGIC {
            return Err(crate::BoxError::from("invalid file magic"));
        }

        let format_major_version = meta_reader.read_u16_le().await?;
        let format_minor_version = meta_reader.read_u16_le().await?;
        let format_patch_version = meta_reader.read_u16_le().await?;
        let format_tweak_version = meta_reader.read_u16_le().await?;
        let format_version = [
            format_major_version,
            format_minor_version,
            format_patch_version,
            format_tweak_version,
        ];

        let buf = &mut [0u8; Self::CHONKER_FILE_HEADER_PADDING.len()];
        meta_reader.read_exact(buf).await?;
        if buf != Self::CHONKER_FILE_HEADER_PADDING {
            return Err(crate::BoxError::from("invalid padding"));
        }

        Ok(ChonkerArchiveHeader { format_version })
    }

    pub(crate) async fn read_data<R>(
        _context: &DecodeContext,
        data_reader: &mut R,
    ) -> crate::BoxResult<ChonkerArchiveHeader>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let buf = &mut [0u8; Self::CHONKER_DATA_MAGIC.len()];
        data_reader.read_exact(buf).await?;
        if buf != &Self::CHONKER_DATA_MAGIC {
            return Err(crate::BoxError::from("invalid file magic"));
        }

        let format_major_version = data_reader.read_u16_le().await?;
        let format_minor_version = data_reader.read_u16_le().await?;
        let format_patch_version = data_reader.read_u16_le().await?;
        let format_tweak_version = data_reader.read_u16_le().await?;
        let format_version = [
            format_major_version,
            format_minor_version,
            format_patch_version,
            format_tweak_version,
        ];

        let buf = &mut [0u8; Self::CHONKER_FILE_HEADER_PADDING.len()];
        data_reader.read_exact(buf).await?;
        if buf != Self::CHONKER_FILE_HEADER_PADDING {
            return Err(crate::BoxError::from("invalid padding"));
        }

        Ok(ChonkerArchiveHeader { format_version })
    }

    pub(crate) async fn write_meta<MW>(&self, _context: &EncodeContext, meta_writer: &mut MW) -> crate::BoxResult<()>
    where
        MW: tokio::io::AsyncWrite + Unpin,
    {
        meta_writer.write_all(&Self::CHONKER_META_MAGIC).await?;

        meta_writer.write_u16_le(self.format_version[0]).await?;
        meta_writer.write_u16_le(self.format_version[1]).await?;
        meta_writer.write_u16_le(self.format_version[2]).await?;
        meta_writer.write_u16_le(self.format_version[3]).await?;

        meta_writer
            .write_all(&[0u8; Self::CHONKER_FILE_HEADER_PADDING.len()])
            .await?;
        Ok(())
    }

    pub(crate) async fn write_data<DW>(&self, _context: &EncodeContext, data_writer: &mut DW) -> crate::BoxResult<()>
    where
        DW: tokio::io::AsyncWrite + Unpin,
    {
        data_writer.write_all(&Self::CHONKER_DATA_MAGIC).await?;

        data_writer.write_u16_le(self.format_version[0]).await?;
        data_writer.write_u16_le(self.format_version[1]).await?;
        data_writer.write_u16_le(self.format_version[2]).await?;
        data_writer.write_u16_le(self.format_version[3]).await?;

        data_writer
            .write_all(&[0u8; Self::CHONKER_FILE_HEADER_PADDING.len()])
            .await?;
        Ok(())
    }
}
