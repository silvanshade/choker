pub(crate) mod chunk;
pub(crate) mod header;
pub(crate) mod meta;

use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite};

use crate::{
    archive::{header::ChonkerArchiveHeader, meta::ChonkerArchiveMeta},
    codec::{decode::DecodeContext, encode::EncodeContext},
};

pub struct ChonkerArchive;

// Archive bytes layout:
//
// | offset           |                  size | description                                           |
// |------------------|-----------------------|-------------------------------------------------------|
// |         0        |                    16 | Archive file magic ("CHONKERFILE\xF0\x9F\x98\xB8\0"). |
// |        16        |                     2 | Archive format version (u64 le).                      |
// |        18        |                    14 | Padding.                                              |
// |        32        | size - 40 - meta - 32 | Compressed data.                                      |
// | size - 40 - meta |             meta      | Compressed meta.                                      |
// | size - 40        |         8             | Size of compressed meta (u64 le).                     |
// | size - 32        |        32             | Checksum of [size - 40 - meta .. size - 32].          |

// NOTE: Currently we construct the archive in post-order, where the meta is at the end of the file.
// This allows for more efficient encoding (since we can stream out encoded bytes as they are
// created without buffering) but is not suitable for streaming decoding. We might consider allowing
// this to be configured by the encoder in the future. In this case, we can use the padding bytes to
// specify some additional data describing the layout strategy.

impl ChonkerArchive {
    pub async fn create<R, W>(
        context: Arc<EncodeContext>,
        reader: R,
        reader_size: Option<u64>,
        writer: &mut W,
    ) -> crate::BoxResult<ChonkerArchiveMeta>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        let header = ChonkerArchiveHeader::default();
        header.write(&context, writer).await?;
        let meta = crate::codec::encode::encode_chunks(context.clone(), reader, reader_size, writer).await?;
        meta.write(&context, writer).await?;
        Ok(meta)
    }

    pub async fn decode<'ctx, R, W>(
        context: &'ctx mut DecodeContext,
        reader: &mut R,
        reader_size: u64,
        writer: &mut W,
    ) -> crate::BoxResult<&'ctx rkyv::Archived<ChonkerArchiveMeta>>
    where
        R: AsyncRead + AsyncSeek + Unpin,
        W: AsyncWrite + Unpin,
    {
        let header = ChonkerArchiveHeader::read(context, reader).await?;
        let (meta_size, meta) = ChonkerArchiveMeta::read(context, reader, reader_size).await?;
        reader.seek(std::io::SeekFrom::Start(32)).await?;
        let reader = reader.take(reader_size - 16 - 2 - 14 - meta_size - 8 - 32);
        Ok(meta)
    }
}

#[cfg(test)]
mod test {
    use std::io::Read;

    #[test]
    fn test() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(1);

        let bytes = rkyv::to_bytes::<HashSet<u64>, 256>(&set).unwrap();
        dbg!(&bytes);

        let crashing_bytes: [u8; 28] = [
            0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 236, 255, 255, 255, 240, 255, 255, 255,
        ];

        assert_eq!(&bytes[..], &crashing_bytes[..]);

        // let crashing_bytes = crashing_bytes.to_vec();
        let mut crashing_vec = rkyv::AlignedVec::with_capacity(crashing_bytes.len());
        // unsafe {
        //     std::ptr::copy_nonoverlapping(crashing_bytes.as_ptr(), crashing_vec.as_mut_ptr(), crashing_bytes.len());
        //     crashing_vec.set_len(crashing_bytes.len());
        // }
        let mut reader = std::io::Cursor::new(&crashing_bytes);
        unsafe { crashing_vec.set_len(crashing_bytes.len()) };
        reader.read_exact(crashing_vec.as_mut_slice()).unwrap();
        let archived = unsafe { rkyv::archived_root::<HashSet<u64>>(&crashing_vec) };
        assert_eq!(*archived.iter().next().unwrap(), 1);
    }
}
