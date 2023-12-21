use rkyv::bytecheck;

#[cfg_attr(feature = "debug", derive(Debug))]
#[derive(Clone, rkyv::Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, rkyv::CheckBytes))]
pub(crate) enum ArchiveChunk {
    Data { hash: [u8; 32], length: u64, offset: u64 },
    Dupe { pointer: u64 },
}

impl Default for ArchiveChunk {
    fn default() -> Self {
        let hash = <[u8; 32]>::default();
        let length = u64::default();
        let offset = u64::default();
        ArchiveChunk::Data { hash, length, offset }
    }
}
