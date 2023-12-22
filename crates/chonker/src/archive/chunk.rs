use rkyv::bytecheck;

#[cfg_attr(feature = "debug", derive(Debug))]
#[derive(Clone, rkyv::Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, rkyv::CheckBytes))]
pub(crate) enum ArchiveChunk {
    Data {
        checksum: [u8; 32],
        src_offset: u64,
        src_length: u32,
        arc_offset: u64,
        arc_length: u32,
    },
    Dupe {
        index: u64,
    },
}

impl Default for ArchiveChunk {
    fn default() -> Self {
        ArchiveChunk::Data {
            checksum: <[u8; 32]>::default(),
            src_offset: u64::default(),
            src_length: u32::default(),
            arc_offset: u64::default(),
            arc_length: u32::default(),
        }
    }
}
