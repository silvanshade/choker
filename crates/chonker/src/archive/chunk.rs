use rkyv::bytecheck;

#[cfg_attr(feature = "debug", derive(Debug))]
#[derive(Clone, Copy, Default, rkyv::Archive, rkyv::Serialize)]
#[archive_attr(derive(Clone, Copy, Debug, rkyv::CheckBytes))]
pub(crate) struct ArchiveChunk {
    pub(crate) checksum: [u8; 32],
    pub(crate) src_offset: u64,
    pub(crate) src_length: u32,
    pub(crate) arc_offset: u64,
    pub(crate) arc_length: u32,
}
