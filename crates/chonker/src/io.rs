pub(crate) trait AsZstdWriteBuf {
    type WriteBuf<'a>: zstd::zstd_safe::WriteBuf
    where
        Self: 'a;

    fn as_zstd_write_buf(&mut self) -> Self::WriteBuf<'_>;
}

pub(crate) struct AlignedVecZstdWriteBuf<'a>(pub(crate) &'a mut rkyv::AlignedVec);

unsafe impl zstd::zstd_safe::WriteBuf for AlignedVecZstdWriteBuf<'_> {
    fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    fn capacity(&self) -> usize {
        self.0.capacity()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }

    unsafe fn filled_until(&mut self, n: usize) {
        self.0.set_len(n);
    }
}

impl AsZstdWriteBuf for rkyv::AlignedVec {
    type WriteBuf<'a> = AlignedVecZstdWriteBuf<'a>;

    fn as_zstd_write_buf(&mut self) -> Self::WriteBuf<'_> {
        AlignedVecZstdWriteBuf(self)
    }
}
