use crate::BoxResult;

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

pub async fn fast_seq_open<P>(path: P) -> BoxResult<tokio::fs::File>
where
    P: AsRef<std::path::Path>,
{
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(winapi::um::winbase::FILE_FLAG_SEQUENTIAL_SCAN)
            .open(path)
    }
    #[cfg(not(windows))]
    {
        let file = tokio::fs::File::open(path).await?;

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;

            unsafe {
                #[cfg(target_os = "macos")]
                libc::fcntl(file.as_raw_fd(), libc::F_RDAHEAD, 1);

                #[cfg(not(target_os = "macos"))]
                libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
            }
        }

        Ok(file)
    }
}
