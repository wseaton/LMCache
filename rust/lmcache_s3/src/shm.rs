use anyhow::{Context, Result};
use memmap2::MmapMut;
use parking_lot::Mutex;
use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;

/// represents a single shared memory buffer
#[derive(Debug)]
pub struct ShmBuffer {
    pub name: String,
    #[allow(dead_code)]
    pub mmap: MmapMut,
    pub ptr: *mut u8,
    pub size: usize,
}

// safety: raw pointer is managed by mmap which is thread-safe
unsafe impl Send for ShmBuffer {}
unsafe impl Sync for ShmBuffer {}

impl ShmBuffer {
    /// create new shared memory buffer in /dev/shm (Linux) or /tmp (macOS)
    pub fn new(name: String, size: usize) -> Result<Self> {
        #[cfg(target_os = "linux")]
        let base_path = "/dev/shm";

        #[cfg(target_os = "macos")]
        let base_path = "/tmp";

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let base_path = "/tmp";

        let path = PathBuf::from(base_path).join(&name);

        // create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .with_context(|| format!("Failed to create shm file: {:?}", path))?;

        // set file size
        file.set_len(size as u64)
            .context("Failed to set shm file size")?;

        // create memory map
        let mut mmap = unsafe {
            MmapMut::map_mut(file.as_raw_fd())
                .context("Failed to create memory map")?
        };

        let ptr = mmap.as_mut_ptr();

        Ok(Self {
            name,
            mmap,
            ptr,
            size,
        })
    }

    /// get raw pointer to buffer (for memcpy operations)
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// get mutable raw pointer to buffer
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// get buffer as slice
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    /// get buffer as mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

impl Drop for ShmBuffer {
    fn drop(&mut self) {
        // unlink the shared memory file
        #[cfg(target_os = "linux")]
        let base_path = "/dev/shm";

        #[cfg(target_os = "macos")]
        let base_path = "/tmp";

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let base_path = "/tmp";

        let path = PathBuf::from(base_path).join(&self.name);
        let _ = std::fs::remove_file(path);
    }
}

/// pool of shared memory buffers for S3 operations
pub struct ShmBufferPool {
    buffers: Arc<Mutex<Vec<ShmBuffer>>>,
    #[allow(dead_code)]
    buffer_size: usize,
    #[allow(dead_code)]
    name_prefix: String,
}

impl ShmBufferPool {
    /// create new buffer pool
    pub fn new(num_buffers: usize, buffer_size: usize, name_prefix: String) -> Result<Self> {
        let mut buffers = Vec::with_capacity(num_buffers);

        for i in 0..num_buffers {
            let name = format!("{}_{}.part", name_prefix, i);
            let buffer = ShmBuffer::new(name, buffer_size)
                .with_context(|| format!("Failed to create buffer {}", i))?;
            buffers.push(buffer);
        }

        Ok(Self {
            buffers: Arc::new(Mutex::new(buffers)),
            buffer_size,
            name_prefix,
        })
    }

    /// allocate a buffer from the pool (blocking until available)
    pub fn allocate(&self) -> Result<ShmBuffer> {
        loop {
            {
                let mut buffers = self.buffers.lock();
                if let Some(buffer) = buffers.pop() {
                    return Ok(buffer);
                }
            }
            // buffer pool exhausted, wait briefly
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
    }

    /// try to allocate a buffer without blocking
    #[allow(dead_code)]
    pub fn try_allocate(&self) -> Option<ShmBuffer> {
        let mut buffers = self.buffers.lock();
        buffers.pop()
    }

    /// return buffer to pool
    pub fn free(&self, buffer: ShmBuffer) {
        let mut buffers = self.buffers.lock();
        buffers.push(buffer);
    }

    /// get buffer size
    #[allow(dead_code)]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// get number of available buffers
    pub fn available_count(&self) -> usize {
        self.buffers.lock().len()
    }
}

impl Drop for ShmBufferPool {
    fn drop(&mut self) {
        // buffers will be dropped and cleaned up automatically
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_buffer_creation() {
        let name = "test_buffer_1".to_string();
        let size = 4096;
        let buffer = ShmBuffer::new(name.clone(), size).unwrap();

        assert_eq!(buffer.size, size);
        assert_eq!(buffer.name, name);

        // verify we can write and read
        let mut_slice = unsafe {
            std::slice::from_raw_parts_mut(buffer.ptr, size)
        };
        mut_slice[0] = 42;
        assert_eq!(mut_slice[0], 42);
    }

    #[test]
    fn test_buffer_pool() {
        let pool = ShmBufferPool::new(4, 4096, "test_pool".to_string()).unwrap();
        assert_eq!(pool.available_count(), 4);

        // allocate buffers
        let buf1 = pool.allocate().unwrap();
        assert_eq!(pool.available_count(), 3);

        let buf2 = pool.allocate().unwrap();
        assert_eq!(pool.available_count(), 2);

        // return buffers
        pool.free(buf1);
        assert_eq!(pool.available_count(), 3);

        pool.free(buf2);
        assert_eq!(pool.available_count(), 4);
    }
}
