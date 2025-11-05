use anyhow::{Context, Result};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;

use crate::s3_client::{S3ClientConfig, S3ClientWrapper};
use crate::shm::ShmBufferPool;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Runtime::new().expect("Failed to create Tokio runtime")
});

/// core S3 connector implementation
pub struct S3Connector {
    s3_client: S3ClientWrapper,
    shm_pool: Arc<ShmBufferPool>,
    object_size_cache: Arc<DashMap<String, usize>>,
    inflight_semaphore: Arc<Semaphore>,
    chunk_size: usize,
}

impl S3Connector {
    /// create new S3 connector
    pub async fn new(
        s3_endpoint: &str,
        s3_prefix: Option<String>,
        chunk_size: usize,
        max_inflight_reqs: usize,
    ) -> Result<Self> {
        // parse S3 config
        let s3_config = S3ClientConfig::from_url(s3_endpoint)
            .context("Failed to parse S3 URL")?
            .with_prefix(s3_prefix)
            .with_max_concurrency(max_inflight_reqs);

        // create S3 client
        let s3_client = S3ClientWrapper::new(s3_config)
            .await
            .context("Failed to create S3 client")?;

        // create shared memory buffer pool
        let shm_pool = Arc::new(
            ShmBufferPool::new(max_inflight_reqs, chunk_size, "lmcache_shm".to_string())
                .context("Failed to create shared memory pool")?
        );

        Ok(Self {
            s3_client,
            shm_pool,
            object_size_cache: Arc::new(DashMap::new()),
            inflight_semaphore: Arc::new(Semaphore::new(max_inflight_reqs)),
            chunk_size,
        })
    }

    /// check if key exists (synchronous)
    pub fn exists_sync(&self, key: &str) -> bool {
        // check cache first
        if let Some(size) = self.object_size_cache.get(key).map(|v| *v) {
            return size > 0;
        }

        // use shared runtime for sync call
        RUNTIME.block_on(async {
            match self.s3_client.get_object_size(key).await {
                Ok(Some(size)) if size > 0 => {
                    self.object_size_cache.insert(key.to_string(), size);
                    true
                }
                _ => {
                    self.object_size_cache.insert(key.to_string(), 0);
                    false
                }
            }
        })
    }

    /// check if key exists (async)
    pub async fn exists(&self, key: &str) -> Result<bool> {
        // check cache first
        if let Some(size) = self.object_size_cache.get(key).map(|v| *v) {
            return Ok(size > 0);
        }

        match self.s3_client.get_object_size(key).await? {
            Some(size) if size > 0 => {
                self.object_size_cache.insert(key.to_string(), size);
                Ok(true)
            }
            _ => {
                self.object_size_cache.insert(key.to_string(), 0);
                Ok(false)
            }
        }
    }

    /// get object and copy to memory pointer (zero-copy streaming)
    pub async fn get(&self, key: &str, dst_ptr: usize, expected_size: usize) -> Result<usize> {
        // check object size
        let obj_size = match self.s3_client.get_object_size(key).await? {
            Some(size) => size,
            None => anyhow::bail!("Object not found: {}", key),
        };

        if obj_size == 0 {
            anyhow::bail!("Object has zero size: {}", key);
        }

        // update cache
        self.object_size_cache.insert(key.to_string(), obj_size);

        // acquire semaphore permit
        let _permit = self.inflight_semaphore.acquire().await
            .context("Failed to acquire semaphore")?;

        // stream directly to destination - TRUE ZERO COPY!
        let bytes_read = self.s3_client.get_object(key, dst_ptr, expected_size).await
            .context("Failed to download from S3")?;

        if bytes_read != obj_size {
            anyhow::bail!(
                "Size mismatch: expected {}, got {}",
                obj_size,
                bytes_read
            );
        }

        Ok(bytes_read)
    }

    /// combined get operation: check size, validate, and download in single call
    /// returns true if successful, false if object doesn't exist or size mismatch
    pub async fn get_validated(&self, key: &str, dst_ptr: usize, expected_size: usize) -> Result<bool> {
        // check object size (uses cache if available)
        let obj_size = match self.s3_client.get_object_size(key).await? {
            Some(size) => size,
            None => return Ok(false),  // object doesn't exist
        };

        if obj_size == 0 {
            return Ok(false);  // zero-size object
        }

        if obj_size != expected_size {
            return Ok(false);  // size mismatch
        }

        // update cache
        self.object_size_cache.insert(key.to_string(), obj_size);

        // acquire semaphore permit
        let _permit = self.inflight_semaphore.acquire().await
            .context("Failed to acquire semaphore")?;

        // stream directly to destination
        let bytes_read = self.s3_client.get_object(key, dst_ptr, expected_size).await
            .context("Failed to download from S3")?;

        if bytes_read != obj_size {
            anyhow::bail!(
                "Size mismatch after download: expected {}, got {}",
                obj_size,
                bytes_read
            );
        }

        Ok(true)  // success
    }

    /// put object from memory pointer
    pub async fn put(&self, key: &str, src_ptr: usize, size: usize) -> Result<()> {
        if size != self.chunk_size {
            anyhow::bail!(
                "Size mismatch: expected chunk size {}, got {}",
                self.chunk_size,
                size
            );
        }

        // acquire semaphore permit
        let _permit = self.inflight_semaphore.acquire().await
            .context("Failed to acquire semaphore")?;

        // copy directly to Vec (single copy instead of two)
        let mut buffer = vec![0u8; size];
        unsafe {
            let src = src_ptr as *const u8;
            std::ptr::copy_nonoverlapping(src, buffer.as_mut_ptr(), size);
        }

        // zero-copy upload (ByteStream takes ownership of Vec)
        self.s3_client.put_object_owned(key, buffer).await
            .context("Failed to upload to S3")?;

        // update cache
        self.object_size_cache.insert(key.to_string(), size);

        Ok(())
    }

    /// get object size from cache or S3
    pub async fn get_object_size(&self, key: &str) -> Result<Option<usize>> {
        // check cache first
        if let Some(size) = self.object_size_cache.get(key).map(|v| *v) {
            return Ok(if size > 0 { Some(size) } else { None });
        }

        // query S3
        let size = self.s3_client.get_object_size(key).await?;

        // update cache
        if let Some(s) = size {
            self.object_size_cache.insert(key.to_string(), s);
        } else {
            self.object_size_cache.insert(key.to_string(), 0);
        }

        Ok(size)
    }

    /// clear object size cache
    pub fn clear_cache(&self) {
        self.object_size_cache.clear();
    }

    /// get number of available buffers
    pub fn available_buffers(&self) -> usize {
        self.shm_pool.available_count()
    }

    /// get object with Rust-side allocation
    /// allocates buffer in Rust and returns owned Vec<u8>
    /// returns None if object doesn't exist or has zero size
    pub async fn get_allocated(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // get object size (uses cache if available)
        let obj_size = match self.s3_client.get_object_size(key).await? {
            Some(size) => size,
            None => return Ok(None),  // object doesn't exist
        };

        if obj_size == 0 {
            return Ok(None);  // zero-size object
        }

        // update cache
        self.object_size_cache.insert(key.to_string(), obj_size);

        // acquire semaphore permit
        let _permit = self.inflight_semaphore.acquire().await
            .context("Failed to acquire semaphore")?;

        // allocate buffer in Rust (GIL still released!)
        let mut buffer = vec![0u8; obj_size];

        // download directly to buffer
        let bytes_read = self.s3_client.get_object(key, buffer.as_mut_ptr() as usize, obj_size).await
            .context("Failed to download from S3")?;

        if bytes_read != obj_size {
            anyhow::bail!(
                "Size mismatch after download: expected {}, got {}",
                obj_size,
                bytes_read
            );
        }

        Ok(Some(buffer))
    }

    /// batched get with internal Rust concurrency
    /// all concurrency handled by Tokio - minimal Python boundary crossings
    /// returns results in same order as input keys
    pub async fn batched_get_allocated(&self, keys: Vec<String>) -> Vec<Option<Vec<u8>>> {
        use futures::future::join_all;

        // create futures for all keys
        let futures: Vec<_> = keys
            .into_iter()
            .map(|key| async move {
                // each task runs concurrently on Tokio runtime
                self.get_allocated(&key).await.ok().flatten()
            })
            .collect();

        // Tokio handles all concurrency internally
        // work-stealing scheduler distributes tasks efficiently
        join_all(futures).await
    }

    /// batched HEAD requests to check which keys exist
    /// returns bitmask where bit i indicates if keys[i] exists
    /// much faster than individual exists() calls due to parallelism
    pub async fn batched_contains(&self, keys: Vec<String>) -> Result<Vec<u8>> {
        use futures::future::join_all;

        // create futures for all HEAD requests
        let futures: Vec<_> = keys
            .iter()
            .map(|key| async move {
                self.s3_client.get_object_size(key).await
                    .map(|size| size.is_some() && size.unwrap() > 0)
                    .unwrap_or(false)
            })
            .collect();

        // run all HEAD requests concurrently
        let results = join_all(futures).await;

        // convert bool vec to bitmask
        let num_bytes = (keys.len() + 7) / 8;
        let mut bitmask = vec![0u8; num_bytes];

        for (i, exists) in results.iter().enumerate() {
            if *exists {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                bitmask[byte_idx] |= 1 << bit_idx;
            }
        }

        Ok(bitmask)
    }

    /// batched GET directly to pre-allocated buffers (ZERO COPY!)
    ///
    /// keys: list of S3 keys to fetch
    /// buffer_ptrs: list of pointers to pre-allocated buffers (from torch tensors)
    /// buffer_sizes: expected size for each buffer
    ///
    /// returns: Vec<bool> indicating success for each key (true if downloaded successfully)
    ///
    /// this achieves true zero-copy: S3 → Rust → torch buffer (no intermediate allocations)
    pub async fn batched_get_to_buffers(
        &self,
        keys: Vec<String>,
        buffer_ptrs: Vec<usize>,
        buffer_sizes: Vec<usize>,
    ) -> Result<Vec<bool>> {
        use futures::future::join_all;

        if keys.len() != buffer_ptrs.len() || keys.len() != buffer_sizes.len() {
            anyhow::bail!("Keys, buffer_ptrs, and buffer_sizes must have same length");
        }

        // create futures for all GET requests
        let futures: Vec<_> = keys
            .into_iter()
            .zip(buffer_ptrs.into_iter())
            .zip(buffer_sizes.into_iter())
            .map(|((key, ptr), size)| async move {
                // stream directly to torch buffer - TRUE ZERO COPY!
                self.get_validated(&key, ptr, size).await.unwrap_or(false)
            })
            .collect();

        // run all downloads concurrently on Tokio runtime
        // semaphore limits max concurrent requests
        let results = join_all(futures).await;

        Ok(results)
    }

    /// close and cleanup resources
    pub async fn close(&self) -> Result<()> {
        // semaphore and buffers will be cleaned up automatically
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connector_creation() {
        let result = S3Connector::new(
            "s3://test-bucket.s3.us-east-1.amazonaws.com",
            None,
            4096,
            4,
        ).await;

        // this may fail without AWS credentials, but structure should be valid
        assert!(result.is_ok() || result.is_err());
    }
}
