mod connector;
mod s3_client;
mod shm;
mod types;

use once_cell::sync::{Lazy, OnceCell};
use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyModule;
use std::sync::Arc;
use tokio::runtime::Runtime;

use connector::S3Connector;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Runtime::new().expect("Failed to create Tokio runtime")
});

/// python-facing S3 connector class
#[pyclass]
pub struct RustS3Connector {
    connector: Arc<OnceCell<S3Connector>>,
}

#[pymethods]
impl RustS3Connector {
    #[new]
    fn new() -> Self {
        Self {
            connector: Arc::new(OnceCell::new()),
        }
    }

    /// initialize the connector (must be called after construction)
    #[pyo3(signature = (s3_endpoint, chunk_size, max_inflight_reqs, s3_prefix=None))]
    fn initialize(
        &self,
        py: Python<'_>,
        s3_endpoint: String,
        chunk_size: usize,
        max_inflight_reqs: usize,
        s3_prefix: Option<String>,
    ) -> PyResult<()> {
        let connector_arc = self.connector.clone();

        py.allow_threads(|| {
            let connector = RUNTIME.block_on(async move {
                S3Connector::new(
                    &s3_endpoint,
                    s3_prefix,
                    chunk_size,
                    max_inflight_reqs,
                ).await
            })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to initialize S3 connector: {}", e)))?;

            connector_arc.set(connector)
                .map_err(|_| PyRuntimeError::new_err("Connector already initialized"))?;

            Ok(())
        })
    }

    /// check if key exists (sync version)
    fn exists_sync(&self, key: String) -> PyResult<bool> {
        let connector = self.connector.get()
            .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

        Ok(connector.exists_sync(&key))
    }

    /// check if key exists (async version)
    fn exists(&self, py: Python<'_>, key: String) -> PyResult<bool> {
        let connector_arc = self.connector.clone();

        py.allow_threads(|| {
            let connector = connector_arc.get()
                .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

            RUNTIME.block_on(async move {
                connector.exists(&key)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("exists failed: {}", e)))
            })
        })
    }

    /// get object and copy to memory location
    fn get(
        &self,
        py: Python<'_>,
        key: String,
        dst_ptr: usize,
        expected_size: usize,
    ) -> PyResult<usize> {
        let connector_arc = self.connector.clone();

        py.allow_threads(|| {
            let connector = connector_arc.get()
                .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

            RUNTIME.block_on(async move {
                connector.get(&key, dst_ptr, expected_size)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("get failed: {}", e)))
            })
        })
    }

    /// combined get operation: check size, validate, and download in single call
    /// returns true if successful, false if object doesn't exist or size mismatch
    fn get_validated(
        &self,
        py: Python<'_>,
        key: String,
        dst_ptr: usize,
        expected_size: usize,
    ) -> PyResult<bool> {
        let connector_arc = self.connector.clone();

        py.allow_threads(|| {
            let connector = connector_arc.get()
                .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

            RUNTIME.block_on(async move {
                connector.get_validated(&key, dst_ptr, expected_size)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("get_validated failed: {}", e)))
            })
        })
    }

    /// put object from memory location
    fn put(
        &self,
        py: Python<'_>,
        key: String,
        src_ptr: usize,
        size: usize,
    ) -> PyResult<()> {
        let connector_arc = self.connector.clone();

        py.allow_threads(|| {
            let connector = connector_arc.get()
                .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

            RUNTIME.block_on(async move {
                connector.put(&key, src_ptr, size)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("put failed: {}", e)))
            })
        })
    }

    /// get object size
    fn get_object_size(
        &self,
        py: Python<'_>,
        key: String,
    ) -> PyResult<Option<usize>> {
        let connector_arc = self.connector.clone();

        py.allow_threads(|| {
            let connector = connector_arc.get()
                .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

            RUNTIME.block_on(async move {
                connector.get_object_size(&key)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("get_object_size failed: {}", e)))
            })
        })
    }

    /// get object with Rust-side allocation
    /// returns Python coroutine that resolves to bytes or None
    /// entire operation (allocate + download) happens without GIL
    fn get_allocated<'p>(
        &self,
        py: Python<'p>,
        key: String,
    ) -> PyResult<&'p PyAny> {
        let connector_arc = self.connector.clone();

        // return Python coroutine that Python can await directly
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let connector = connector_arc.get()
                .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

            // perform async get (no GIL held during this!)
            let buffer_opt = connector.get_allocated(&key)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("get_allocated failed: {}", e)))?;

            // convert to Python bytes
            Python::with_gil(|py| {
                match buffer_opt {
                    Some(buffer) => {
                        let py_bytes = pyo3::types::PyBytes::new(py, &buffer);
                        let py_obj: Py<pyo3::PyAny> = py_bytes.into_py(py);
                        Ok(Some(py_obj))
                    }
                    None => Ok(None),
                }
            })
        })
    }

    /// batched get with internal Rust concurrency (work queue pattern)
    /// takes list of keys, returns list of bytes/None in same order
    /// ALL concurrency handled internally by Tokio - only 2 boundary crossings total!
    fn batched_get_allocated<'p>(
        &self,
        py: Python<'p>,
        keys: Vec<String>,
    ) -> PyResult<&'p PyAny> {
        let connector_arc = self.connector.clone();

        // return Python coroutine
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let connector = connector_arc.get()
                .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

            // perform batched get - ALL concurrency happens in Rust/Tokio
            // no per-operation boundary crossings!
            let results = connector.batched_get_allocated(keys).await;

            // convert results to Python list
            Python::with_gil(|py| {
                let py_list = pyo3::types::PyList::empty(py);
                for result_opt in results {
                    match result_opt {
                        Some(buffer) => {
                            let py_bytes = pyo3::types::PyBytes::new(py, &buffer);
                            py_list.append(py_bytes)?;
                        }
                        None => {
                            py_list.append(py.None())?;
                        }
                    }
                }
                let result: Py<pyo3::PyAny> = py_list.into_py(py);
                Ok(result)
            })
        })
    }

    /// batched HEAD requests to check which keys exist
    /// returns bitmask as bytes where bit i indicates if keys[i] exists
    /// e.g., for 10 keys, returns 2 bytes: [0b00000111, 0b00000011] means keys 0,1,2,8,9 exist
    fn batched_contains<'p>(
        &self,
        py: Python<'p>,
        keys: Vec<String>,
    ) -> PyResult<&'p PyAny> {
        let connector_arc = self.connector.clone();

        // return Python coroutine
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let connector = connector_arc.get()
                .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

            // perform batched contains check - all HEAD requests run concurrently
            let bitmask = connector.batched_contains(keys)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("batched_contains failed: {}", e)))?;

            // convert to Python bytes
            Python::with_gil(|py| {
                let py_bytes = pyo3::types::PyBytes::new(py, &bitmask);
                let result: Py<pyo3::PyAny> = py_bytes.into_py(py);
                Ok(result)
            })
        })
    }

    /// batched GET directly to pre-allocated buffers (ZERO COPY!)
    ///
    /// keys: list of S3 keys to fetch
    /// buffer_ptrs: list of memory addresses (from torch tensor.data_ptr())
    /// buffer_sizes: expected size for each buffer (should match chunk_size)
    ///
    /// returns: list of bool indicating success for each key
    ///
    /// this achieves true zero-copy: S3 stream → Rust → torch buffer
    /// no intermediate allocations, no memcpy to Python!
    fn batched_get_to_buffers<'p>(
        &self,
        py: Python<'p>,
        keys: Vec<String>,
        buffer_ptrs: Vec<usize>,
        buffer_sizes: Vec<usize>,
    ) -> PyResult<&'p PyAny> {
        let connector_arc = self.connector.clone();

        // return Python coroutine
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let connector = connector_arc.get()
                .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

            // perform batched get to buffers - TRUE ZERO COPY!
            let results = connector.batched_get_to_buffers(keys, buffer_ptrs, buffer_sizes)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("batched_get_to_buffers failed: {}", e)))?;

            // convert to Python list
            Python::with_gil(|py| {
                let py_list = pyo3::types::PyList::empty(py);
                for success in results {
                    py_list.append(success)?;
                }
                let result: Py<pyo3::PyAny> = py_list.into_py(py);
                Ok(result)
            })
        })
    }

    /// clear the object size cache
    fn clear_cache(&self) -> PyResult<()> {
        let connector = self.connector.get()
            .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

        connector.clear_cache();
        Ok(())
    }

    /// get number of available buffers
    fn available_buffers(&self) -> PyResult<usize> {
        let connector = self.connector.get()
            .ok_or_else(|| PyRuntimeError::new_err("Connector not initialized"))?;

        Ok(connector.available_buffers())
    }

    /// close and cleanup
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let connector_arc = self.connector.clone();

        py.allow_threads(|| {
            if let Some(connector) = connector_arc.get() {
                RUNTIME.block_on(async move {
                    connector.close()
                        .await
                        .map_err(|e| PyRuntimeError::new_err(format!("close failed: {}", e)))
                })
            } else {
                Ok(())
            }
        })
    }
}


/// python module definition
#[pymodule]
fn lmcache_s3(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RustS3Connector>()?;
    Ok(())
}
