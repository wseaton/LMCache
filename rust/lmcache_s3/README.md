# LMCache Rust S3 Connector

high-performance Rust implementation of the S3 connector for LMCache, providing improved performance, memory safety, and better async handling compared to the Python implementation.

## Architecture

The implementation consists of three layers:

1. **Rust Core** (`src/`): Native S3 operations using `aws-sdk-s3`
   - `connector.rs`: Main connector logic with async S3 operations
   - `s3_client.rs`: AWS SDK wrapper for S3 operations
   - `shm.rs`: Shared memory buffer pool for zero-copy transfers
   - `lib.rs`: PyO3 bindings exposing Rust to Python

2. **Python Wrapper** (`lmcache/v1/storage_backend/connector/rust_s3_connector.py`):
   - Implements `RemoteConnector` interface
   - Integrates with `LocalCPUBackend` memory allocator
   - Handles `MemoryObj` lifecycle

3. **Adapter Integration** (`lmcache/v1/storage_backend/connector/s3_adapter.py`):
   - Selects between Rust and Python implementations via env var
   - Maintains backward compatibility

## Features

### Implemented (Core Features)
- ✅ Async S3 operations (GET, PUT, HEAD)
- ✅ Shared memory buffer pool for efficient transfers
- ✅ Object size caching to reduce HEAD requests
- ✅ Concurrency control with semaphores
- ✅ Zero-copy memory transfers
- ✅ Python asyncio integration via PyO3

### Future Enhancements
- ⏳ Batched operations (`batched_get`, `batched_put`)
- ⏳ Priority queue for operation scheduling
- ⏳ Non-blocking prefetch operations
- ⏳ S3 Express support
- ⏳ Multipart upload for large chunks

## Building

### Prerequisites
- Rust toolchain (1.70+)
- Python 3.10+
- maturin (`pip install maturin` or included in build deps)

### Development Build
```bash
# build and install in development mode
cd rust/lmcache_s3
maturin develop --release

# or build wheel for distribution
maturin build --release --out dist/
```

### Integration with LMCache Build
The Rust extension is configured in `pyproject.toml` and will be built automatically when building LMCache with the appropriate build tools installed.

## Usage

### Environment Variables
- `LMCACHE_USE_RUST_S3=1` (default): Use Rust S3 connector
- `LMCACHE_USE_RUST_S3=0`: Fall back to Python implementation

### Example
```python
import asyncio
from lmcache.v1.storage_backend.connector import CreateConnector
from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend
from lmcache.v1.config import LMCacheEngineConfig

# configuration
config = LMCacheEngineConfig(
    extra_config={
        "s3_region": "us-east-1",
        "s3_max_inflight_reqs": 64,
        "s3_file_prefix": "lmcache",
    }
)

loop = asyncio.get_event_loop()
local_backend = LocalCPUBackend(config)

# create connector (automatically uses Rust if available)
connector = CreateConnector(
    "s3://my-bucket.s3.us-east-1.amazonaws.com",
    loop,
    local_backend,
    config=config
)

# use connector
key = CacheEngineKey(...)
memory_obj = await connector.get(key)
await connector.put(key, memory_obj)
await connector.close()
```

## Performance Considerations

### Shared Memory Buffers
The Rust implementation uses `/dev/shm` for temporary buffers during S3 transfers. This provides:
- Memory pooling to avoid repeated allocations
- Potential for zero-copy operations
- Better performance for large KV cache chunks (multi-MB)

Buffer pool size is controlled by `s3_max_inflight_reqs` (default: 64).

### Async Runtime
Uses Tokio for async operations, which provides:
- Efficient task scheduling
- Better CPU utilization
- Lower latency for concurrent S3 requests

## Testing

### Unit Tests (Rust)
```bash
cd rust/lmcache_s3
cargo test
```

### Integration Tests (Python)
```bash
# test bindings without CUDA
python test_rust_s3_bindings.py

# full integration tests (requires CUDA)
pytest tests/v1/storage_backend/test_rust_s3_connector.py
```

## Troubleshooting

### Import Error: "No module named 'lmcache_s3'"
The Rust extension isn't built or installed. Run:
```bash
maturin develop --release
```

### Build Error: "CUDA_HOME not set"
This is from the main LMCache CUDA extensions, not the Rust S3 connector.
Build the Rust extension separately:
```bash
maturin build --release --manifest-path rust/lmcache_s3/Cargo.toml
pip install target/wheels/*.whl
```

### Runtime Error: "Connector not initialized"
Call `post_init()` on the connector after creation and metadata setup.

## Development

### Code Structure
```
rust/lmcache_s3/
├── Cargo.toml          # Rust dependencies
├── src/
│   ├── lib.rs          # PyO3 module entry point
│   ├── connector.rs    # Core S3 connector
│   ├── s3_client.rs    # AWS SDK wrapper
│   ├── shm.rs          # Shared memory pool
│   └── types.rs        # Common types
└── tests/              # Rust unit tests
```

### Adding Features
1. Implement in Rust core (`src/connector.rs`)
2. Add PyO3 bindings (`src/lib.rs`)
3. Update Python wrapper (`rust_s3_connector.py`)
4. Add tests

### Dependencies
See `Cargo.toml` for full list. Key dependencies:
- `pyo3 = "0.20"`: Python bindings
- `pyo3-asyncio = "0.20"`: Async Python integration
- `tokio = "1.42"`: Async runtime
- `aws-sdk-s3 = "1.108"`: S3 client
- `memmap2 = "0.9"`: Memory mapping for shm

## License

Apache-2.0 (same as LMCache)
