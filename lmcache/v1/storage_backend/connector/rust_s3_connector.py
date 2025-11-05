# SPDX-License-Identifier: Apache-2.0
# wraps the Rust S3 connector to implement RemoteConnector interface
from typing import List, Optional
import asyncio

# First Party
from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import MemoryObj, BytesBufferMemoryObj
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector
from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend

logger = init_logger(__name__)


class RustS3Connector(RemoteConnector):
    """
    Python wrapper around the Rust S3 connector.
    Implements RemoteConnector interface using Rust backend.
    """

    def __init__(
        self,
        s3_endpoint: str,
        loop: asyncio.AbstractEventLoop,
        local_cpu_backend: LocalCPUBackend,
        s3_part_size: Optional[int],
        s3_file_prefix: Optional[str],
        s3_max_io_concurrency: int,
        s3_max_inflight_reqs: int,
        s3_prefer_http2: bool,
        s3_region: str,
        s3_enable_s3express: bool,
    ):
        if not s3_endpoint.startswith("s3://"):
            raise ValueError("S3 url must start with 's3://'")

        self.s3_endpoint = s3_endpoint
        self.s3_prefix = s3_file_prefix
        self.loop = loop
        self.local_cpu_backend = local_cpu_backend
        self.s3_part_size = s3_part_size
        self.s3_max_inflight_reqs = s3_max_inflight_reqs

        # import Rust connector (will be built by maturin)
        try:
            from lmcache_s3 import RustS3Connector as RustBackend
            self.rust_connector = RustBackend()
        except ImportError as e:
            raise ImportError(
                "Failed to import Rust S3 connector. "
                "Please build the Rust extension with: maturin develop"
            ) from e

    def post_init(self):
        """Initialize the Rust connector with configuration"""
        logger.info("Post-initializing Rust S3 connector")

        if self.s3_part_size is None:
            self.s3_part_size = self.full_chunk_size

        assert self.s3_part_size == self.full_chunk_size, (
            "S3 part size must be equal to chunk size in S3Connector"
        )

        # initialize Rust connector (now synchronous with GIL release)
        self.rust_connector.initialize(
            s3_endpoint=self.s3_endpoint,
            chunk_size=self.full_chunk_size,
            max_inflight_reqs=self.s3_max_inflight_reqs,
            s3_prefix=self.s3_prefix,
        )

    async def exists(self, key: CacheEngineKey) -> bool:
        """Check if key exists in S3"""
        return await asyncio.to_thread(self.rust_connector.exists, key.to_string())

    def exists_sync(self, key: CacheEngineKey) -> bool:
        """Check if key exists (synchronous)"""
        return self.rust_connector.exists_sync(key.to_string())

    async def get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        """
        Get object from S3 and return as MemoryObj.
        Uses Rust-side allocation with true async (no thread pool).
        """
        key_str = key.to_string()

        # await Rust coroutine directly (no thread pool needed!)
        # Rust async operation runs on Tokio runtime without GIL
        buffer = await self.rust_connector.get_allocated(key_str)

        if buffer is None:
            # object doesn't exist or has zero size
            return None

        # wrap bytes in BytesBufferMemoryObj (no copy, just wraps the buffer)
        memory_obj = BytesBufferMemoryObj(buffer)

        return memory_obj

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj):
        """
        Upload object to S3 from MemoryObj
        """
        key_str = key.to_string()

        assert memory_obj.get_physical_size() == self.s3_part_size, (
            "Saving unfull chunk is not supported in RustS3Connector."
        )

        # upload from memory object's data pointer
        await asyncio.to_thread(
            self.rust_connector.put,
            key_str,
            memory_obj.data_ptr,
            memory_obj.get_physical_size(),
        )

    async def list(self) -> List[str]:
        """List all keys (not implemented)"""
        raise NotImplementedError

    def support_ping(self) -> bool:
        """Ping not supported"""
        return False

    async def ping(self) -> int:
        """Ping not implemented"""
        raise NotImplementedError

    def support_batched_get(self) -> bool:
        """Batched get supported via batched_get_to_buffers"""
        return True

    async def batched_get(
        self, keys: List[CacheEngineKey]
    ) -> List[Optional[MemoryObj]]:
        """
        Batched get using ZERO-COPY direct buffer writes.
        Pre-allocates torch tensors and streams S3 data directly into them.
        """
        if not keys:
            return []

        # pre-allocate memory objects from LocalCPUBackend
        memory_objs = []
        buffer_ptrs = []
        buffer_sizes = []
        key_strs = []

        for key in keys:
            # allocate buffer from local CPU backend
            mem_obj = self.local_cpu_backend.allocate(self.full_chunk_size)
            memory_objs.append(mem_obj)
            buffer_ptrs.append(mem_obj.data_ptr)
            buffer_sizes.append(self.full_chunk_size)
            key_strs.append(key.to_string())

        # batched download directly to torch buffers (ZERO COPY!)
        results = await self.rust_connector.batched_get_to_buffers(
            key_strs, buffer_ptrs, buffer_sizes
        )

        # return MemoryObj for successful downloads, None for failures
        final_results = []
        for i, success in enumerate(results):
            if success:
                final_results.append(memory_objs[i])
            else:
                # free the buffer since download failed
                self.local_cpu_backend.free(memory_objs[i])
                final_results.append(None)

        return final_results

    def support_batched_async_contains(self) -> bool:
        """Batched contains supported"""
        return True

    async def batched_async_contains(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
        pin: bool = False,
    ) -> int:
        """
        Check which keys exist using batched HEAD requests.
        Returns count of existing keys.
        """
        if not keys:
            return 0

        key_strs = [key.to_string() for key in keys]

        # get bitmask indicating which keys exist
        bitmask_bytes = await self.rust_connector.batched_contains(key_strs)

        # count set bits in bitmask
        count = 0
        for byte in bitmask_bytes:
            count += bin(byte).count('1')

        return count

    def support_batched_get_non_blocking(self) -> bool:
        """Non-blocking batched get supported via batched_get_to_buffers"""
        return True

    async def batched_get_non_blocking(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
    ) -> List[MemoryObj]:
        """
        Non-blocking batched get - returns only successful downloads.
        Uses zero-copy direct-to-buffer writes.
        """
        results = await self.batched_get(keys)

        # filter out None values (failed downloads)
        return [mem_obj for mem_obj in results if mem_obj is not None]

    async def close(self):
        """Close and cleanup resources"""
        await asyncio.to_thread(self.rust_connector.close)
