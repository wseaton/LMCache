# SPDX-License-Identifier: Apache-2.0
# Standard
import os

# First Party
from lmcache.logging import init_logger
from lmcache.v1.storage_backend.connector import (
    ConnectorAdapter,
    ConnectorContext,
)
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

logger = init_logger(__name__)

# environment variables to control which implementation to use
USE_CRT_S3 = os.getenv("LMCACHE_USE_CRT_S3", "0") == "1"
USE_RUST_S3 = os.getenv("LMCACHE_USE_RUST_S3", "0") == "1"


class S3ConnectorAdapter(ConnectorAdapter):
    """Adapter for S3 Server connectors."""

    def __init__(self) -> None:
        super().__init__("s3://")

    def create_connector(self, context: ConnectorContext) -> RemoteConnector:
        config = context.config
        assert config is not None

        if config.extra_config is not None:
            # Different parts can be transferred in parallel.
            self.s3_part_size = config.extra_config.get("s3_part_size", None)
            self.s3_max_io_concurrency = config.extra_config.get(
                "s3_max_io_concurrency", 64
            )
            self.s3_max_inflight_reqs = config.extra_config.get(
                "s3_max_inflight_reqs", 64
            )
            self.s3_prefer_http2 = config.extra_config.get("s3_prefer_http2", True)
            self.s3_region = config.extra_config.get("s3_region", None)
            self.s3_enable_s3express = config.extra_config.get(
                "s3_enable_s3express", True
            )
            self.s3_file_prefix = config.extra_config.get("s3_file_prefix", None)

        logger.info(f"Creating S3 connector for URL: {context.url}")

        s3_endpoint = context.url

        # choose implementation based on environment variables
        # priority: CRT > Rust > Python
        if USE_CRT_S3:
            logger.info("Using C++ AWS CRT S3 connector implementation")
            # Local
            from .crt_s3_connector import CrtS3Connector

            return CrtS3Connector(
                s3_endpoint=s3_endpoint,
                loop=context.loop,
                local_cpu_backend=context.local_cpu_backend,
                s3_part_size=self.s3_part_size,
                s3_file_prefix=self.s3_file_prefix,
                s3_max_io_concurrency=self.s3_max_io_concurrency,
                s3_max_inflight_reqs=self.s3_max_inflight_reqs,
                s3_prefer_http2=self.s3_prefer_http2,
                s3_region=self.s3_region,
                s3_enable_s3express=self.s3_enable_s3express,
            )
        elif USE_RUST_S3:
            logger.info("Using Rust S3 connector implementation")
            # Local
            from .rust_s3_connector import RustS3Connector

            return RustS3Connector(
                s3_endpoint=s3_endpoint,
                loop=context.loop,
                local_cpu_backend=context.local_cpu_backend,
                s3_part_size=self.s3_part_size,
                s3_file_prefix=self.s3_file_prefix,
                s3_max_io_concurrency=self.s3_max_io_concurrency,
                s3_max_inflight_reqs=self.s3_max_inflight_reqs,
                s3_prefer_http2=self.s3_prefer_http2,
                s3_region=self.s3_region,
                s3_enable_s3express=self.s3_enable_s3express,
            )
        else:
            logger.info("Using Python S3 connector implementation")
            # Local
            from .s3_connector import S3Connector

            return S3Connector(
                s3_endpoint=s3_endpoint,
                loop=context.loop,
                local_cpu_backend=context.local_cpu_backend,
                s3_part_size=self.s3_part_size,
                s3_file_prefix=self.s3_file_prefix,
                s3_max_io_concurrency=self.s3_max_io_concurrency,
                s3_max_inflight_reqs=self.s3_max_inflight_reqs,
                s3_prefer_http2=self.s3_prefer_http2,
                s3_region=self.s3_region,
                s3_enable_s3express=self.s3_enable_s3express,
            )
