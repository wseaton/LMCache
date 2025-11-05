#!/usr/bin/env -S uv run --no-project --with boto3
# /// script
# dependencies = ["boto3"]
# ///
"""
integration test for Rust S3 connector with MinIO
"""

import os
import asyncio
import boto3
from botocore.client import Config

# configure for MinIO
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "test-lmcache"


def setup_minio():
    """create bucket in MinIO if it doesn't exist"""
    print("Setting up MinIO bucket...")

    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"✓ Created bucket: {BUCKET_NAME}")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"✓ Bucket already exists: {BUCKET_NAME}")
    except Exception as e:
        print(f"✗ Failed to create bucket: {e}")
        return False

    return True


async def test_rust_connector():
    """test Rust S3Connector with MinIO"""
    print("\n" + "="*60)
    print("Testing Rust S3Connector")
    print("="*60)

    # set AWS env vars for MinIO
    os.environ['AWS_ACCESS_KEY_ID'] = MINIO_ACCESS_KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = MINIO_SECRET_KEY
    os.environ['AWS_ENDPOINT_URL'] = MINIO_ENDPOINT
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['LMCACHE_USE_RUST_S3'] = '1'

    try:
        # import LMCache components
        from lmcache.v1.storage_backend.connector.rust_s3_connector import RustS3Connector
        from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend

        print("\n1. Initializing components...")
        backend = LocalCPUBackend()
        connector = RustS3Connector(backend)

        # initialize with MinIO endpoint
        s3_url = f"s3://{BUCKET_NAME}"
        await connector.initialize(s3_url)
        print(f"✓ Initialized connector with URL: {s3_url}")

        # test data
        test_key = "test-rust-connector/data.bin"
        test_data = b"Hello from Rust S3 Connector! " * 100  # ~3KB

        print(f"\n2. Testing PUT operation...")
        print(f"   Key: {test_key}")
        print(f"   Size: {len(test_data)} bytes")

        # create memory object and write test data
        memory_obj = backend.allocate(len(test_data))
        memory_obj.write_from_bytes(test_data)

        # upload to MinIO
        await connector.put(test_key, memory_obj)
        print(f"✓ PUT successful")

        print(f"\n3. Testing EXISTS operation...")
        exists = await connector.exists(test_key)
        print(f"✓ EXISTS returned: {exists}")

        if not exists:
            print("✗ Object should exist but doesn't!")
            return False

        print(f"\n4. Testing GET operation...")
        retrieved_memory_obj = await connector.get(test_key)

        if retrieved_memory_obj is None:
            print("✗ GET returned None")
            return False

        retrieved_data = retrieved_memory_obj.to_bytes()
        print(f"✓ Retrieved {len(retrieved_data)} bytes")

        print(f"\n5. Verifying data integrity...")
        if retrieved_data == test_data:
            print(f"✓ Data matches! ({len(retrieved_data)} bytes)")
        else:
            print(f"✗ Data mismatch!")
            print(f"   Expected: {len(test_data)} bytes")
            print(f"   Got: {len(retrieved_data)} bytes")
            return False

        print(f"\n6. Testing non-existent key...")
        missing_exists = await connector.exists("nonexistent-key")
        print(f"✓ EXISTS for missing key returned: {missing_exists}")

        if missing_exists:
            print("✗ Non-existent key should return False!")
            return False

        print(f"\n7. Cleanup...")
        await connector.close()
        print(f"✓ Connector closed")

        print("\n" + "="*60)
        print("✓ All tests passed!")
        print("="*60)
        return True

    except ImportError as e:
        print(f"✗ Failed to import: {e}")
        print("\nMake sure to run: just build")
        return False
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_python_connector_comparison():
    """test Python S3Connector for comparison"""
    print("\n" + "="*60)
    print("Testing Python S3Connector (for comparison)")
    print("="*60)

    os.environ['LMCACHE_USE_RUST_S3'] = '0'

    try:
        from lmcache.v1.storage_backend.connector.s3_connector import S3Connector
        from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend

        backend = LocalCPUBackend()
        connector = S3Connector(backend)

        s3_url = f"s3://{BUCKET_NAME}"
        await connector.initialize(s3_url)
        print(f"✓ Initialized Python connector")

        test_key = "test-python-connector/data.bin"
        test_data = b"Hello from Python S3 Connector! " * 100

        memory_obj = backend.allocate(len(test_data))
        memory_obj.write_from_bytes(test_data)

        await connector.put(test_key, memory_obj)
        print(f"✓ Python PUT successful")

        retrieved = await connector.get(test_key)
        if retrieved and retrieved.to_bytes() == test_data:
            print(f"✓ Python GET successful and data matches")
        else:
            print(f"✗ Python GET failed")
            return False

        await connector.close()
        print("✓ Python connector test passed")
        return True

    except Exception as e:
        print(f"⚠ Python connector test failed (this is OK if only testing Rust): {e}")
        return False


async def main():
    """run all tests"""
    print("="*60)
    print("LMCache Rust S3 Connector Integration Tests")
    print("="*60)

    if not setup_minio():
        print("\n✗ MinIO setup failed. Make sure MinIO is running:")
        print("  just minio-start")
        return 1

    rust_passed = await test_rust_connector()

    if not rust_passed:
        print("\n✗ Rust connector tests failed!")
        return 1

    # optionally test Python connector too
    # python_passed = await test_python_connector_comparison()

    print("\n" + "="*60)
    print("✓ Integration tests completed successfully!")
    print("="*60)
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
