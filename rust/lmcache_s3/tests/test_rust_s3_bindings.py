#!/usr/bin/env python3
"""
simple test to verify Rust S3 connector bindings work
can be run on Mac without CUDA/GPUs
"""

import sys


def test_import():
    """test that we can import the Rust module"""
    print("Testing import of lmcache_s3 module...")
    try:
        import lmcache_s3
        print("✓ Successfully imported lmcache_s3")
        return True
    except ImportError as e:
        print(f"✗ Failed to import lmcache_s3: {e}")
        return False


def test_instantiation():
    """test that we can create a RustS3Connector instance"""
    print("\nTesting RustS3Connector instantiation...")
    try:
        from lmcache_s3 import RustS3Connector

        connector = RustS3Connector()
        print("✓ Successfully created RustS3Connector instance")
        return True
    except Exception as e:
        print(f"✗ Failed to create RustS3Connector: {e}")
        return False


def test_python_wrapper():
    """test the Python wrapper class"""
    print("\nTesting Python wrapper import...")
    try:
        from lmcache.v1.storage_backend.connector.rust_s3_connector import (
            RustS3Connector,
        )

        print("✓ Successfully imported RustS3Connector wrapper")

        # note: we can't fully initialize without asyncio loop and LocalCPUBackend
        # but we can verify the class exists and is importable
        print(f"  Class: {RustS3Connector.__name__}")
        print(f"  Module: {RustS3Connector.__module__}")
        return True
    except Exception as e:
        print(f"✗ Failed to import Python wrapper: {e}")
        return False


def test_adapter_selection():
    """test that the adapter can select between implementations"""
    print("\nTesting S3ConnectorAdapter implementation selection...")
    try:
        import os

        from lmcache.v1.storage_backend.connector.s3_adapter import (
            S3ConnectorAdapter,
            USE_RUST_S3,
        )

        print(f"  USE_RUST_S3 = {USE_RUST_S3}")
        print(f"  LMCACHE_USE_RUST_S3 = {os.getenv('LMCACHE_USE_RUST_S3', '1')}")

        adapter = S3ConnectorAdapter()
        print(f"✓ Successfully created S3ConnectorAdapter")
        print(f"  Schema: {adapter.schema}")
        return True
    except Exception as e:
        print(f"✗ Failed to test adapter: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """run all tests"""
    print("=" * 60)
    print("Rust S3 Connector Bindings Test Suite")
    print("=" * 60)

    tests = [
        test_import,
        test_instantiation,
        test_python_wrapper,
        test_adapter_selection,
    ]

    results = []
    for test in tests:
        results.append(test())

    print("\n" + "=" * 60)
    print("Test Results")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")

    if passed == total:
        print("\n✓ All tests passed!")
        return 0
    else:
        print(f"\n✗ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
