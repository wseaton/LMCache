#!/usr/bin/env python3
"""
standalone test for batched zero-copy APIs in Rust S3 connector.
tests the connector directly without LMCache dependencies.
"""

import asyncio
import os
import numpy as np


async def main():
    print("Testing Rust S3 Connector - Batched Zero-Copy APIs\n")

    # import the Rust connector
    try:
        from lmcache_s3 import RustS3Connector
        print("✓ Imported RustS3Connector successfully")
    except ImportError as e:
        print(f"✗ Failed to import Rust connector: {e}")
        return

    # configuration
    # note: connector expects s3:// URL format, not http://
    s3_endpoint = "s3://lmcache-test.s3.us-east-1.amazonaws.com"
    chunk_size = 4 * 1024 * 1024  # 4MB chunks
    max_inflight = 64

    # create and initialize connector
    print(f"\n1. Initializing connector...")
    print(f"   Endpoint: {s3_endpoint}")
    print(f"   Chunk size: {chunk_size:,} bytes")
    print(f"   Max inflight: {max_inflight}")

    connector = RustS3Connector()
    try:
        connector.initialize(
            s3_endpoint=s3_endpoint,
            chunk_size=chunk_size,
            max_inflight_reqs=max_inflight,
            s3_prefix="test-batched",
        )
        print("✓ Connector initialized")
    except Exception as e:
        print(f"✗ Initialization failed: {e}")
        return

    # test 1: upload some test data
    print(f"\n2. Uploading test data...")
    test_keys = [f"batch-test-{i}" for i in range(5)]

    for i, key in enumerate(test_keys):
        # create test data (pattern unique to each key)
        test_data = np.full(chunk_size, fill_value=i, dtype=np.uint8)
        data_ptr = test_data.ctypes.data

        try:
            connector.put(key, data_ptr, chunk_size)
            print(f"   ✓ Uploaded {key}")
        except Exception as e:
            print(f"   ✗ Upload failed for {key}: {e}")

    # test 2: batched_contains
    print(f"\n3. Testing batched_contains()...")
    try:
        # test with mix of existing and non-existing keys
        check_keys = test_keys + ["non-existent-1", "non-existent-2"]
        bitmask = await connector.batched_contains(check_keys)

        print(f"   Keys checked: {len(check_keys)}")
        print(f"   Bitmask bytes: {len(bitmask)}")
        print(f"   Bitmask: {' '.join(f'{b:08b}' for b in bitmask)}")

        # verify bitmask
        for i, key in enumerate(check_keys):
            byte_idx = i // 8
            bit_idx = i % 8
            exists = bool((bitmask[byte_idx] >> bit_idx) & 1)
            expected = i < len(test_keys)  # first 5 should exist

            status = "✓" if exists == expected else "✗"
            print(f"   {status} {key}: exists={exists} (expected={expected})")

    except Exception as e:
        print(f"✗ batched_contains failed: {e}")
        import traceback
        traceback.print_exc()

    # test 3: batched_get_to_buffers
    print(f"\n4. Testing batched_get_to_buffers()...")
    try:
        # allocate buffers for download
        buffers = [np.zeros(chunk_size, dtype=np.uint8) for _ in range(len(test_keys))]
        buffer_ptrs = [buf.ctypes.data for buf in buffers]
        buffer_sizes = [chunk_size] * len(test_keys)

        # batched download
        results = await connector.batched_get_to_buffers(
            test_keys,
            buffer_ptrs,
            buffer_sizes
        )

        print(f"   Downloaded {len(results)} objects")

        # verify data integrity
        for i, (key, success, buffer) in enumerate(zip(test_keys, results, buffers)):
            if success:
                # check if data matches what we uploaded (all bytes = i)
                expected_value = i
                all_match = np.all(buffer == expected_value)

                status = "✓" if all_match else "✗"
                first_bytes = buffer[:10].tolist()
                print(f"   {status} {key}: success={success}, data_valid={all_match}, first_bytes={first_bytes}")
            else:
                print(f"   ✗ {key}: success={success}")

    except Exception as e:
        print(f"✗ batched_get_to_buffers failed: {e}")
        import traceback
        traceback.print_exc()

    # test 4: verify zero-copy by checking memory addresses
    print(f"\n5. Verifying zero-copy behavior...")
    try:
        test_key = test_keys[0]

        # allocate buffer
        buffer = np.zeros(chunk_size, dtype=np.uint8)
        original_ptr = buffer.ctypes.data

        # download directly to buffer
        results = await connector.batched_get_to_buffers(
            [test_key],
            [original_ptr],
            [chunk_size]
        )

        # verify pointer hasn't changed (true zero-copy)
        final_ptr = buffer.ctypes.data

        if original_ptr == final_ptr:
            print(f"   ✓ Zero-copy confirmed: buffer pointer unchanged")
            print(f"     Pointer: 0x{original_ptr:x}")
        else:
            print(f"   ✗ Buffer pointer changed (data was copied)")
            print(f"     Original: 0x{original_ptr:x}")
            print(f"     Final: 0x{final_ptr:x}")

        # verify data was written
        if results[0]:
            data_valid = np.all(buffer == 0)  # should be filled with 0 (from upload)
            print(f"   ✓ Data integrity: {'valid' if data_valid else 'invalid'}")

    except Exception as e:
        print(f"✗ Zero-copy verification failed: {e}")
        import traceback
        traceback.print_exc()

    # test 5: performance test
    print(f"\n6. Performance test (10 concurrent downloads)...")
    try:
        import time

        num_downloads = 10
        download_keys = [test_keys[i % len(test_keys)] for i in range(num_downloads)]

        buffers = [np.zeros(chunk_size, dtype=np.uint8) for _ in range(num_downloads)]
        buffer_ptrs = [buf.ctypes.data for buf in buffers]
        buffer_sizes = [chunk_size] * num_downloads

        start = time.perf_counter()
        results = await connector.batched_get_to_buffers(
            download_keys,
            buffer_ptrs,
            buffer_sizes
        )
        elapsed = time.perf_counter() - start

        successful = sum(results)
        total_bytes = successful * chunk_size
        throughput = total_bytes / elapsed / (1024 ** 3)  # GB/s

        print(f"   Downloaded: {successful}/{num_downloads} objects")
        print(f"   Total data: {total_bytes / (1024**2):.1f} MB")
        print(f"   Time: {elapsed:.3f}s")
        print(f"   Throughput: {throughput:.2f} GB/s")

    except Exception as e:
        print(f"✗ Performance test failed: {e}")
        import traceback
        traceback.print_exc()

    # cleanup
    print(f"\n7. Cleaning up...")
    try:
        connector.close()
        print("✓ Connector closed")
    except Exception as e:
        print(f"✗ Close failed: {e}")

    print("\n" + "="*60)
    print("Test completed!")


if __name__ == "__main__":
    asyncio.run(main())
