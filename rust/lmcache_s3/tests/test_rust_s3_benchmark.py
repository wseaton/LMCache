#!/usr/bin/env python3
"""
benchmark comparing Rust S3 connector vs Python S3 connector
tests GET/PUT performance with MinIO under realistic concurrent workloads
"""

import os
import asyncio
import time
import ctypes
import statistics
from typing import List, Tuple, Dict
import boto3
from botocore.client import Config

# configure for MinIO
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "test-lmcache-bench"

# benchmark parameters
CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
NUM_WARMUP_RUNS = 3
NUM_SEQUENTIAL_RUNS = 10
CONCURRENCY_LEVELS = [2, 4, 8, 16]  # test different concurrency levels
NUM_CONCURRENT_OPS = 20  # total operations per concurrency test
MAX_INFLIGHT = 16  # increase to support higher concurrency


def setup_minio():
    """create bucket and test objects in MinIO"""
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

    # upload test objects for GET benchmarks
    # need enough for sequential + concurrent tests
    total_objects = NUM_SEQUENTIAL_RUNS + NUM_WARMUP_RUNS + NUM_CONCURRENT_OPS * 2
    test_data = b"Benchmark data! " * (CHUNK_SIZE // 16)

    print(f"Uploading {total_objects} test objects...")
    for i in range(total_objects):
        key = f"bench-get_{i}.bin"
        try:
            s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=test_data)
        except Exception as e:
            print(f"✗ Failed to upload {key}: {e}")
            return False

    print(f"✓ Uploaded {total_objects} test objects")
    return True


class BenchmarkResults:
    """store benchmark results"""
    def __init__(self):
        self.sequential_get: List[float] = []
        self.sequential_put: List[float] = []
        self.concurrent_get: Dict[int, Tuple[float, float]] = {}  # level -> (total_time, avg_latency)
        self.concurrent_put: Dict[int, Tuple[float, float]] = {}
        self.mixed_workload: Dict[int, float] = {}  # level -> total_time


async def benchmark_rust_connector() -> BenchmarkResults:
    """benchmark Rust S3 connector with various workloads"""
    print("\n" + "="*60)
    print("Benchmarking Rust S3 Connector")
    print("="*60)

    # set AWS env vars for MinIO
    os.environ['AWS_ACCESS_KEY_ID'] = MINIO_ACCESS_KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = MINIO_SECRET_KEY
    os.environ['AWS_ENDPOINT_URL'] = MINIO_ENDPOINT
    os.environ['AWS_REGION'] = 'us-east-1'

    results = BenchmarkResults()

    try:
        from lmcache_s3 import RustS3Connector

        connector = RustS3Connector()
        s3_url = f"s3://{BUCKET_NAME}"
        connector.initialize(s3_url, CHUNK_SIZE, MAX_INFLIGHT)

        # prepare buffers for concurrent operations
        buffers = []
        for _ in range(max(CONCURRENCY_LEVELS)):
            buf = (ctypes.c_ubyte * CHUNK_SIZE)()
            buffers.append((buf, ctypes.addressof(buf)))

        # warmup
        print("\n[Warmup]")
        for i in range(NUM_WARMUP_RUNS):
            key = f"bench-get_{i}.bin"
            await asyncio.to_thread(connector.get, key, buffers[0][1], CHUNK_SIZE)

        # === SEQUENTIAL TESTS ===
        print(f"\n[Sequential GET - {NUM_SEQUENTIAL_RUNS} runs]")
        for i in range(NUM_SEQUENTIAL_RUNS):
            key = f"bench-get_{NUM_WARMUP_RUNS + i}.bin"
            start = time.perf_counter()
            await asyncio.to_thread(connector.get, key, buffers[0][1], CHUNK_SIZE)
            elapsed = time.perf_counter() - start
            results.sequential_get.append(elapsed)
            print(f"  Run {i+1}: {elapsed*1000:.2f}ms")

        print(f"\n[Sequential PUT - {NUM_SEQUENTIAL_RUNS} runs]")
        put_data = b"PUT benchmark! " * (CHUNK_SIZE // 15)
        ctypes.memmove(buffers[0][0], put_data, len(put_data))
        for i in range(NUM_SEQUENTIAL_RUNS):
            key = f"bench-rust-put-seq_{i}.bin"
            start = time.perf_counter()
            await asyncio.to_thread(connector.put, key, buffers[0][1], CHUNK_SIZE)
            elapsed = time.perf_counter() - start
            results.sequential_put.append(elapsed)
            print(f"  Run {i+1}: {elapsed*1000:.2f}ms")

        # === CONCURRENT GET TESTS ===
        key_offset = NUM_WARMUP_RUNS + NUM_SEQUENTIAL_RUNS
        for level in CONCURRENCY_LEVELS:
            print(f"\n[Concurrent GET - {level} parallel operations]")

            async def concurrent_get_task(task_id: int) -> float:
                """single GET task"""
                key = f"bench-get_{key_offset + task_id}.bin"
                start = time.perf_counter()
                await asyncio.to_thread(connector.get, key, buffers[task_id % len(buffers)][1], CHUNK_SIZE)
                return time.perf_counter() - start

            # run operations with specified concurrency
            latencies = []
            start_time = time.perf_counter()
            for batch_start in range(0, NUM_CONCURRENT_OPS, level):
                batch_size = min(level, NUM_CONCURRENT_OPS - batch_start)
                tasks = [concurrent_get_task(batch_start + i) for i in range(batch_size)]
                batch_latencies = await asyncio.gather(*tasks)
                latencies.extend(batch_latencies)
            total_time = time.perf_counter() - start_time

            avg_latency = statistics.mean(latencies)
            throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / total_time
            results.concurrent_get[level] = (total_time, avg_latency)
            print(f"  Total time: {total_time:.2f}s")
            print(f"  Avg latency: {avg_latency*1000:.2f}ms")
            print(f"  Throughput: {throughput:.2f} MB/s")

        # === CONCURRENT PUT TESTS ===
        for level in CONCURRENCY_LEVELS:
            print(f"\n[Concurrent PUT - {level} parallel operations]")

            async def concurrent_put_task(task_id: int) -> float:
                """single PUT task"""
                key = f"bench-rust-put-concurrent_{level}_{task_id}.bin"
                buf_idx = task_id % len(buffers)
                start = time.perf_counter()
                await asyncio.to_thread(connector.put, key, buffers[buf_idx][1], CHUNK_SIZE)
                return time.perf_counter() - start

            latencies = []
            start_time = time.perf_counter()
            for batch_start in range(0, NUM_CONCURRENT_OPS, level):
                batch_size = min(level, NUM_CONCURRENT_OPS - batch_start)
                tasks = [concurrent_put_task(batch_start + i) for i in range(batch_size)]
                batch_latencies = await asyncio.gather(*tasks)
                latencies.extend(batch_latencies)
            total_time = time.perf_counter() - start_time

            avg_latency = statistics.mean(latencies)
            throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / total_time
            results.concurrent_put[level] = (total_time, avg_latency)
            print(f"  Total time: {total_time:.2f}s")
            print(f"  Avg latency: {avg_latency*1000:.2f}ms")
            print(f"  Throughput: {throughput:.2f} MB/s")

        # === MIXED WORKLOAD TESTS ===
        for level in CONCURRENCY_LEVELS:
            print(f"\n[Mixed Workload - {level} parallel GET+PUT]")

            async def mixed_task(task_id: int) -> float:
                """alternating GET and PUT tasks"""
                buf_idx = task_id % len(buffers)
                if task_id % 2 == 0:
                    # GET
                    key = f"bench-get_{key_offset + task_id}.bin"
                    start = time.perf_counter()
                    await asyncio.to_thread(connector.get, key, buffers[buf_idx][1], CHUNK_SIZE)
                else:
                    # PUT
                    key = f"bench-rust-put-mixed_{level}_{task_id}.bin"
                    start = time.perf_counter()
                    await asyncio.to_thread(connector.put, key, buffers[buf_idx][1], CHUNK_SIZE)
                return time.perf_counter() - start

            start_time = time.perf_counter()
            for batch_start in range(0, NUM_CONCURRENT_OPS, level):
                batch_size = min(level, NUM_CONCURRENT_OPS - batch_start)
                tasks = [mixed_task(batch_start + i) for i in range(batch_size)]
                await asyncio.gather(*tasks)
            total_time = time.perf_counter() - start_time

            throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / total_time
            results.mixed_workload[level] = total_time
            print(f"  Total time: {total_time:.2f}s")
            print(f"  Throughput: {throughput:.2f} MB/s")

        await asyncio.to_thread(connector.close)
        return results

    except ImportError as e:
        print(f"✗ Failed to import Rust connector: {e}")
        print("\nRun: just build")
        return results
    except Exception as e:
        print(f"✗ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        return results


async def benchmark_python_connector() -> BenchmarkResults:
    """benchmark Python S3 connector with various workloads"""
    print("\n" + "="*60)
    print("Benchmarking Python S3 Connector (using boto3)")
    print("="*60)

    results = BenchmarkResults()

    try:
        import aioboto3
        import tempfile
        import mmap

        # create boto3 session for async operations
        session = aioboto3.Session()

        # create shared memory buffers for concurrent operations
        shm_buffers = []
        for _ in range(max(CONCURRENCY_LEVELS)):
            shm = tempfile.NamedTemporaryFile(
                prefix="bench_shm", suffix=".part", dir="/tmp", delete=False
            )
            os.ftruncate(shm.fileno(), CHUNK_SIZE)
            with open(shm.name, "r+b") as f:
                mm = mmap.mmap(f.fileno(), CHUNK_SIZE)
            shm_buffers.append((shm.name, mm))

        # create s3 client once
        async with session.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1'
        ) as s3_client:

            # helper for GET operation
            async def get_object(key: str, shm_path: str, mm: mmap.mmap) -> float:
                """download object and measure time"""
                start = time.perf_counter()
                response = await s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
                data = await response['Body'].read()
                elapsed = time.perf_counter() - start
                # write to mmap to simulate same I/O pattern as Rust
                mm.seek(0)
                mm.write(data)
                return elapsed

            # helper for PUT operation
            async def put_object(key: str, shm_path: str, mm: mmap.mmap) -> float:
                """upload object and measure time"""
                mm.seek(0)
                data = mm.read(CHUNK_SIZE)
                start = time.perf_counter()
                await s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=data)
                elapsed = time.perf_counter() - start
                return elapsed

            # warmup
            print("\n[Warmup]")
            for i in range(NUM_WARMUP_RUNS):
                key = f"bench-get_{i}.bin"
                await get_object(key, shm_buffers[0][0], shm_buffers[0][1])

            # === SEQUENTIAL TESTS ===
            print(f"\n[Sequential GET - {NUM_SEQUENTIAL_RUNS} runs]")
            for i in range(NUM_SEQUENTIAL_RUNS):
                key = f"bench-get_{NUM_WARMUP_RUNS + i}.bin"
                elapsed = await get_object(key, shm_buffers[0][0], shm_buffers[0][1])
                results.sequential_get.append(elapsed)
                print(f"  Run {i+1}: {elapsed*1000:.2f}ms")

            print(f"\n[Sequential PUT - {NUM_SEQUENTIAL_RUNS} runs]")
            put_data = b"PUT benchmark! " * (CHUNK_SIZE // 15)
            shm_buffers[0][1].seek(0)
            shm_buffers[0][1].write(put_data)
            for i in range(NUM_SEQUENTIAL_RUNS):
                key = f"bench-python-put-seq_{i}.bin"
                elapsed = await put_object(key, shm_buffers[0][0], shm_buffers[0][1])
                results.sequential_put.append(elapsed)
                print(f"  Run {i+1}: {elapsed*1000:.2f}ms")

            # === CONCURRENT GET TESTS ===
            key_offset = NUM_WARMUP_RUNS + NUM_SEQUENTIAL_RUNS
            for level in CONCURRENCY_LEVELS:
                print(f"\n[Concurrent GET - {level} parallel operations]")

                async def concurrent_get_task(task_id: int) -> float:
                    key = f"bench-get_{key_offset + task_id}.bin"
                    shm_idx = task_id % len(shm_buffers)
                    return await get_object(key, shm_buffers[shm_idx][0], shm_buffers[shm_idx][1])

                latencies = []
                start_time = time.perf_counter()
                for batch_start in range(0, NUM_CONCURRENT_OPS, level):
                    batch_size = min(level, NUM_CONCURRENT_OPS - batch_start)
                    tasks = [concurrent_get_task(batch_start + i) for i in range(batch_size)]
                    batch_latencies = await asyncio.gather(*tasks)
                    latencies.extend(batch_latencies)
                total_time = time.perf_counter() - start_time

                avg_latency = statistics.mean(latencies)
                throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / total_time
                results.concurrent_get[level] = (total_time, avg_latency)
                print(f"  Total time: {total_time:.2f}s")
                print(f"  Avg latency: {avg_latency*1000:.2f}ms")
                print(f"  Throughput: {throughput:.2f} MB/s")

            # === CONCURRENT PUT TESTS ===
            for level in CONCURRENCY_LEVELS:
                print(f"\n[Concurrent PUT - {level} parallel operations]")

                async def concurrent_put_task(task_id: int) -> float:
                    key = f"bench-python-put-concurrent_{level}_{task_id}.bin"
                    shm_idx = task_id % len(shm_buffers)
                    return await put_object(key, shm_buffers[shm_idx][0], shm_buffers[shm_idx][1])

                latencies = []
                start_time = time.perf_counter()
                for batch_start in range(0, NUM_CONCURRENT_OPS, level):
                    batch_size = min(level, NUM_CONCURRENT_OPS - batch_start)
                    tasks = [concurrent_put_task(batch_start + i) for i in range(batch_size)]
                    batch_latencies = await asyncio.gather(*tasks)
                    latencies.extend(batch_latencies)
                total_time = time.perf_counter() - start_time

                avg_latency = statistics.mean(latencies)
                throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / total_time
                results.concurrent_put[level] = (total_time, avg_latency)
                print(f"  Total time: {total_time:.2f}s")
                print(f"  Avg latency: {avg_latency*1000:.2f}ms")
                print(f"  Throughput: {throughput:.2f} MB/s")

            # === MIXED WORKLOAD TESTS ===
            for level in CONCURRENCY_LEVELS:
                print(f"\n[Mixed Workload - {level} parallel GET+PUT]")

                async def mixed_task(task_id: int) -> float:
                    shm_idx = task_id % len(shm_buffers)
                    if task_id % 2 == 0:
                        # GET
                        key = f"bench-get_{key_offset + task_id}.bin"
                        return await get_object(key, shm_buffers[shm_idx][0], shm_buffers[shm_idx][1])
                    else:
                        # PUT
                        key = f"bench-python-put-mixed_{level}_{task_id}.bin"
                        return await put_object(key, shm_buffers[shm_idx][0], shm_buffers[shm_idx][1])

                start_time = time.perf_counter()
                for batch_start in range(0, NUM_CONCURRENT_OPS, level):
                    batch_size = min(level, NUM_CONCURRENT_OPS - batch_start)
                    tasks = [mixed_task(batch_start + i) for i in range(batch_size)]
                    await asyncio.gather(*tasks)
                total_time = time.perf_counter() - start_time

                throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / total_time
                results.mixed_workload[level] = total_time
                print(f"  Total time: {total_time:.2f}s")
                print(f"  Throughput: {throughput:.2f} MB/s")

        # cleanup
        for shm_path, mm in shm_buffers:
            mm.close()
            os.unlink(shm_path)

        return results

    except ImportError as e:
        print(f"✗ Failed to import Python dependencies: {e}")
        print("\nRun: pip install awscrt")
        return results
    except Exception as e:
        print(f"✗ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        return results


def print_sequential_comparison(rust_results: BenchmarkResults, python_results: BenchmarkResults):
    """compare sequential performance"""
    print("\n" + "="*60)
    print("SEQUENTIAL PERFORMANCE")
    print("="*60)

    if rust_results.sequential_get and python_results.sequential_get:
        rust_mean = statistics.mean(rust_results.sequential_get)
        python_mean = statistics.mean(python_results.sequential_get)
        speedup = python_mean / rust_mean

        print(f"\nGET Operations:")
        print(f"  Rust:    {rust_mean*1000:7.2f} ms  ({(CHUNK_SIZE/(1024*1024))/rust_mean:6.2f} MB/s)")
        print(f"  Python:  {python_mean*1000:7.2f} ms  ({(CHUNK_SIZE/(1024*1024))/python_mean:6.2f} MB/s)")
        print(f"  Speedup: {speedup:.2f}x {'(Rust faster)' if speedup > 1 else '(Python faster)'}")

    if rust_results.sequential_put and python_results.sequential_put:
        rust_mean = statistics.mean(rust_results.sequential_put)
        python_mean = statistics.mean(python_results.sequential_put)
        speedup = python_mean / rust_mean

        print(f"\nPUT Operations:")
        print(f"  Rust:    {rust_mean*1000:7.2f} ms  ({(CHUNK_SIZE/(1024*1024))/rust_mean:6.2f} MB/s)")
        print(f"  Python:  {python_mean*1000:7.2f} ms  ({(CHUNK_SIZE/(1024*1024))/python_mean:6.2f} MB/s)")
        print(f"  Speedup: {speedup:.2f}x {'(Rust faster)' if speedup > 1 else '(Python faster)'}")


def print_concurrent_comparison(rust_results: BenchmarkResults, python_results: BenchmarkResults):
    """compare concurrent performance"""
    print("\n" + "="*60)
    print("CONCURRENT GET PERFORMANCE")
    print("="*60)
    print(f"\n{'Level':<8} {'Rust (s)':<12} {'Python (s)':<12} {'Rust MB/s':<12} {'Python MB/s':<14} {'Speedup':<10}")
    print("-" * 78)

    for level in CONCURRENCY_LEVELS:
        if level in rust_results.concurrent_get and level in python_results.concurrent_get:
            rust_time, _ = rust_results.concurrent_get[level]
            python_time, _ = python_results.concurrent_get[level]
            rust_throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / rust_time
            python_throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / python_time
            speedup = python_time / rust_time

            print(f"{level:<8} {rust_time:<12.2f} {python_time:<12.2f} {rust_throughput:<12.1f} {python_throughput:<14.1f} {speedup:<10.2f}x")

    print("\n" + "="*60)
    print("CONCURRENT PUT PERFORMANCE")
    print("="*60)
    print(f"\n{'Level':<8} {'Rust (s)':<12} {'Python (s)':<12} {'Rust MB/s':<12} {'Python MB/s':<14} {'Speedup':<10}")
    print("-" * 78)

    for level in CONCURRENCY_LEVELS:
        if level in rust_results.concurrent_put and level in python_results.concurrent_put:
            rust_time, _ = rust_results.concurrent_put[level]
            python_time, _ = python_results.concurrent_put[level]
            rust_throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / rust_time
            python_throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / python_time
            speedup = python_time / rust_time

            print(f"{level:<8} {rust_time:<12.2f} {python_time:<12.2f} {rust_throughput:<12.1f} {python_throughput:<14.1f} {speedup:<10.2f}x")


def print_mixed_comparison(rust_results: BenchmarkResults, python_results: BenchmarkResults):
    """compare mixed workload performance"""
    print("\n" + "="*60)
    print("MIXED WORKLOAD PERFORMANCE (50% GET / 50% PUT)")
    print("="*60)
    print(f"\n{'Level':<8} {'Rust (s)':<12} {'Python (s)':<12} {'Rust MB/s':<12} {'Python MB/s':<14} {'Speedup':<10}")
    print("-" * 78)

    for level in CONCURRENCY_LEVELS:
        if level in rust_results.mixed_workload and level in python_results.mixed_workload:
            rust_time = rust_results.mixed_workload[level]
            python_time = python_results.mixed_workload[level]
            rust_throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / rust_time
            python_throughput = (NUM_CONCURRENT_OPS * CHUNK_SIZE / (1024*1024)) / python_time
            speedup = python_time / rust_time

            print(f"{level:<8} {rust_time:<12.2f} {python_time:<12.2f} {rust_throughput:<12.1f} {python_throughput:<14.1f} {speedup:<10.2f}x")


async def main():
    """run all benchmarks"""
    print("="*60)
    print("LMCache S3 Connector Benchmark")
    print("Rust vs Python Performance Comparison")
    print("="*60)
    print(f"\nParameters:")
    print(f"  Chunk size:           {CHUNK_SIZE / (1024*1024):.1f} MB")
    print(f"  Warmup runs:          {NUM_WARMUP_RUNS}")
    print(f"  Sequential runs:      {NUM_SEQUENTIAL_RUNS}")
    print(f"  Concurrent ops:       {NUM_CONCURRENT_OPS}")
    print(f"  Concurrency levels:   {CONCURRENCY_LEVELS}")
    print(f"  Max inflight:         {MAX_INFLIGHT}")

    if not setup_minio():
        print("\n✗ MinIO setup failed. Make sure MinIO is running:")
        print("  just minio-start")
        return 1

    # benchmark Rust connector
    rust_results = await benchmark_rust_connector()

    # benchmark Python connector
    python_results = await benchmark_python_connector()

    # print comprehensive comparison
    print("\n" + "="*60)
    print("BENCHMARK RESULTS")
    print("="*60)

    print_sequential_comparison(rust_results, python_results)
    print_concurrent_comparison(rust_results, python_results)
    print_mixed_comparison(rust_results, python_results)

    print("\n" + "="*60)
    print("Benchmark completed!")
    print("="*60)
    print("\nKey Insights:")
    print("- Sequential: baseline single-operation performance")
    print("- Concurrent: tests parallel operation handling (2, 4, 8, 16 concurrent)")
    print("- Mixed: realistic workload with 50% GET + 50% PUT operations")
    print("- Higher throughput at higher concurrency shows better async handling")

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
