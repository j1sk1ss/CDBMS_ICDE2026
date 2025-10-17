import os
import time
import argparse
import pandas as pd

from tqdm import tqdm
from typing import Callable

from functions.hash_function import HashFunction

# ==== Checksum generators ====
from functions.crc16 import CRC16
from functions.crc32 import CRC32
from functions.crc64 import CRC64
from functions.murmur_hash3 import murmurHash3
from functions.xxhash64 import xxHash64

# ==== Cryptographic functions ====
from functions.md5 import MD5
from functions.blake2b import Blake2B
from functions.ripemd160 import Ripemd160
from functions.sha256 import SHA256
from functions.sha512 import SHA512
from functions.sha3 import SHA3


HASH_FUNCTIONS: dict[str, HashFunction] = {
    "crc16": CRC16(),
    "crc32": CRC32(),
    "crc64": CRC64(),
    "murmur3": murmurHash3(),
    "xxhash64": xxHash64(),
    "md5": MD5(),
    "blake": Blake2B(),
    "ripemd": Ripemd160(),
    "sha256": SHA256(),
    "sha512": SHA512(),
    "sha3": SHA3()
}


def to_fixed_8_3(filename: str) -> str:
    name, ext = os.path.splitext(os.path.basename(filename))
    name = name.upper()[:8].ljust(8)
    ext = ext.upper().lstrip('.')[:3].ljust(3)
    return name + ext


def benchmark_hash_function_repeated(
    csv_path: str,
    hash_name: str,
    hash_func: Callable[[str], str],
    repeat: int = 3,
    chunk_size: int = 10_000_000
) -> dict:
    times = []
    total_records = 0

    for attempt in range(repeat):
        current_records = 0
        current_time = 0.0

        with pd.read_csv(csv_path, usecols=['name'], dtype=str, chunksize=chunk_size) as reader:
            for chunk in reader:
                names = chunk['name'].astype(str)

                start = time.perf_counter()
                for name in tqdm(names, desc=f"{hash_name} [run {attempt + 1}/{repeat}]", leave=False):
                    transformed = to_fixed_8_3(name)
                    _ = hash_func(transformed)
                end = time.perf_counter()

                current_time += end - start
                current_records += len(names)

        times.append(current_time)
        total_records = current_records

    avg_time_total = sum(times) / repeat
    avg_time_per_record_us = (avg_time_total / total_records) * 1e6 if total_records else 0

    print(f"--- {hash_name} ---")
    print(f"Avg total time over {repeat} runs: {avg_time_total:.4f} seconds")
    print(f"Total records: {total_records}")
    print(f"Avg per hash: {avg_time_per_record_us:.2f} µs")

    return {
        "hash": hash_name,
        "total_records": total_records,
        "avg_total_time_sec": avg_time_total,
        "avg_time_us": avg_time_per_record_us
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hash function benchmark")
    parser.add_argument("--dataset", type=str, required=True, help="Path to dataset CSV with 'name' column")
    parser.add_argument("--hashes", nargs="+", required=True, help="Hash functions to use (e.g. crc32 sha256)")
    parser.add_argument("--repeat", type=int, default=3, help="How many times to repeat each benchmark")
    parser.add_argument("--out", type=str, default="benchmark_results.csv", help="Output CSV for results")
    args = parser.parse_args()

    unknown = [h for h in args.hashes if h not in HASH_FUNCTIONS]
    if unknown:
        print(f"Unknown hash functions: {unknown}")
        print(f"Available: {list(HASH_FUNCTIONS.keys())}")
        exit(1)

    results = []

    for name in args.hashes:
        func = HASH_FUNCTIONS[name]
        print(f"\n--- Testing: {name} ---")
        result = benchmark_hash_function_repeated(
            csv_path=args.dataset,
            hash_name=name,
            hash_func=func.hash,
            repeat=args.repeat
        )
        results.append(result)

    df = pd.DataFrame(results)
    df = df.sort_values(by="avg_time_us")
    df.to_csv(args.out, index=False)

    print("\n=== Benchmark Summary ===")
    print(df.to_string(index=False))
    print(f"\nSaved to: {args.out}")
