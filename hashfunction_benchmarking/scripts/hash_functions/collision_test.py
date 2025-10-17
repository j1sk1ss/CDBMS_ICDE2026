import os
import sys
import pyfiglet
import argparse
import textwrap
import pandas as pd

from tqdm import tqdm
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


def test_collisions_in_memory(
    csv_path: str, column: str, save_col: bool, hasher: HashFunction, chunk_size: int = 10_000_000
) -> None:
    seen_hashes: dict[str, str] = {}
    collisions: list[tuple[str, str, str]] = []

    with pd.read_csv(csv_path, usecols=[column], dtype=str, chunksize=chunk_size) as reader:
        for chunk in reader:
            for name in tqdm(chunk[column], desc=f"Hashing ({hasher.__class__.__name__})", leave=False):
                transformed = to_fixed_8_3(str(name))
                hashed: str = str(hasher.hash(transformed))

                if hashed not in seen_hashes:
                    seen_hashes[hashed] = name
                else:
                    original_name = seen_hashes[hashed]
                    if original_name != name:
                        collisions.append((original_name, name, hashed))

    print(f"Found {len(collisions)} collisions!")
    if collisions and save_col:
        df = pd.DataFrame(collisions, columns=["left", "right", "hash"])
        df.to_csv(f"{hasher.__class__.__name__}_collisions.csv", index=False)
        print(f"Collisions saved to {hasher.__class__.__name__}_collisions.csv")


def print_welcome(parser: argparse.ArgumentParser) -> None:
    print("Welcome! Avaliable arguments:\n")
    help_text = parser.format_help()
    options_start = help_text.find("optional arguments:")
    if options_start != -1:
        options_text = help_text[options_start:]
        print(textwrap.indent(options_text, "    "))
    else:
        print(textwrap.indent(help_text, "    "))


if __name__ == "__main__":
    ascii_banner = pyfiglet.figlet_format("Collision tester")
    print(ascii_banner)

    parser = argparse.ArgumentParser(description="Test collisions on dataset")
    parser.add_argument("--dataset", type=str, required=True, help="Path to dataset CSV")
    parser.add_argument("--column", type=str, required=True, default="name", help="Dataset column name")
    parser.add_argument("--hashes", nargs="+", required=True, help="Hash functions to use (e.g. crc32 murmur3)")
    parser.add_argument("--save-col", action="store_true", help="Save csv file with collisions")
    args = parser.parse_args()

    if len(sys.argv) == 1:
        print_welcome(parser=parser)
        exit(1)

    unknown = [h for h in args.hashes if h not in HASH_FUNCTIONS]
    if unknown:
        print(f"Unknown hash functions: {unknown}")
        print(f"Available: {list(HASH_FUNCTIONS.keys())}")
        exit(1)

    for name in args.hashes:
        func = HASH_FUNCTIONS[name]
        print(f"--- Checking with {name} ---")
        test_collisions_in_memory(args.dataset, args.column, args.save_col, func)
