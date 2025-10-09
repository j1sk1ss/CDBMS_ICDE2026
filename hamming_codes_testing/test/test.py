import os
import sys
import glob
import time
import random
import argparse
import pyfiglet
import textwrap
import subprocess

from tools.injector import (
    random_bitflips, 
    white_noise_bitflips,
    scratch_emulation
)

def _clean_tools(coder: str, decoder: str, generator: str) -> None:
    tools: list[str] = [coder, decoder, generator]
    for tool in tools:
        if os.path.exists(tool):
            print(f"[CLEANUP] remove old {tool}")
            os.remove(tool)

def _bit_differences(ffile: str, sfile: str) -> int:
    chunk_size = 8192
    diff_bits = 0

    with open(ffile, "rb") as f1, open(sfile, "rb") as f2:
        while True:
            b1 = f1.read(chunk_size)
            b2 = f2.read(chunk_size)
            if not b1 and not b2:
                break

            if len(b1) < len(b2):
                b1 += b'\x00' * (len(b2) - len(b1))
            elif len(b2) < len(b1):
                b2 += b'\x00' * (len(b1) - len(b2))

            for byte1, byte2 in zip(b1, b2):
                diff_bits += bin(byte1 ^ byte2).count('1')

    return diff_bits

def _fill_file(path: str, size: int) -> list[int]:
    content: list[int] = []
    with open(path, "wb") as f:
        for _ in range(size):
            byte = random.randint(0, 255)
            f.write(byte.to_bytes(1, "little"))
            content.append(byte)
            
    return content

def _launch_tool(tool: str, args: list[str]) -> float:
    """Launch provided builded tool

    Args:
        tool (str): Tool path
        args (list[str]): Passed arguments
    """
    print(f"[LAUNCH] tool={tool}, args={args}")
    start_time = time.perf_counter()
    subprocess.run([tool, *args], check=True)
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    return elapsed

def _build_tools(gcc: str, basedir: str, coder: str, decoder: str, generator: str) -> None:
    """Build tools with provided GCC

    Args:
        gcc (str): Avaliable GCC version. Example: gcc-14, gcc
        coder (str): Coder .c file without extention. Example: tools/file2hamm
        decoder (str): Decoder .c file without extention. Example: tools/hamm2file
        generator (str): Generator .c file without extention. Example: tools/gen_file
    """
    tools: list[str] = [coder, decoder, generator]
    for tool in tools:
        c_file = f"{tool}.c"
        
        if os.path.exists(tool):
            print(f"[CLEANUP] remove old {tool}")
            os.remove(tool)
        
        print(f"[BUILD] {c_file} -> {tool}")
        c_files = glob.glob(f"{basedir}/src/*.c")
        subprocess.run([gcc, f"-I{basedir}/include", "-O2", "-Wall", "-o", tool, *c_files, c_file], check=True)

def _print_statistics(size: int, diff: int, dectime: float, enctime: float) -> None:
    total_bits = size * 8
    percent_error = (diff / total_bits) * 100 if total_bits > 0 else 0
    print("=" * 50)
    print(f"{'Statistic':<25} | {'Value'}")
    print("-" * 50)
    print(f"{'Original file size (b)':<25} | {size}")
    print(f"{'Total bits':<25} | {total_bits}")
    print(f"{'Bit differences':<25} | {diff}")
    print(f"{'Percentage errors':<25} | {percent_error:.6f}%")
    print(f"{'Encoding time (s)':<25} | {enctime:.6f}")
    print(f"{'Decoding time (s)':<25} | {dectime:.6f}")
    print("=" * 50)

def _print_parity_info() -> None:
    print("parity_bits=2 => Hamming 3,1")
    print("parity_bits=3 => Hamming 7,4")
    print("parity_bits=4 => Hamming 15,11")
    print("parity_bits=5 => Hamming 31,26")
    print("parity_bits=6 => Hamming 63,57")
    print("parity_bits=7 => Hamming 127,120")
    print("parity_bits=8 => Hamming 255,247")
    print("parity_bits=9 => Hamming 511,502")

def _print_welcome(parser: argparse.ArgumentParser) -> None:
    print("Welcome! Avaliable arguments:\n")
    help_text = parser.format_help()
    options_start = help_text.find("optional arguments:")
    if options_start != -1:
        options_text = help_text[options_start:]
        print(textwrap.indent(options_text, "    "))
    else:
        print(textwrap.indent(help_text, "    "))

if __name__ == "__main__":
    ascii_banner = pyfiglet.figlet_format("Hamming codes!")
    print(ascii_banner)
    
    parser = argparse.ArgumentParser(description="Inject bitflips into disk image.")
    # === Basic setup ===
    parser.add_argument("--gcc", type=str, default="gcc-14", help="Avaliable GCC version")
    parser.add_argument("--hamm-api", type=str, default="..", help="Path to hamming API")
    parser.add_argument("--coder", type=str, default="tools/file2hamm", help="Coder .c file")
    parser.add_argument("--decoder", type=str, default="tools/hamm2file", help="Decoder .c file")
    parser.add_argument("--file-gen", type=str, default="tools/gen_file", help="Generator .c file")
    parser.add_argument("--repeat", type=int, default=1, help="Repeat count")
    
    # === Coder and Decoder setup ===
    parser.add_argument("--file-size", type=str, default="512", help="Source file (with data) size")
    parser.add_argument("--parity-bits", type=str, default="2", help="Parity bits count, see --parity-help for info")
    parser.add_argument("--parity-help", action="store_true")
    parser.add_argument("--src-file", type=str, default="image.img", help="Source file (with data) path that will be generated")
    parser.add_argument("--coded-file", type=str, default="image.hamm", help="Encoded source file name")
    parser.add_argument("--decoded-file", type=str, default="decoded.img", help="Decoded encoded source file name")
    
    # === Bitflips emulation ===
    parser.add_argument("--strategy", type=str, default="none", help="random, scratch or wnoise")
    parser.add_argument("--scratch-length", type=int, default=1024)
    parser.add_argument("--flip-prob", type=float, default=.5)
    parser.add_argument("--width", type=int, default=1)
    parser.add_argument("--intensity", type=float, default=.7)
    parser.add_argument("--flips-size", type=int, default=10)
    args = parser.parse_args()

    if len(sys.argv) == 1:
        _print_welcome(parser=parser)

    if args.parity_help:
        _print_parity_info()
        exit(1)

    _build_tools(gcc=args.gcc, basedir=args.hamm_api, coder=args.coder, decoder=args.decoder, generator=args.file_gen)
    
    diff: int = 0
    enctime: float = .0
    dectime: float = .0
    for _ in range(args.repeat):
        _launch_tool(tool=args.file_gen, args=["--fs", args.file_size, "--out", args.src_file])
        
        print(f"[FILE] filling file {args.src_file} with random data...")
        _fill_file(path=args.src_file, size=int(args.file_size))
        
        enctime += _launch_tool(tool=args.coder, args=["--pb", args.parity_bits, "--target", args.src_file, "--out", args.coded_file])

        if args.strategy == "random":
            random_bitflips(file_path=args.coded_file, num_flips=args.flips_size)
        elif args.strategy == "wnoise":
            white_noise_bitflips(file_path=args.coded_file, flip_probability=args.flip_prob)
        elif args.strategy == "scratch":
            scratch_emulation(file_path=args.coded_file, scratch_length=args.scratch_length, width=args.width, intensity=args.intensity)
        
        dectime += _launch_tool(tool=args.decoder, args=["--pb", args.parity_bits, "--target", args.coded_file, "--out", args.decoded_file])
        diff += _bit_differences(ffile=args.src_file, sfile=args.decoded_file)
    
    _print_statistics(size=int(args.file_size), diff=diff / args.repeat, enctime=enctime / args.repeat, dectime=dectime / args.repeat)
    _clean_tools(coder=args.coder, decoder=args.decoder, generator=args.file_gen)
