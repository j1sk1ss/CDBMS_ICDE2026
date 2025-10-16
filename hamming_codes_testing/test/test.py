import os
import sys
import time
import random
import argparse
import pyfiglet
import textwrap
import subprocess

from pathlib import Path

from unix import (
    get_cpu_freq,
    estimate_energy,
    get_cpu_ticks,
    get_cpu_temp
)

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

def _launch_tool(tool: str, args: list[str]) -> tuple[float, float, int, float, float]:
    """Launch provided builded tool

    Args:
        tool (str): Tool path
        args (list[str]): Passed arguments
    """
    print(f"[LAUNCH] tool={tool}, args={args}")
    
    start_time = time.perf_counter()
    start_proc_time = time.process_time()
    start_ticks = get_cpu_ticks()
    start_temp = get_cpu_temp()
    
    subprocess.run([tool, *args], check=True)
    
    end_time = time.perf_counter()
    end_proc_time = time.process_time()
    end_ticks = get_cpu_ticks()
    end_temp = get_cpu_temp()
    freq_ghz = get_cpu_freq()
    
    elapsed = end_time - start_time
    cpu_time = end_proc_time - start_proc_time
    tick_diff = end_ticks - start_ticks
    temp_diff = end_temp - start_temp
    energy = estimate_energy(cpu_time, freq_ghz)
    
    return elapsed, cpu_time, tick_diff, temp_diff, energy

def _build_tools(gcc: str, gener: str, makefile_dir: str) -> None:
    """
    Build tools using Makefile in the specified directory.

    Args:
        makefile_dir (str): Path to the directory containing Makefile
    """
    makefile_path = Path(makefile_dir) / "Makefile"
    if not makefile_path.exists():
        raise FileNotFoundError(f"Makefile not found in {makefile_dir}")

    print(f"[BUILD] Running 'make' in {makefile_dir}")
    subprocess.run(["make"], cwd=makefile_dir, check=True)
    subprocess.run([gcc, f"{gener}.c", "-o", gener], check=True)
    print(f"[BUILD] Completed")

def _print_statistics(
    size: int, diff: int, enctime: float, dectime: float, 
    cpu_enc: float, cpu_dec: float, ticks_enc: int, ticks_dec: int, 
    temp_enc: float, temp_dec: float, energy_enc: float, energy_dec: float
) -> None:
    total_bits = size * 8
    percent_error = (diff / total_bits) * 100 if total_bits > 0 else 0
    print("=" * 80)
    print(f"{'Statistic':<40} | {'Value'}")
    print("-" * 80)
    print(f"{'Original file size (bytes)':<40} | {size}")
    print(f"{'Total bits':<40} | {total_bits}")
    print(f"{'Bit differences':<40} | {diff}")
    print(f"{'Percentage errors':<40} | {percent_error:.6f}%")
    print(f"{'Encoding time (s)':<40} | {enctime:.6f}")
    print(f"{'Decoding time (s)':<40} | {dectime:.6f}")
    print(f"{'Encoding CPU time (s)':<40} | {cpu_enc:.6f}")
    print(f"{'Decoding CPU time (s)':<40} | {cpu_dec:.6f}")
    print(f"{'Encoding CPU ticks':<40} | {ticks_enc}")
    print(f"{'Decoding CPU ticks':<40} | {ticks_dec}")
    print(f"{'Temp change during Encoding (°C)':<40} | {temp_enc:+.2f}")
    print(f"{'Temp change during Decoding (°C)':<40} | {temp_dec:+.2f}")
    print(f"{'Estimated CPU energy Encoding (J)':<40} | {energy_enc:.6f}")
    print(f"{'Estimated CPU energy Decoding (J)':<40} | {energy_dec:.6f}")
    print("=" * 80)

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
    parser.add_argument("--builder", type=str, help="Path to Makefile that builds coder and decoder")
    parser.add_argument("--file-gen", type=str, default="tools/gen_file", help="Generator .c file")
    parser.add_argument("--repeat", type=int, default=1, help="Repeat count")
    
    # === Coder and Decoder setup ===
    parser.add_argument("--file-size", type=str, default="512", help="Source file (with data) size")
    parser.add_argument("--parity-bits", type=str, default="2", help="Parity bits count, see --parity-help for info")
    parser.add_argument("--parity-help", action="store_true")
    parser.add_argument("--src-file", type=str, default="image.img", help="Source file (with data) path that will be generated")
    parser.add_argument("--coded-file", type=str, default="image.enc", help="Encoded source file name")
    parser.add_argument("--decoded-file", type=str, default="image.dec", help="Decoded encoded source file name")
    
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

    _build_tools(gcc=args.gcc, gener=args.file_gen, makefile_dir=args.builder)
    
    diff = enctime = dectime = cpu_enc = cpu_dec = energy_enc = energy_dec = 0.0
    ticks_enc = ticks_dec = 0
    temp_enc = temp_dec = 0.0
    
    repeats = args.repeat
    for _ in range(repeats):
        _launch_tool(tool=args.file_gen, args=["--fs", args.file_size, "--out", args.src_file])
        
        print(f"[FILE] filling file {args.src_file} with random data...")
        _fill_file(path=args.src_file, size=int(args.file_size))
        
        enc_elapsed, enc_cpu, enc_ticks, enc_temp, enc_energy = _launch_tool(args.coder, ["--pb", args.parity_bits, "--target", args.src_file, "--out", args.coded_file])
        enctime += enc_elapsed
        cpu_enc += enc_cpu
        ticks_enc += enc_ticks
        temp_enc += enc_temp
        energy_enc += enc_energy
        
        if args.strategy == "random":
            random_bitflips(file_path=args.coded_file, num_flips=args.flips_size)
        elif args.strategy == "wnoise":
            print(f"WN strategy, flip_prob={args.flip_prob}")
            white_noise_bitflips(file_path=args.coded_file, flip_probability=args.flip_prob)
        elif args.strategy == "scratch":
            scratch_emulation(file_path=args.coded_file, scratch_length=args.scratch_length, width=args.width, intensity=args.intensity)
        
        dec_elapsed, dec_cpu, dec_ticks, dec_temp, dec_energy = _launch_tool(args.decoder, ["--pb", args.parity_bits, "--target", args.coded_file, "--out", args.decoded_file])
        dectime += dec_elapsed
        cpu_dec += dec_cpu
        ticks_dec += dec_ticks
        temp_dec += dec_temp
        energy_dec += dec_energy

        diff += _bit_differences(ffile=args.src_file, sfile=args.decoded_file)
    
    _print_statistics(
        size=int(args.file_size),
        diff=diff / repeats,
        enctime=enctime / repeats,
        dectime=dectime / repeats,
        cpu_enc=cpu_enc / repeats,
        cpu_dec=cpu_dec / repeats,
        ticks_enc=ticks_enc // repeats,
        ticks_dec=ticks_dec // repeats,
        temp_enc=temp_enc / repeats,
        temp_dec=temp_dec / repeats,
        energy_enc=energy_enc / repeats,
        energy_dec=energy_dec / repeats
    )
    
    _clean_tools(coder=args.coder, decoder=args.decoder, generator=args.file_gen)