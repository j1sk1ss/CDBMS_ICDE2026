# Hamming testing
Example command:
```bash
python main.py \
    --gcc gcc-14 \
    --hamm-api ../ \
    --coder tools/file2hamm \
    --decoder tools/hamm2file \
    --file-gen tools/gen_file \
    --file-size 1024 \
    --parity-bits 3 \
    --strategy random \
    --flips-size 20
```

# Basic Setup
| Argument     | Type | Default           | Description                                                 |
| ------------ | ---- | ----------------- | ----------------------------------------------------------- |
| `--gcc`      | str  | `gcc-14`          | Available GCC version used to build the C tools.            |
| `--hamm-api` | str  | `..`              | Path to the Hamming API (header and source files).          |
| `--coder`    | str  | `tools/file2hamm` | Path to the coder C file (source to Hamming encoding tool). |
| `--decoder`  | str  | `tools/hamm2file` | Path to the decoder C file (Hamming decoding tool).         |
| `--file-gen` | str  | `tools/gen_file`  | Path to the file generator C file.                          |
| `--repeat`   | int  | 1                 | Number of times to repeat the full encode/decode process.   |

# Coder and Decoder Setup
| Argument         | Type | Default       | Description                                                                                  |
| ---------------- | ---- | ------------- | -------------------------------------------------------------------------------------------- |
| `--file-size`    | str  | `512`         | Size of the source file to generate (in bytes).                                              |
| `--parity-bits`  | str  | `2`           | Number of parity bits to use for Hamming encoding. Use `--parity-help` for more information. |
| `--parity-help`  | flag | False         | Show information about parity bits.                                                          |
| `--src-file`     | str  | `image.img`   | Path to the generated source file.                                                           |
| `--coded-file`   | str  | `image.hamm`  | Output path for the encoded (Hamming) file.                                                  |
| `--decoded-file` | str  | `decoded.img` | Output path for the decoded file.                                                            |

# Bitflips Emulation
| Argument           | Type  | Default | Description                                                                                        |
| ------------------ | ----- | ------- | -------------------------------------------------------------------------------------------------- |
| `--strategy`       | str   | `none`  | Strategy for error simulation: `random` (random bit flips) or `scratch` (continuous error region). |
| `--scratch-length` | int   | 1024    | Length of the scratch region in bytes (used with `scratch` strategy).                              |
| `--width`          | int   | 1       | Width of the scratch (number of bytes affected at a time).                                         |
| `--intensity`      | float | 0.7     | Probability of bit flips within the scratch region (0.0–1.0).                                      |
| `--flips-size`     | int   | 10      | Number of random bit flips (used with `random` strategy).                                          |
