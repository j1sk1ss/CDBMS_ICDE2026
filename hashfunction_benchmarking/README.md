# Usage
## Perfomance 
```
cargo run -- perf-tester --dataset <path> --hashes <names>
```
## Collisions
```
cargo run -- col-tester --dataset <path> --hashes <names>
```

# Directory
- `datasets/` - Generated datasets for hash functions perfomance tests.
- `scripts/` - Python scripts for collision testing, dataset GAN and random generation.
- `src/` - Rust benchmark for `HashLib`.
