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
- `datasets/` - Generated datasets for hash function performance tests. Extended datasets (with over 10M entries) available upon request.
- `scripts/` - Python scripts for collision testing, dataset GAN and random generation.
- `src/` - Rust benchmark for `HashLib`.
