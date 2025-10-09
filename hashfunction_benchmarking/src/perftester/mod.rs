use std::time::{Duration, Instant};
use clap::Parser;
use indicatif::ProgressBar;
use figlet_rs::FIGfont;
use crate::misc;

#[derive(Parser, Debug)]
#[command(author, version, about = "Test HashLib perfomance on dataset", long_about = None)]
pub struct Args {
    #[arg(long)]
    dataset: String,

    #[arg(long, default_value = "name")]
    column: String,

    #[arg(long, required = true, num_args=1..)]
    hashes: Vec<String>
}

fn test_perfomance_in_memory(
    dataset: &str,
    column: &str,
    hasher: &dyn hash_lib::Hasher,
) -> Duration {
    println!("dataset={}, column={}", dataset, column);
    let mut rdr = match csv::Reader::from_path(dataset) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to open dataset {}: {}", dataset, e);
            return Duration::new(0, 0);
        }
    };

    let headers = rdr.headers().unwrap().clone();
    let col_index = match headers.iter().position(|h| h == column) {
        Some(i) => i,
        None => {
            eprintln!("Column '{}' not found in dataset headers: {:?}", column, headers);
            return Duration::new(0, 0);
        }
    };

    let total_records = std::fs::read_to_string(dataset).unwrap().lines().skip(1).count() as u64;
    let bar = ProgressBar::new(total_records);

    let mut total_bytes: u64 = 0;
    let start = Instant::now();
    for (i, result) in rdr.records().enumerate() {
        let record = match result {
            Ok(rec) => rec,
            Err(e) => {
                eprintln!("Failed to read record {}: {}", i + 1, e);
                continue;
            }
        };

        if let Some(value) = record.get(col_index) {
            let bytes = value.as_bytes();
            total_bytes += bytes.len() as u64;
            hasher.hash(bytes);
        }

        bar.inc(1);
    }

    bar.finish();
    let duration = start.elapsed();
    let seconds = duration.as_secs_f64();
    let throughput_mib = (total_bytes as f64) / (1024.0 * 1024.0) / seconds;

    println!(
        "Hashed {} bytes in {:.2?} ({:.2} MiB/s)",
        total_bytes, duration, throughput_mib
    );

    return duration;
}

pub fn run(args: Args) -> () {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("HashLib perf-tester");
    println!("{}", figure.unwrap());

    let hash_functions = misc::get_hash_functions();
    let available_names: std::collections::HashSet<String> = hash_functions.iter().map(|f| f.name()).collect();
    let unknown: Vec<&String> = args.hashes.iter().filter(|h| !available_names.contains(*h)).collect();
    if !unknown.is_empty() {
        eprintln!("Unknown hash functions: {:?}", unknown);
        eprintln!("Available: {:?}", available_names);
        std::process::exit(1);
    }

    for func in hash_functions.iter() {
        if args.hashes.iter().any(|h| h == &func.name()) {
            println!("--- Running hash function: {} ---", func.name());
            test_perfomance_in_memory(&args.dataset, &args.column, func.as_ref());
        }
    }
}
