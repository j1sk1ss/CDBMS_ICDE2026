use clap::Parser;
use std::collections::HashMap;
use indicatif::ProgressBar;
use figlet_rs::FIGfont;
use crate::misc;

#[derive(Parser, Debug)]
#[command(author, version, about = "Test HashLib for collisions on dataset", long_about = None)]
pub struct Args {
    #[arg(long)]
    dataset: String,

    #[arg(long, default_value = "name")]
    column: String,

    #[arg(long, required = true, num_args=1..)]
    hashes: Vec<String>,

    #[arg(long, default_value_t = false)]
    save_col: bool
}

fn test_collisions_in_memory(
    dataset: &str,
    column: &str,
    save_col: bool,
    hasher: &dyn hash_lib::Hasher,
) -> () {
    println!("dataset={}, column={}, save_col={}", dataset, column, save_col);
    let mut rdr = match csv::Reader::from_path(dataset) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to open dataset {}: {}", dataset, e);
            return;
        }
    };

    let headers = rdr.headers().unwrap().clone();
    let col_index = match headers.iter().position(|h| h == column) {
        Some(i) => i,
        None => {
            eprintln!("Column '{}' not found in dataset headers: {:?}", column, headers);
            return;
        }
    };

    let total_records = std::fs::read_to_string(dataset).unwrap().lines().skip(1).count() as u64;
    let bar = ProgressBar::new(total_records);

    let mut hashes = HashMap::new();
    let mut collision_count = 0;

    for (i, result) in rdr.records().enumerate() {
        let record = match result {
            Ok(rec) => rec,
            Err(e) => {
                eprintln!("Failed to read record {}: {}", i + 1, e);
                continue;
            }
        };

        if let Some(value) = record.get(col_index) {
            let hash = hasher.hash(value.as_bytes());
            if hashes.contains_key(&hash) {
                collision_count += 1;
                if save_col {
                    println!("Collision: '{}' <-> '{}'", value, hashes[&hash]);
                }
            } 
            else {
                hashes.insert(hash.clone(), value.to_string());
            }
        }

        bar.inc(1);
    }

    print!("Collission count {}\n", collision_count);
}

pub fn run(args: Args) -> () {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("HashLib col-tester");
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
            test_collisions_in_memory(&args.dataset, &args.column, args.save_col, func.as_ref());
        }
    }
}
