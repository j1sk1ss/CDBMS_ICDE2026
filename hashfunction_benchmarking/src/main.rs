use clap::{Parser, Subcommand};

mod misc;
mod coltester;
mod perftester;

#[derive(Parser)]
#[command(name = "Toolbox")]
#[command(about = "Multimodule CLI tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    ColTester(coltester::Args),
    PerfTester(perftester::Args)
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::ColTester(args) => {
            coltester::run(args);
        },
        Commands::PerfTester(args) => {
            perftester::run(args);
        }
    }
}
