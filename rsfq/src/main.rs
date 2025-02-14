use clap::{self, Parser};
use log::{info, Level};
use simple_logger::init_with_level;
use tokio;

use rsfq::{cli::Args, core::get_fastqs, nf::distribute};

const NF_LOG: &str = ".nextflow.log";
const NF_WORK: &str = "work";
const NF_HISTORY: &str = ".nextflow";

#[tokio::main]
async fn main() {
    let start = std::time::Instant::now();
    init_with_level(Level::Info).unwrap();

    let args: Args = Args::parse();
    args.check();

    if args.nextflow {
        match args.accession {
            rsfq::cli::AccessionType::Single(_) => {
                log::error!("ERROR: Nextflow mode can only accept a list of accessions!");
                std::process::exit(1);
            }
            rsfq::cli::AccessionType::List(accessions) => {
                log::info!("INFO: Running in Nextflow mode...");
                distribute(
                    accessions,
                    args.executor,
                    args.attempts,
                    args.outdir,
                    args.threads,
                    args.queue,
                    args.sleep,
                );

                std::fs::remove_file(NF_LOG).expect("ERROR: Could not remove Nextflow log files!");
                std::fs::remove_dir_all(NF_WORK)
                    .expect("ERROR: Could not remove Nextflow work directory!");
                std::fs::remove_dir_all(NF_HISTORY)
                    .expect("ERROR: Could not remove Nextflow history!");
            }
        }
    } else {
        get_fastqs(args).await;
    }

    let elapsed = start.elapsed();
    info!("Elapsed time: {:.3?}", elapsed);
}
