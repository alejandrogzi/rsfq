/// rsfq: another fastq downloader but in rust
/// Alejandro Gonzales-Irribarren, 2025
///
/// This crate is a partial re-implementation of fastq-dump, a tool for downloading FASTQ files
/// from the SRA and ENA databases. It provides a command-line interface for downloading FASTQ
/// files and supports various options for customizing the download process. It covers the entire
/// process from accession to download.
///
/// To get help, run:
///
/// ```shell
/// rsfq --help
/// ```
///
/// The basic usage just requires a single accession:
///
/// ```shell
/// rsfq -a accession
/// ```
///
/// or a list of accessions separated by commas:
///
/// ```shell
/// rsfq -a accession1,accession2,accession3
/// ```
///
/// or in a .txt
///
/// ```shell
/// rsfq -a accessions.txt
/// ```
///
use std::path::PathBuf;

use clap::{self, Parser};
use log::{info, Level};
use simple_logger::init_with_level;
use tokio;

use rsfq::{
    cli::Args,
    core::get_fastqs,
    nf::distribute,
    utils::{__clean_nf_dirs, __move_to_root},
};

const NF_LOG: &str = ".nextflow.log";
const NF_HISTORY: &str = ".nextflow";

#[tokio::main]
async fn main() {
    let start = std::time::Instant::now();
    init_with_level(Level::Info).unwrap_or_else(|e| {
        panic!("Failed to initialize logger: {}", e);
    });

    let args: Args = Args::parse();
    args.check();

    if args.nextflow {
        match args.accession {
            rsfq::cli::AccessionType::Single(_) => {
                log::error!("ERROR: Nextflow mode can only accept a list of accessions!");
                std::process::exit(1);
            }
            rsfq::cli::AccessionType::List(accessions) => {
                let outdir = args.outdir.unwrap_or(PathBuf::from("DOWNLOADS"));

                log::info!("INFO: Running in Nextflow mode...");
                distribute(
                    accessions,
                    args.executor,
                    args.attempts,
                    &outdir,
                    args.threads,
                    args.queue,
                    args.sleep,
                    args.retriever,
                    args.queue_size,
                    args.provider,
                );

                log::info!("INFO: Cleaning and joining output files...");
                std::fs::remove_file(NF_LOG).unwrap_or_else(|e| {
                    log::error!("ERROR: Could not remove Nextflow log files!: {}", e);
                    std::process::exit(1);
                });
                std::fs::remove_dir_all(NF_HISTORY).unwrap_or_else(|e| {
                    log::error!("ERROR: Could not remove Nextflow history!: {}", e);
                    std::process::exit(1);
                });

                // INFO: moving/joining output files
                // INFO: here is also the place to use --group-by [not implemented yet]
                __move_to_root(&outdir);

                // LOGS.iter().for_each(|log| {
                //     let file = format!("{}.{}", "rsfq", log);
                //     __concat(&outdir, log, &file);
                // });

                __clean_nf_dirs(&outdir);
            }
        }
    } else {
        log::info!("INFO: Running in local mode...");
        get_fastqs(args).await;
    }

    let elapsed = start.elapsed();
    info!("Elapsed time: {:.3?}", elapsed);
}
