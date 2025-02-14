use crate::{
    cli::{AccessionType, Args},
    provs::ena::get_run_info,
    utils::validate_query,
};

use md5::Context;
use tokio::process::Command;
use walkdir::WalkDir;

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

const PAIRED: &str = "PAIRED";
const R1: &str = "_1.fastq.gz";
const R2: &str = "_2.fastq.gz";
const MB: usize = 1_048_576; // 1 MB
const BUFFER_SIZE: usize = 10 * MB; // 10 MB

pub async fn get_fastqs(args: Args) {
    match args.accession {
        AccessionType::Single(accession) => {
            let query = validate_query(&accession);
            let data = get_run_info(query, args.attempts, args.sleep).await;

            if data.len() > 1 {
                log::warn!("WARNING: More than one run found! Using the first one...");
            }

            // INFO: just download the run
            let run = data.get(0).expect("ERROR: No data found!").to_owned();
            let _ = download_fastq(run, args.outdir, args.attempts, args.sleep, args.force).await;
        }
        AccessionType::List(_) => {
            // INFO: download fastq files for a list of accessions
            todo!()
        }
    }
}

pub async fn download_fastq<K: AsRef<Path> + Debug + Send + Sync>(
    run: HashMap<String, String>,
    outdir: Option<K>,
    attempts: usize,
    sleep: usize,
    force: bool,
) {
    let ftp = run
        .get("fastq_ftp")
        .expect("ERROR: No fastq_ftp field found in the run data!");
    let md5 = run
        .get("fastq_md5")
        .expect("ERROR: No fastq_md5 field found in the run data!");
    let layout = run
        .get("library_layout")
        .expect("ERROR: No library_layout field found in the run data!");

    for (ftp, md5) in ftp.split(';').zip(md5.split(';')) {
        if layout == PAIRED {
            if !ftp.ends_with(R1) && !ftp.ends_with(R2) {
                let observed = Path::new(ftp).file_name().unwrap().to_str().unwrap();
                let expected = format!("{}.fastq.gz", run.get("run_accession").unwrap());

                if observed != expected {
                    log::error!(
                        "ERROR: Expected {} but found {} in the fastq_ftp field",
                        expected,
                        observed
                    );
                    std::process::exit(1);
                }
            }

            if !md5.is_empty() {
                let outdir = outdir
                    .as_ref()
                    .map(|x| x.as_ref())
                    .unwrap_or_else(|| Path::new("DOWNLOADS"));
                let _ = download(ftp, outdir, attempts, sleep, force, md5).await;
            } else {
                log::error!("ERROR: No MD5 checksum found for {}", ftp);
                std::process::exit(1);
            }
        }
    }
}

pub async fn download<K: AsRef<Path> + Debug>(
    ftp: &str,
    outdir: K,
    max_attempts: usize,
    sleep: usize,
    force: bool,
    md5: &str,
) -> Option<PathBuf> {
    let mut attempt = 0;
    let fastq = outdir.as_ref().join(
        Path::new(ftp)
            .file_name()
            .expect("ERROR: No file name found")
            .to_str()
            .expect("ERROR: Invalid file name!"),
    );

    log::info!("Downloading {} to {}", ftp, fastq.display(),);
    if fastq.exists() {
        if force {
            log::warn!(
                "WARNING: File {} already exists! Overwriting...",
                fastq.display()
            );
        } else {
            log::warn!(
                "WARNING: File {} already exists! Skipping download...",
                fastq.display()
            );
            return None;
        }
    }

    let mut cmd = Command::new("aria2c");
    cmd.arg("-x4")
        .arg("-c")
        .arg(format!("-o {}", fastq.display()))
        .arg(format!("http://{}", ftp));

    while max_attempts >= attempt {
        let output = cmd
            .output()
            .await
            .expect("ERROR: Failed to execute command");
        let status = output.status.code().expect("ERROR: No exit code found!");

        if status != 0 {
            log::error!("ERROR: Failed to download {} with status {}", ftp, status);
            attempt += 1;
            tokio::time::sleep(tokio::time::Duration::from_secs(sleep as u64)).await;
        } else {
            if force {
                log::info!("--force used, skipping MD5sum check for {}", ftp);
                break;
            } else {
                let fq_md5 = md5sum(&fastq)
                    .await
                    .expect("ERROR: Failed to calculate MD5sum!");

                if fq_md5 != md5 {
                    log::error!(
                        "ERROR: MD5 checksum failed for {}. Expected: {} Observed: {}",
                        ftp,
                        md5,
                        fq_md5
                    );
                    attempt += 1;
                    tokio::time::sleep(tokio::time::Duration::from_secs(sleep as u64)).await;
                } else {
                    log::info!("Downloaded {} successfully!", ftp);
                    break;
                }
            }
        }
    }

    Some(fastq)
}

pub async fn md5sum<K: AsRef<Path> + Debug>(fastq: &K) -> Option<String> {
    let fastq = if !fastq.as_ref().exists() {
        check_fq_path(fastq).expect("ERROR: File not found!")
    } else {
        fastq.as_ref().to_path_buf()
    };

    let file = File::open(fastq).ok()?;
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
    let mut hasher = Context::new();
    let mut buffer = vec![0; BUFFER_SIZE];

    loop {
        let bytes_read = reader.read(&mut buffer).ok()?;
        if bytes_read == 0 {
            break;
        }
        hasher.consume(&buffer[..bytes_read]);
    }

    Some(format!("{:x}", hasher.compute()))
}

fn check_fq_path<K: AsRef<Path> + Debug>(fastq: K) -> Option<PathBuf> {
    // WARN: try to look inside the Nextflow work directory
    let nf_work_dir = std::env::current_dir().expect("ERROR: Could not get current directory!");

    if nf_work_dir.exists() {
        let filename = fastq
            .as_ref()
            .file_name()
            .expect("ERROR: No file name found");

        for entry in WalkDir::new(nf_work_dir)
            .min_depth(2)
            .into_iter()
            .filter_map(Result::ok)
        {
            if entry.file_name() == filename && entry.path().is_file() {
                log::warn!(
                    "Found {} inside the Nextflow work directory!",
                    entry.path().display()
                );
                return Some(entry.into_path());
            }
        }
    }

    log::error!("ERROR: File {:?} not found!", fastq);
    None
}
