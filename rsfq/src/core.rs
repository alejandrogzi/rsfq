use crate::{
    cli::{AccessionType, Args},
    provs::{
        ena::get_run_info,
        sra::{download_run as download_from_sra, SRAError},
        Provider,
    },
    utils::{validate_query, Layout, Retriever},
};

use futures::stream::{self, StreamExt};
use md5::Context;
use walkdir::WalkDir;

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

const PAIRED: &str = "PAIRED";
const SINGLE: &str = "SINGLE";
const FASTQ_FTP: &str = "fastq_ftp";
const FASTQ_MD5: &str = "fastq_md5";
const LIBRARY_LAYOUT: &str = "library_layout";
const RUN_ACCESSION: &str = "run_accession";
const R1: &str = "_1.fastq.gz";
const R2: &str = "_2.fastq.gz";
const MB: usize = 1_048_576; // 1 MB
const BUFFER_SIZE: usize = 10 * MB; // 10 MB
const QUEUE_SIZE: usize = 50; // 50 requests

const EXTENSIONS: &[&str] = &[
    ".fastq.gz",
    ".fq.gz",
    "_subreads.fastq.gz",
    "_subreads.fq.gz",
    ".subreads.fastq.gz",
    ".subreads.fq.gz",
];

/// Download fastq files for a single accession or a list of accessions
///
/// # Arguments
///
/// * `args` - Command line arguments
///
/// # Returns
///
/// * `Result<(), Error>` - Result of the operation
///
/// # Examples
///
/// ```rust, no_run
/// use rsfq::core::get_fastqs;
/// use rsfq::cli::{AccessionType, Args};
/// use rsfq::provs::Provider;
/// use rsfq::utils::{Layout, Retriever};
///
/// #[tokio::main]
/// async fn main() {
///     let args = Args {
///         accession: AccessionType::Single("SRR123456".to_string()),
///         outdir: None,
///         attempts: 3,
///         sleep: 5,
///         force: false,
///         metadata: false,
///         threads: 4,
///         group_by_experiment: false,
///         group_by_sample: false,
///         prefix: "fastq".to_string(),
///         nextflow: false,
///         executor: "local".to_string(),
///         queue: "null".to_string(),
///         check_if_downloadable: false,
///         retriever: Retriever::Aria2c,
///         queue_size: 10,
///         layout: Layout::Global,
///         provider: Provider::ENA,
///     };
///     get_fastqs(args).await;
/// }
/// ```
pub async fn get_fastqs(args: Args) {
    match args.accession {
        AccessionType::Single(accession) => {
            process_run(
                accession.clone(),
                args.outdir,
                args.attempts,
                args.sleep,
                args.force,
                args.metadata,
                args.retriever,
                args.check_if_downloadable,
                args.provider,
                args.layout,
                args.threads,
            )
            .await;
        }
        AccessionType::List(accessions) => {
            // INFO: download fastq files for a list of accessions
            let stream = stream::iter(accessions.into_iter().map(|accession| {
                process_run(
                    accession.clone(),
                    args.outdir.clone(),
                    args.attempts,
                    args.sleep,
                    args.force,
                    args.metadata,
                    args.retriever.clone(),
                    args.check_if_downloadable,
                    args.provider,
                    args.layout,
                    args.threads,
                )
            }))
            .buffer_unordered(QUEUE_SIZE);

            stream.collect::<Vec<_>>().await;
        }
    }
}

/// Process a single run and download the FASTQ files.
///
/// # Arguments
///
/// * `accession` - The accession number of the run to process.
/// * `outdir` - The output directory to save the downloaded files.
/// * `attempts` - The number of attempts to make when downloading the files.
/// * `sleep` - The number of seconds to sleep between attempts.
/// * `force` - Whether to force the download even if the file already exists.
/// * `metadata` - Whether to download the metadata for the run.
///
/// # Returns
///
/// * `Result<(), Error>` - A result indicating success or failure.
///
/// # Examples
///
/// ```rust, no_run
/// use rsfq::core::process_run;
/// use rsfq::provs::Provider;
/// use rsfq::utils::{Layout, Retriever};
///
/// #[tokio::main]
/// async fn main() {
///     process_run(
///         "ERR123456".to_string(),
///         None,
///         3,
///         5,
///         false,
///         false,
///         Retriever::Aria2c,
///         false,
///         Provider::ENA,
///         Layout::Global,
///         4,
///     )
///     .await;
/// }
/// ```
pub async fn process_run(
    accession: String,
    outdir: Option<PathBuf>,
    attempts: usize,
    sleep: usize,
    force: bool,
    metadata: bool,
    retriever: Retriever,
    check_if_downloadable: bool,
    provider: Provider,
    layout: Layout,
    threads: usize,
) {
    let query = validate_query(&accession);

    let data = get_run_info(query, attempts, sleep).await;

    if metadata || check_if_downloadable {
        if check_if_downloadable {
            let binding = HashMap::new();
            let run = data.get(0).unwrap_or(&binding);

            if run.is_empty() {
                println!("NOT_FOUND\t{}", accession);
            } else {
                let binding = String::new();
                let fastq_ftp = run.get(FASTQ_FTP).unwrap_or(&binding);

                if fastq_ftp.is_empty() {
                    println!("NOT_FOUND\t{}", accession);
                } else {
                    println!("DOWNLOADABLE\t{}", accession);
                }
            }
        } else {
            log::info!("Found {} runs!", data.len());
            log::info!("Run data: {:#?}", data);
        }
        return;
    }

    if data.len() > 1 {
        log::warn!("WARNING: More than one run found! Using the first one...");
    }

    let run = data
        .get(0)
        .unwrap_or_else(|| {
            log::error!("ERROR: No data found!");
            std::process::exit(1);
        })
        .to_owned();

    log::info!("Run data: {:#?}", data);

    match provider {
        Provider::ENA => {
            let _ = download_fastq(
                run.clone(),
                outdir.clone(),
                attempts,
                sleep,
                force,
                retriever,
                layout,
            )
            .await;
        }
        Provider::SRA => {
            let run_accession = run
                .get(RUN_ACCESSION)
                .unwrap_or_else(|| {
                    log::error!("ERROR: No run_accession field found in the run data!");
                    std::process::exit(1);
                })
                .to_string();

            let target_outdir = outdir.clone().unwrap_or_else(|| PathBuf::from("DOWNLOADS"));

            match download_from_sra(
                &run_accession,
                &target_outdir,
                threads,
                attempts,
                sleep,
                force,
                layout,
            )
            .await
            {
                Ok(paths) => {
                    log::info!("Downloaded {} via SRA: {:?}", run_accession, paths);
                }
                Err(SRAError::MissingTool(tool)) => {
                    log::warn!(
                        "{} not found. Falling back to ENA download for {}",
                        tool,
                        run_accession
                    );
                    let _ = download_fastq(
                        run.clone(),
                        outdir,
                        attempts,
                        sleep,
                        force,
                        retriever,
                        layout,
                    )
                    .await;
                }
                Err(err) => {
                    log::error!(
                        "ERROR: SRA download failed for {}: {:?}",
                        run_accession,
                        err
                    );
                    std::process::exit(1);
                }
            }
        }
    }
}

/// Download the FASTQ files for a given run.
///
/// # Arguments
///
/// * `run` - A HashMap containing the run information.
/// * `outdir` - An optional output directory where the downloaded files will be saved.
/// * `attempts` - The number of attempts to download the files.
/// * `sleep` - The sleep duration in seconds between attempts.
/// * `force` - A flag indicating whether to force the download even if the file already exists.
///
/// # Returns
///
/// A Result indicating the success or failure of the download operation.
///
/// # Example
///
/// ```rust, no_run
/// use rsfq::core::download_fastq;
/// use rsfq::utils::{Layout, Retriever};
/// use std::collections::HashMap;
/// use std::path::Path;
///
/// #[tokio::main]
/// async fn main() {
///     let run = HashMap::from([
///         ("fastq_ftp".to_string(), "ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR123456/SRR123456.fastq.gz".to_string()),
///         ("fastq_md5".to_string(), "md5sum".to_string()),
///         ("library_layout".to_string(), "SINGLE".to_string()),
///         ("run_accession".to_string(), "SRR123456".to_string()),
///     ]);
///     let outdir = Some(Path::new("/path/to/output"));
///     let attempts = 3;
///     let sleep = 5;
///     let force = false;
///     let retriever = Retriever::Aria2c;
///     let layout = Layout::Global;
///
///     download_fastq(run, outdir, attempts, sleep, force, retriever, layout).await;
/// }
/// ```
pub async fn download_fastq<K: AsRef<Path> + Debug + Send + Sync>(
    run: HashMap<String, String>,
    outdir: Option<K>,
    attempts: usize,
    sleep: usize,
    force: bool,
    retriever: Retriever,
    layout: Layout,
) {
    let fastq_ftp = run.get(FASTQ_FTP).unwrap_or_else(|| {
        log::error!("ERROR: No fastq_ftp field found in the run data!");
        std::process::exit(1);
    });
    let fastq_md5 = run.get(FASTQ_MD5).unwrap_or_else(|| {
        log::error!("ERROR: No fastq_md5 field found in the run data!");
        std::process::exit(1);
    });
    let library_layout = run.get(LIBRARY_LAYOUT).unwrap_or_else(|| {
        log::error!("ERROR: No library_layout field found in the run data!");
        std::process::exit(1);
    });
    let accession = run.get(RUN_ACCESSION).unwrap_or_else(|| {
        log::error!("ERROR: No run_accession field found in the run data!");
        std::process::exit(1);
    });

    let outdir = outdir
        .as_ref()
        .map(|x| x.as_ref())
        .unwrap_or_else(|| Path::new("DOWNLOADS"));

    let ftp_entries = fastq_ftp.split(';').collect::<Vec<&str>>();
    let md5_entries = fastq_md5.split(';');

    // INFO: performs strick matching of the number of files, scRNA-Seq will have only one file
    match layout {
        Layout::Single => {
            if ftp_entries.len() != 1 {
                log::error!(
                    "ERROR: Only single FASTQ files were expected! Found {} files for {}",
                    ftp_entries.len(),
                    accession
                );
                std::process::exit(1);
            }
        }
        Layout::Paired => {
            if ftp_entries.len() != 2 {
                log::error!(
                    "ERROR: Only paired FASTQ files were expected! Found {} files for {}",
                    ftp_entries.len(),
                    accession
                );
                std::process::exit(1);
            }
        }
        Layout::Global => {}
    }

    for (ftp, md5) in ftp_entries.into_iter().zip(md5_entries) {
        let observed = Path::new(ftp)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_else(|| {
                log::error!("ERROR: Could not extract filename from {}", ftp);
                std::process::exit(1);
            });

        if library_layout == PAIRED {
            if !(ftp.ends_with(R1) || ftp.ends_with(R2)) {
                if !__has_expected_filename(accession, observed, EXTENSIONS) {
                    log::error!(
                        "ERROR: Expected {}.fastq.gz/.fq.gz/*subreads.fastq.gz but found {} in the fastq_ftp field",
                        accession,
                        observed
                    );
                    std::process::exit(1);
                }
            }
        } else if library_layout == SINGLE {
            if !__has_expected_filename(accession, observed, EXTENSIONS) {
                log::error!(
                    "ERROR: Expected {}.fastq.gz/.fq.gz/*subreads.fastq.gz but found {} in the fastq_ftp field",
                    accession,
                    observed
                );
                std::process::exit(1);
            }
        }

        if md5.is_empty() {
            log::error!("ERROR: No MD5 checksum found for {}", ftp);
            std::process::exit(1);
        }

        let _ = download(ftp, outdir, attempts, sleep, force, md5, retriever).await;
    }
}

/// Check if a filename has one of the expected extensions.
///
/// # Arguments
///
/// * `filename` - The filename to check.
/// * `extensions` - The expected extensions.
///
/// # Returns
///
/// `true` if the filename has one of the expected extensions, `false` otherwise.
///
/// # Example
///
/// ```rust, no_run
/// use rsfq::core::__has_expected_filename;
/// let filename = "example.fastq.gz";
/// let extensions = &["fastq.gz", "fq.gz"];
/// let accession = "example";
/// assert!(__has_expected_filename(accession, filename, extensions));
/// ```
pub fn __has_expected_filename(accession: &str, observed: &str, extensions: &[&str]) -> bool {
    extensions.iter().any(|&ext| {
        let expected = format!("{}{}", accession, ext);
        expected == observed
    })
}

/// Download a file from an FTP server and verify its MD5 checksum.
///
/// # Arguments
///
/// * `ftp` - The FTP URL of the file to download.
/// * `outdir` - The directory where the file should be downloaded.
/// * `max_attempts` - The maximum number of download attempts.
/// * `sleep` - The number of seconds to sleep between attempts.
/// * `force` - Whether to overwrite an existing file.
/// * `md5` - The expected MD5 checksum of the file.
///
/// # Returns
///
/// An `Option<PathBuf>` containing the path to the downloaded file, or `None` if the download failed.
///
/// # Example
///
/// ```rust, no_run
/// use rsfq::core::download;
/// use rsfq::utils::Retriever;
/// use std::path::PathBuf;
///
/// #[tokio::main]
/// async fn main() {
///     let ftp = "ftp://ftp.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByRun/sra/SRR/SRR123456/SRR123456.fastq.gz";
///     let outdir = PathBuf::from("/path/to/output");
///     let md5 = "md5sum";
///     let retriever = Retriever::Aria2c;
///
///     match download(ftp, &outdir, 3, 5, false, md5, retriever).await {
///         Some(path) => println!("Downloaded file to: {}", path.display()),
///         None => println!("Download failed"),
///     }
/// }
/// ```
pub async fn download<K: AsRef<Path> + Debug>(
    ftp: &str,
    outdir: K,
    max_attempts: usize,
    sleep: usize,
    force: bool,
    md5: &str,
    retriever: Retriever,
) -> Option<PathBuf> {
    let mut attempt = 0;
    let fastq = outdir.as_ref().join(
        Path::new(ftp)
            .file_name()
            .unwrap_or_else(|| {
                log::error!("ERROR: No file name found");
                std::process::exit(1);
            })
            .to_str()
            .unwrap_or_else(|| {
                log::error!("ERROR: Invalid file name!");
                std::process::exit(1);
            }),
    );

    log::info!("Downloading {} to {}", ftp, fastq.display());

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

    let mut cmd = retriever.materialize(ftp, &fastq);

    while max_attempts >= attempt {
        let output = cmd.output().await.unwrap_or_else(|e| {
            log::error!("ERROR: Failed to execute command: {}", e);
            std::process::exit(1);
        });

        let status = output.status.code().unwrap_or_else(|| {
            log::error!("ERROR: No exit code found!");
            std::process::exit(1);
        });

        if status != 0 {
            log::error!("ERROR: Failed to download {} with status {}", ftp, status);
            attempt += 1;
            tokio::time::sleep(tokio::time::Duration::from_secs(sleep as u64)).await;
        } else {
            if force {
                log::info!("--force used, skipping MD5sum check for {}", ftp);
                break;
            } else {
                let fq_md5 = md5sum(&fastq).await.unwrap_or_else(|| {
                    log::error!("ERROR: Failed to calculate MD5sum!");
                    std::process::exit(1);
                });

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

/// Calculate the MD5 checksum of a FASTQ file.
///
/// # Arguments
///
/// * `fastq` - A reference to a `Path` or `PathBuf` representing the FASTQ file.
///
/// # Returns
///
/// An `Option<String>` containing the MD5 checksum as a hexadecimal string, or `None` if an error occurs.
///
/// # Examples
///
/// ```rust, no_run
/// use rsfq::core::md5sum;
/// use std::path::Path;
///
/// #[tokio::main]
/// async fn main() {
///     let fastq = Path::new("path/to/fastq/file.fastq");
///     let md5 = md5sum(&fastq).await;
///     println!("MD5 checksum: {:?}", md5);
/// }
/// ```
pub async fn md5sum<K: AsRef<Path> + Debug>(fastq: &K) -> Option<String> {
    let fastq = if !fastq.as_ref().exists() {
        check_fq_path(fastq).unwrap_or_else(|| {
            log::error!("ERROR: File not found!");
            std::process::exit(1);
        })
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

/// Check if the provided fastq path is valid and return the absolute path.
///
/// # Arguments
///
/// * `fastq` - The path to the fastq file to check.
///
/// # Returns
///
/// * `Option<PathBuf>` - The absolute path of the fastq file if it exists, or `None` if it does not.
///
/// # Examples
///
/// ```rust, no_run
/// use rsfq::core::check_fq_path;
/// use std::path::PathBuf;
/// let fastq_path = PathBuf::from("/path/to/fastq");
/// let absolute_path = check_fq_path(fastq_path);
/// assert!(absolute_path.is_some());
/// ```
pub fn check_fq_path<K: AsRef<Path> + Debug>(fastq: K) -> Option<PathBuf> {
    // WARN: try to look inside the Nextflow work directory
    let nf_work_dir = std::env::current_dir().unwrap_or_else(|e| {
        log::error!("ERROR: Could not get current directory!: {}", e);
        std::process::exit(1);
    });

    if nf_work_dir.exists() {
        let filename = fastq.as_ref().file_name().unwrap_or_else(|| {
            log::error!("ERROR: No file name found");
            std::process::exit(1);
        });

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
