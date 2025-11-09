use crate::utils::Layout;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;
use which::which;

const PREFETCH: &str = "prefetch";
const FASTERQ_DUMP: &str = "fasterq-dump";
const PIGZ: &str = "pigz";

/// Errors that can occur while downloading runs from SRA.
#[derive(Debug)]
pub enum SRAError {
    MissingTool(&'static str),
    CommandFailed { tool: &'static str, code: i32 },
    NotFound(&'static str),
    Io(std::io::Error),
    NoFastqProduced(String),
    LayoutMismatch(String),
}

impl From<std::io::Error> for SRAError {
    fn from(value: std::io::Error) -> Self {
        SRAError::Io(value)
    }
}

/// Ensure all SRA command line tools are available in PATH.
///
/// # Returns
///
/// A `Result` with an `SRAError` if any of the tools are not available.
fn ensure_tools() -> Result<(), SRAError> {
    for tool in [PREFETCH, FASTERQ_DUMP, PIGZ] {
        which(tool).map_err(|_| SRAError::MissingTool(tool))?;
    }
    Ok(())
}

/// Download FASTQs for a run accession via SRA.
///
/// # Arguments
///
/// * `accession` - The SRA run accession to download.
/// * `outdir` - The directory to download the FASTQs to.
/// * `threads` - The number of threads to use for downloading.
/// * `attempts` - The number of attempts to make for each download.
/// * `sleep` - The number of seconds to sleep between attempts.
/// * `force` - Whether to force downloading of existing files.
/// * `layout` - The layout of the run.
///
/// # Returns
///
/// A vector of paths to the downloaded FASTQs.
///
/// # Example
///
/// ```no_run
/// use rsfq::provs::sra::download_run;
/// use rsfq::provs::sra::Layout;
///
/// let outdir = "~/Downloads/SRA";
/// let layout = Layout::Paired;
///
/// download_run(
///     "SRR123456",
///     outdir,
///     4,
///     3,
///     5,
///     false,
///     layout,
/// ).await.unwrap();
/// ```
pub async fn download_run<K: AsRef<Path>>(
    accession: &str,
    outdir: K,
    threads: usize,
    attempts: usize,
    sleep: usize,
    force: bool,
    layout: Layout,
) -> Result<Vec<PathBuf>, SRAError> {
    ensure_tools()?;

    let outdir = outdir.as_ref();
    std::fs::create_dir_all(outdir)?;

    let gz_paths = gz_candidates(accession, outdir);
    if !force && layout_satisfied(layout, outdir, accession) {
        log::info!(
            "Skipping download for {} because FASTQ files already exist",
            accession
        );
        return Ok(existing_paths(&gz_paths));
    }

    if force {
        remove_existing(&gz_paths)?;
    }

    run_with_retry(
        || {
            let mut cmd = Command::new(PREFETCH);
            cmd.arg(accession)
                .arg("--max-size")
                .arg("10T")
                .arg("-o")
                .arg(format!("{}.sra", accession))
                .current_dir(outdir);
            cmd
        },
        attempts,
        sleep,
        PREFETCH,
    )
    .await?;

    run_with_retry(
        || {
            let mut cmd = Command::new(FASTERQ_DUMP);
            cmd.arg(accession)
                .arg("--split-3")
                .arg("--mem")
                .arg("1G")
                .arg("--threads")
                .arg(threads.max(1).to_string())
                .current_dir(outdir);
            cmd
        },
        attempts,
        sleep,
        FASTERQ_DUMP,
    )
    .await?;

    let produced = compress_fastqs(accession, outdir, threads).await?;
    cleanup_sra(accession, outdir)?;

    if !layout_satisfied(layout, outdir, accession) {
        return Err(SRAError::LayoutMismatch(accession.to_string()));
    }

    Ok(if produced.is_empty() {
        existing_paths(&gz_paths)
    } else {
        produced
    })
}

/// Compress FASTQs for a run accession via SRA.
///
/// # Arguments
///
/// * `accession` - The SRA run accession to download.
/// * `outdir` - The directory to download the FASTQs to.
/// * `threads` - The number of threads to use for downloading.
///
/// # Returns
///
/// A vector of paths to the compressed FASTQs.
///
/// # Example
///
/// ```no_run
/// use rsfq::provs::sra::compress_fastqs;
/// use rsfq::provs::sra::Layout;
///
/// let outdir = "~/Downloads/SRA";
/// let layout = Layout::Paired;
///
/// compress_fastqs(
///     "SRR123456",
///     outdir,
///     4,
/// ).await.unwrap();
/// ```
async fn compress_fastqs(
    accession: &str,
    outdir: &Path,
    threads: usize,
) -> Result<Vec<PathBuf>, SRAError> {
    let cpus = threads.max(1).to_string();
    let raw_candidates = raw_candidates(accession, outdir);
    let mut produced = Vec::new();

    for raw in raw_candidates {
        if raw.exists() {
            let gz = PathBuf::from(format!("{}.gz", raw.to_string_lossy()));

            run_with_retry(
                || {
                    let mut cmd = Command::new(PIGZ);
                    cmd.arg("--force")
                        .arg("-p")
                        .arg(&cpus)
                        .arg("-n")
                        .arg(&raw)
                        .current_dir(outdir);
                    cmd
                },
                1,
                0,
                PIGZ,
            )
            .await?;

            produced.push(gz);
        }
    }

    if produced.is_empty() {
        Err(SRAError::NoFastqProduced(accession.to_string()))
    } else {
        Ok(produced)
    }
}

/// Remove the SRA file for a run accession.
///
/// # Arguments
///
/// * `accession` - The SRA run accession to download.
/// * `outdir` - The directory to download the FASTQs to.
///
/// # Returns
///
/// A `Result` with an `SRAError` if the SRA file could not be removed.
fn cleanup_sra(accession: &str, outdir: &Path) -> Result<(), SRAError> {
    let sra = outdir.join(format!("{}.sra", accession));
    if sra.exists() {
        std::fs::remove_file(&sra)?;
    }
    Ok(())
}

/// Check if the layout of a run is satisfied by the downloaded FASTQs.
///
/// # Arguments
///
/// * `layout` - The layout of the run.
/// * `outdir` - The directory to download the FASTQs to.
/// * `accession` - The SRA run accession to download.
///
/// # Returns
///
/// A boolean indicating if the layout is satisfied.
fn layout_satisfied(layout: Layout, outdir: &Path, accession: &str) -> bool {
    let [single, r1, r2] = gz_candidates(accession, outdir);
    let has_single = single.exists();
    let has_paired = r1.exists() && r2.exists();

    match layout {
        Layout::Single => has_single,
        Layout::Paired => has_paired,
        Layout::Global => has_single || has_paired,
    }
}

/// Remove existing FASTQs for a run accession.
///
/// # Arguments
///
/// * `paths` - The paths to the FASTQs to remove.
///
/// # Returns
///
/// A `Result` with an `SRAError` if any of the FASTQs could not be removed.
fn remove_existing(paths: &[PathBuf; 3]) -> Result<(), SRAError> {
    for path in paths {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    Ok(())
}

/// Get the paths to the existing FASTQs for a run accession.
///
/// # Arguments
///
/// * `paths` - The paths to the FASTQs to check.
///
/// # Returns
///
/// A vector of paths to the existing FASTQs.
fn existing_paths(paths: &[PathBuf; 3]) -> Vec<PathBuf> {
    paths
        .iter()
        .filter(|p| p.exists())
        .cloned()
        .collect::<Vec<_>>()
}

/// Get the paths to the FASTQs for a run accession.
///
/// # Arguments
///
/// * `accession` - The SRA run accession to download.
/// * `outdir` - The directory to download the FASTQs to.
///
/// # Returns
///
/// A vector of paths to the FASTQs.
fn gz_candidates(accession: &str, outdir: &Path) -> [PathBuf; 3] {
    [
        outdir.join(format!("{}.fastq.gz", accession)),
        outdir.join(format!("{}_1.fastq.gz", accession)),
        outdir.join(format!("{}_2.fastq.gz", accession)),
    ]
}

/// Get the paths to the raw FASTQs for a run accession.
///
/// # Arguments
///
/// * `accession` - The SRA run accession to download.
/// * `outdir` - The directory to download the FASTQs to.
///
/// # Returns
///
/// A vector of paths to the raw FASTQs.
fn raw_candidates(accession: &str, outdir: &Path) -> [PathBuf; 3] {
    [
        outdir.join(format!("{}.fastq", accession)),
        outdir.join(format!("{}_1.fastq", accession)),
        outdir.join(format!("{}_2.fastq", accession)),
    ]
}

/// Run a command with retry.
///
/// # Arguments
///
/// * `builder` - The builder for the command to run.
/// * `attempts` - The number of attempts to make for each download.
/// * `sleep` - The number of seconds to sleep between attempts.
/// * `tool` - The name of the tool to run.
///
/// # Returns
///
/// A `Result` with an `SRAError` if the command fails.
///
/// # Example
///
/// ```no_run
/// use rsfq::provs::sra::run_with_retry;
///
/// run_with_retry(
///     || {
///         let mut cmd = Command::new("ls");
///         cmd.arg("-l");
///         cmd
///     },
///     3,
///     5,
///     "ls",
/// ).await.unwrap();
/// ```
async fn run_with_retry<F>(
    mut builder: F,
    attempts: usize,
    sleep: usize,
    tool: &'static str,
) -> Result<(), SRAError>
where
    F: FnMut() -> Command,
{
    let mut current_attempt = 0;
    while current_attempt < attempts {
        current_attempt += 1;
        let mut command = builder();
        let status = command.status().await?;

        if status.success() {
            return Ok(());
        }

        match status.code() {
            Some(3) => return Err(SRAError::NotFound(tool)),
            Some(code) => {
                if current_attempt >= attempts {
                    return Err(SRAError::CommandFailed { tool, code });
                }
            }
            None => {
                return Err(SRAError::CommandFailed { tool, code: -1 });
            }
        }

        tokio::time::sleep(Duration::from_secs(sleep as u64)).await;
    }
    Err(SRAError::CommandFailed { tool, code: 1 })
}
