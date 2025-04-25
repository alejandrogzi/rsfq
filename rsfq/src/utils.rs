use once_cell::sync::Lazy;
use regex::Regex;
use tokio::process::Command;
use walkdir::WalkDir;

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;

static PROJECT_STUDY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^PRJ[EDN][A-Z][0-9]+$|^[EDS]RP[0-9]{6,}$").unwrap());
static SAMPLE_BIOSAMPLE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^SAM[EDN][A-Z]?[0-9]+$|^[EDS]RS[0-9]{6,}$").unwrap());
static EXPERIMENT_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[EDS]RX[0-9]{6,}$").unwrap());
static RUN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[EDS]RR[0-9]{6,}$").unwrap());

/// Validate a query string and return a formatted query string.
///
/// # Arguments
///
/// * `query` - The query string to validate.
///
/// # Returns
///
/// A formatted query string.
///
/// # Examples
///
/// ```
/// let query = "PRJEDNA12345";
/// let formatted_query = validate_query(query);
/// assert_eq!(formatted_query, "(study_accession=PRJEDNA12345 OR secondary_study_accession=PRJEDNA12345)");
/// ```
pub fn validate_query(query: &str) -> String {
    if PROJECT_STUDY_RE.is_match(query) {
        format!(
            "(study_accession={} OR secondary_study_accession={})",
            query, query
        )
    } else if SAMPLE_BIOSAMPLE_RE.is_match(query) {
        format!(
            "(sample_accession={} OR secondary_sample_accession={})",
            query, query
        )
    } else if EXPERIMENT_RE.is_match(query) {
        format!("experiment_accession={}", query)
    } else if RUN_RE.is_match(query) {
        format!("run_accession={}", query)
    } else {
        log::error!(
            r"ERROR: {} is not a Study, Sample, Experiment, or Run accession.
            See https://ena-docs.readthedocs.io/en/latest/submit/general-guide/accessions.html
            for valid options",
            query
        );
        std::process::exit(1);
    }
}

pub fn check_dependencies() {
    // INFO: should check aria2c is installed, otherwise install it
    todo!()
}

pub fn check_nf() {
    todo!()
}

/// Move all `.fastq.gz` files to the root output directory
/// and deletes empty/nested .nf directories
///
/// # Arguments
/// * `outdir` - The output directory to move the files to
///
/// # Example
/// ```rust, no_run
/// use std::path::PathBuf;
/// let outdir = PathBuf::from("/path/to/output");
/// __move_to_root(&outdir);
/// ```
pub fn __move_to_root(outdir: &PathBuf) {
    for entry in WalkDir::new(outdir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_file() && e.path().extension().map_or(false, |ext| ext == "gz")
        })
    {
        let dest = outdir.join(entry.file_name());
        std::fs::rename(entry.path(), dest).expect("ERROR: Failed to move file");
    }
}

/// Clean up Nextflow directories
///
/// # Arguments
/// * `outdir` - The output directory to clean up
///
/// # Example
/// ```rust, no_run
/// use std::path::PathBuf;
///
/// let outdir = PathBuf::from("/path/to/output");
/// __clean_nf_dirs(&outdir);
/// ```
pub fn __clean_nf_dirs(outdir: &PathBuf) {
    for entry in std::fs::read_dir(outdir).expect("ERROR: Failed to read directory") {
        let entry = entry.expect("ERROR: Failed to read directory entry");
        let path = entry.path();

        if path.is_dir() {
            std::fs::remove_dir_all(&path).expect("ERROR: Failed to remove directory");
        }
    }
}

/// Concatenate all files matching the extension into one output file
///
/// # Arguments
/// * `outdir` - The output directory to move the files to
/// * `extension` - The file extension to match
/// * `file` - The output file name
///
/// # Example
/// ```rust, no_run
/// use std::path::PathBuf;
///
/// let outdir = PathBuf::from("/path/to/output");
/// __concat(&outdir, "err", "rsfq.err");
/// ```
pub fn __concat(outdir: &PathBuf, extension: &str, file: &str) {
    let out_path = outdir.join(file);
    let mut writer =
        BufWriter::new(File::create(out_path).expect("ERROR: Failed to create output file"));

    for entry in WalkDir::new(outdir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_file() && e.path().extension().map_or(false, |ext| ext == extension)
        })
    {
        let mut reader =
            BufReader::new(File::open(entry.path()).expect("ERROR: Failed to open file"));
        std::io::copy(&mut reader, &mut writer).expect("ERROR: Failed to concatenate files");
    }
}

/// Representation of a retriever
#[derive(Debug, Clone, Copy)]
pub enum Retriever {
    Wget,
    Aria2c,
    Curl,
}

impl Retriever {
    /// Materialize a URL into a file using the specified retriever.
    ///
    /// # Arguments
    /// * `url` - The URL to materialize.
    /// * `output` - The path to the output file.
    ///
    /// # Returns
    /// A `Command` instance representing the command to execute.
    ///
    /// # Examples
    /// ```rust, no_run
    /// use rsfq::utils::Retriever;
    ///
    /// let retriever = Retriever::Wget;
    /// let url = "https://example.com/file.txt";
    /// let output = PathBuf::from("/path/to/output");
    /// let command = retriever.materialize(url, &output);
    /// ```
    pub fn materialize(&self, url: &str, output: &PathBuf) -> Command {
        match self {
            Retriever::Wget => {
                let mut cmd = Command::new("wget");
                cmd.arg("--no-check-certificate")
                    .arg("-O")
                    .arg(output)
                    .arg(url);

                cmd
            }
            Retriever::Aria2c => {
                let mut cmd = Command::new("aria2c");
                cmd.arg("-x4")
                    .arg("-c")
                    .arg(format!("-o {}", output.display()))
                    .arg(format!("http://{}", url));

                cmd
            }
            Retriever::Curl => {
                let mut cmd = Command::new("curl");
                cmd.arg("-o").arg(output).arg(url);

                cmd
            }
        }
    }
}

/// Create a new `Retriever` instance from a string.
///
/// # Arguments
/// * `s` - The string to parse.
///
/// # Returns
/// A `Result` containing the `Retriever` instance or an error message.
///
/// # Examples
/// ```rust, no_run
/// use rsfq::utils::Retriever;

/// let retriever = Retriever::from_str("aria2c").unwrap();
/// ```
impl std::str::FromStr for Retriever {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "wget" => Ok(Retriever::Wget),
            "aria2c" => Ok(Retriever::Aria2c),
            "curl" => Ok(Retriever::Curl),
            _ => Err(format!("Invalid downloader: {}", s)),
        }
    }
}

/// Display the name of the `Retriever` instance.
///
/// # Examples
/// ```rust, no_run
/// use rsfq::utils::Retriever;

/// let retriever = Retriever::from_str("aria2c").unwrap();
/// println!("{}", retriever);
/// ```
impl std::fmt::Display for Retriever {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Retriever::Wget => write!(f, "wget"),
            Retriever::Aria2c => write!(f, "aria2c"),
            Retriever::Curl => write!(f, "curl"),
        }
    }
}
