use once_cell::sync::Lazy;
use regex::Regex;
use tokio::process::Command;
use walkdir::WalkDir;

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;

static PROJECT_STUDY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^PRJ[EDN][A-Z][0-9]+$|^[EDS]RP[0-9]{6,}$")
        .unwrap_or_else(|e| panic!("Failed to compile PROJECT_STUDY_RE regex: {}", e))
});
static SAMPLE_BIOSAMPLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^SAM[EDN][A-Z]?[0-9]+$|^[EDS]RS[0-9]{6,}$")
        .unwrap_or_else(|e| panic!("Failed to compile SAMPLE_BIOSAMPLE_RE regex: {}", e))
});
static EXPERIMENT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[EDS]RX[0-9]{6,}$")
        .unwrap_or_else(|e| panic!("Failed to compile EXPERIMENT_RE regex: {}", e))
});
static RUN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[EDS]RR[0-9]{6,}$")
        .unwrap_or_else(|e| panic!("Failed to compile RUN_RE regex: {}", e))
});

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
/// fn main() {
///     use rsfq::utils::validate_query;
///     let query = "PRJEB12345";
///     let formatted_query = validate_query(query);
///     assert_eq!(formatted_query, "(study_accession=PRJEB12345 OR secondary_study_accession=PRJEB12345)");
/// }
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
pub fn __move_to_root(outdir: &PathBuf) {
    for entry in WalkDir::new(outdir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_file() && e.path().extension().map_or(false, |ext| ext == "gz")
        })
    {
        let dest = outdir.join(entry.file_name());
        std::fs::rename(entry.path(), dest).unwrap_or_else(|e| {
            log::error!("ERROR: Failed to move file: {}", e);
            std::process::exit(1);
        });
    }
}

/// Clean up Nextflow directories
///
/// # Arguments
/// * `outdir` - The output directory to clean up
pub fn __clean_nf_dirs(outdir: &PathBuf) {
    for entry in std::fs::read_dir(outdir).unwrap_or_else(|e| {
        log::error!("ERROR: Failed to read directory: {}", e);
        std::process::exit(1);
    }) {
        let entry = entry.unwrap_or_else(|e| {
            log::error!("ERROR: Failed to read directory entry: {}", e);
            std::process::exit(1);
        });
        let path = entry.path();

        if path.is_dir() {
            std::fs::remove_dir_all(&path).unwrap_or_else(|e| {
                log::error!("ERROR: Failed to remove directory: {}", e);
                std::process::exit(1);
            });
        }
    }
}

/// Concatenate all files matching the extension into one output file
///
/// # Arguments
/// * `outdir` - The output directory to move the files to
/// * `extension` - The file extension to match
/// * `file` - The output file name
pub fn __concat(outdir: &PathBuf, extension: &str, file: &str) {
    let out_path = outdir.join(file);
    let mut writer = BufWriter::new(File::create(out_path).unwrap_or_else(|e| {
        log::error!("ERROR: Failed to create output file: {}", e);
        std::process::exit(1);
    }));

    for entry in WalkDir::new(outdir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_file() && e.path().extension().map_or(false, |ext| ext == extension)
        })
    {
        let mut reader = BufReader::new(File::open(entry.path()).unwrap_or_else(|e| {
            log::error!("ERROR: Failed to open file: {}", e);
            std::process::exit(1);
        }));
        std::io::copy(&mut reader, &mut writer).unwrap_or_else(|e| {
            log::error!("ERROR: Failed to concatenate files: {}", e);
            std::process::exit(1);
        });
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
    /// use std::path::PathBuf;
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
/// use std::str::FromStr;
/// let retriever = Retriever::from_str("aria2c");
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
/// use std::str::FromStr;
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

/// Enum representing the layout of FASTQ files
#[derive(Debug, Clone, Copy)]
pub enum Layout {
    Single,
    Paired,
    Global,
}

impl std::str::FromStr for Layout {
    type Err = String;

    /// Parse a string into a Layout
    ///
    /// # Arguments
    /// * `s` - The string to parse.
    ///
    /// # Returns
    /// * `Result<Self, Self::Err>` - The parsed Layout.
    ///
    /// # Examples
    /// ```rust, no_run
    /// use rsfq::utils::Layout;
    /// use std::str::FromStr;
    /// let layout = Layout::from_str("single");
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "single" => Ok(Layout::Single),
            "paired" => Ok(Layout::Paired),
            "global" => Ok(Layout::Global),
            _ => Err(format!("Invalid layout: {}", s)),
        }
    }
}

/// Display the name of the `Layout` instance.
impl std::fmt::Display for Layout {
    /// Format the `Layout` instance as a string.
    ///
    /// # Arguments
    /// * `f` - The formatter to use.
    ///
    /// # Returns
    /// * `std::fmt::Result` - The formatted string.
    ///
    /// # Examples
    /// ```rust, no_run
    /// use rsfq::utils::Layout;
    /// use std::str::FromStr;
    /// let layout = Layout::from_str("single").unwrap();
    /// println!("{}", layout);
    /// ```
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Layout::Single => write!(f, "single"),
            Layout::Paired => write!(f, "paired"),
            Layout::Global => write!(f, "global"),
        }
    }
}
