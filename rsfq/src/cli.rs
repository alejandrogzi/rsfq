use clap::{ArgAction, Parser};
use std::{path::PathBuf, str::FromStr};

#[derive(Debug, Parser)]
pub struct Args {
    #[arg(
        short = 'a',
        long = "accession",
        required = true,
        value_name = "ACCESSSION",
        help = "A valid ENA or SRA accession"
    )]
    pub accession: AccessionType,

    #[arg(
        short = 'o',
        long = "outdir",
        value_name = "OUTDIR",
        // default_value = "./DOWLOADS",
        help = "Directory to write FASTQs to"
    )]
    pub outdir: Option<PathBuf>,

    #[arg(
        short = 'm',
        long = "max-attempts",
        required = false,
        value_name = "ATTEMPTS",
        default_value_t = 10,
        help = "Path to BED4 file with blacklisted introns"
    )]
    pub attempts: usize,

    #[arg(
        short = 't',
        long = "threads",
        help = "Number of threads to use for downloading",
        value_name = "THREADS",
        required = false,
        default_value_t = 4
    )]
    pub threads: usize,

    #[arg(
        short = 's',
        long = "sleep",
        required = false,
        value_name = "SLEEP",
        default_value_t = 10,
        help = "Minutes to sleep between download attempts"
    )]
    pub sleep: usize,

    #[arg(
        short = 'f',
        long = "force",
        required = false,
        value_name = "FLAG",
        default_missing_value("true"),
        default_value("false"),
        num_args(0..=1),
        require_equals(true),
        action = ArgAction::Set,
        help = "Overwrite existing files"
    )]
    pub force: bool,

    #[arg(
        short = 'g',
        long = "group-by-experiment",
        required = false,
        value_name = "FLAG",
        default_missing_value("true"),
        default_value("false"),
        num_args(0..=1),
        require_equals(true),
        action = ArgAction::Set,
        help = "Group FASTQs by experiment"
    )]
    pub group_by_experiment: bool,

    #[arg(
        short = 'G',
        long = "group-by-sample",
        required = false,
        value_name = "FLAG",
        default_missing_value("true"),
        default_value("false"),
        num_args(0..=1),
        require_equals(true),
        action = ArgAction::Set,
        help = "Group FASTQs by sample"
    )]
    pub group_by_sample: bool,

    #[arg(
        short = 'p',
        long = "prefix",
        required = false,
        value_name = "PREFIX",
        default_value = "fastq",
        help = "Prefix for FASTQ files"
    )]
    pub prefix: String,

    #[arg(
        long = "nf",
        required = false,
        value_name = "FLAG",
        default_missing_value("true"),
        default_value("false"),
        num_args(0..=1),
        require_equals(true),
        action = ArgAction::Set,
        help = "Use nextflow as a layer to interact with your system"
    )]
    pub nextflow: bool,

    #[arg(
        short = 'e',
        long = "executor",
        required = false,
        value_name = "EXECUTOR",
        default_value = "local",
        requires("nextflow"),
        help = "Nextflow executor",
        value_parser = clap::builder::PossibleValuesParser::new(
            ["slurm", "local", "sge"]
        ),
    )]
    pub executor: String,

    #[arg(
        short = 'q',
        long = "queue",
        required = false,
        value_name = "QUEUE",
        default_value = "null",
        requires("nextflow"),
        help = "HPC queue",
        value_parser = clap::builder::PossibleValuesParser::new(
            ["short", "long", "null"]
        ),
    )]
    pub queue: String,

    #[arg(
        short = 'M',
        long = "metadata",
        required = false,
        value_name = "FLAG",
        default_missing_value("true"),
        default_value("false"),
        num_args(0..=1),
        require_equals(true),
        action = ArgAction::Set,
        help = "Only get metadata for accession, do not download FASTQ files"
    )]
    pub metadata: bool,
}

impl Args {
    pub fn check(&self) {
        // INFO: if dir already exists, do not overwrite

        if let Some(outdir) = &self.outdir {
            if !outdir.exists() {
                std::fs::create_dir_all(outdir).expect("ERROR: Failed to create output directory!");
            }
        }

        if self.group_by_experiment && self.group_by_sample {
            log::error!("ERROR: Cannot group by experiment and sample at the same time!");
            std::process::exit(1);
        }

        log::info!("All arguments were parsed correctly!")
    }
}

#[derive(Debug, Clone)]
pub enum AccessionType {
    Single(String),
    List(Vec<String>),
}

impl FromStr for AccessionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let path = PathBuf::from(s);

        // INFO: assuming .txt file as input
        if let Some(ext) = path.extension() {
            if ext == "txt" {
                let content = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
                let accessions: Vec<String> = content
                    .lines()
                    .map(|line| line.trim().to_string())
                    .collect();
                return Ok(AccessionType::List(accessions));
            }
        } else {
            // INFO: assuming single string with multiple accessions
            let accessions: Vec<String> =
                s.split(',').map(|line| line.trim().to_string()).collect();

            if accessions.len() > 1 {
                return Ok(AccessionType::List(accessions));
            } else {
                return Ok(AccessionType::Single(s.to_string()));
            }
        }

        Ok(AccessionType::Single(s.to_string()))
    }
}
