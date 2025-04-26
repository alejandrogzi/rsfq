use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use crate::utils::Retriever;

const NF_SCRIPT: &str = "rsfq.nf";
const NF_CONFIG: &str = "nextflow.config";
const JOBLIST: &str = "joblist.txt";
const TARGET: &str = "target/release/rsfq";

/// Distributes the given accessions to the specified executor.
///
/// # Arguments
///
/// * `accessions` - A vector of accessions to distribute.
/// * `executor` - The executor to use.
/// * `attempts` - The number of attempts to make.
/// * `outdir` - The output directory.
/// * `threads` - The number of threads to use.
/// * `queue` - The queue to use.
/// * `sleep` - The sleep time between attempts.
///
/// # Returns
///
/// * `()` - Nothing.
///
/// # Examples
///
/// ```rust, no_run
/// use std::path::PathBuf;
///
/// let accessions = vec!["accession1".to_string(), "accession2".to_string()];
/// let executor = "executor".to_string();
/// let attempts = 3;
/// let outdir = PathBuf::from("/path/to/output");
/// let threads = 4;
/// let queue = "queue".to_string();
/// let sleep = 5;
///
/// distribute(accessions, executor, attempts, &outdir, threads, queue, sleep);
/// ```
pub fn distribute(
    accessions: Vec<String>,
    executor: String,
    attempts: usize,
    outdir: &PathBuf,
    threads: usize,
    queue: String,
    sleep: usize,
    retriever: Retriever,
) {
    let joblist = accessions.join("\n");
    std::fs::write(JOBLIST, &joblist).expect("ERROR: Could not create joblist file!");

    let target = std::env::current_dir()
        .expect("ERROR: could not get current_dir!")
        .join(TARGET);

    make_script(target, attempts, sleep).expect("ERROR: Could not create nextflow script!");
    make_config(executor, queue, threads).expect("ERROR: Could not create nextflow config!");

    let outdir = outdir
        .to_str()
        .expect("ERROR: Invalid output directory!")
        .to_string();

    std::fs::create_dir_all(&outdir).expect("ERROR: Could not create output directory!");
    std::env::set_var("NXF_WORK", outdir.clone());

    let mut cmd = std::process::Command::new("nextflow");

    cmd.arg("run")
        .arg(
            std::env::current_dir()
                .expect("ERROR: could not get current_dir!")
                .join(NF_SCRIPT),
        )
        .arg("--joblist")
        .arg(
            std::env::current_dir()
                .expect("ERROR: could not get current_dir!")
                .join(JOBLIST),
        )
        .arg("--outdir")
        .arg(outdir)
        .arg("--retriever")
        .arg(retriever.to_string());

    let status = cmd.status().expect("ERROR: Failed to run nextflow!");
    if !status.success() {
        std::process::exit(1);
    }

    std::fs::remove_file(NF_SCRIPT).expect("ERROR: Could not remove Nextflow script!");
    std::fs::remove_file(NF_CONFIG).expect("ERROR: Could not remove Nextflow config!");
    std::fs::remove_file(JOBLIST).expect("ERROR: Could not remove joblist file!");
}

/// Write a Nextflow script to download accessions in parallel.
///
/// # Arguments
///
/// * `max_attempts` - The maximum number of attempts to download an accession.
/// * `sleep` - The number of seconds to sleep between attempts.
///
/// # Returns
///
/// * `io::Result<()>` - A result indicating success or failure.
///
/// # Examples
///
/// ```rust, no_run
/// use rsfq::nf::make_script;
///
/// let max_attempts = 3;
/// let sleep = 5;
///
/// make_script(max_attempts, sleep).expect("ERROR: Failed to create Nextflow script!");
/// ```
fn make_script(target: PathBuf, max_attempts: usize, sleep: usize) -> io::Result<()> {
    let script = format!(
        r#"#!/usr/bin/env nextflow

process GET {{
    input:
    val(run)
    val(outdir)
    val(retriever)

    script:
    """
    {target} -a ${{run}} --outdir ${{outdir}} --max-attempts {max_attempts} --sleep {sleep} -T ${{retriever}}
    """

}}

workflow {{
    joblist = Channel.fromPath(params.joblist).splitText().map{{ it.trim() }}
    outdir = params.outdir ?: "DOWNLOADS"
    retriever = params.retriever ?: "aria2c"

    GET(joblist, outdir, retriever)
}}
"#,
        target = target.display(),
        max_attempts = max_attempts,
        sleep = sleep
    );

    let mut file = File::create(NF_SCRIPT)?;
    file.write_all(script.as_bytes())?;

    Ok(())
}

/// Write a Nextflow configuration file.
///
/// # Arguments
///
/// * `executor` - The executor to use.
/// * `queue` - The queue to use.
/// * `threads` - The number of threads to use.
///
/// # Returns
///
/// * `io::Result<()>` - A result indicating success or failure.
///
/// # Examples
///
/// ```rust, no_run
/// use rsfq::nf::make_config;
///
/// let executor = "slurm";
/// let queue = "normal";
/// let threads = 4;
///
/// make_config(executor, queue, threads).expect("ERROR: Failed to create Nextflow configuration file!");
/// ```
fn make_config(executor: String, queue: String, threads: usize) -> io::Result<()> {
    let config = format!(
        r#"process {{
    executor = '{executor}'
    queue = '{queue}'
    time = 24.h
    memory = 2.GB
    queueSize = 200
    cpus = {threads}
}}"#,
        executor = executor,
        queue = queue,
        threads = threads
    );

    let mut file = File::create(NF_CONFIG)?;
    file.write_all(config.as_bytes())?;

    Ok(())
}
