use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use crate::{provs::Provider, utils::Retriever};

const NF_SCRIPT: &str = "rsfq.nf";
const NF_CONFIG: &str = "nextflow.config";
const JOBLIST: &str = "joblist";
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
/// use rsfq::nf::distribute;
/// use rsfq::provs::Provider;
/// use rsfq::utils::Retriever;
/// use std::path::PathBuf;
///
/// let accessions = vec!["accession1".to_string(), "accession2".to_string()];
/// let executor = "executor".to_string();
/// let attempts = 3;
/// let outdir = PathBuf::from("/path/to/output");
/// let threads = 4;
/// let queue = "queue".to_string();
/// let sleep = 5;
/// let retriever = Retriever::Aria2c;
/// let queue_size = 10;
///
/// distribute(
///     accessions,
///    executor,
///     attempts,
///     &outdir,
///     threads,
///     queue,
///     sleep,
///     retriever,
///     queue_size,
///     Provider::ENA,
/// );
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
    queue_size: usize,
    provider: Provider,
) {
    let joblist = accessions.join("\n");
    std::fs::write(JOBLIST, &joblist).unwrap_or_else(|e| {
        log::error!("ERROR: Could not create joblist file!: {}", e);
        std::process::exit(1);
    });

    let target = std::env::current_dir()
        .unwrap_or_else(|e| {
            log::error!("ERROR: could not get current_dir!: {}", e);
            std::process::exit(1);
        })
        .join(TARGET);

    make_script(target, attempts, sleep, provider).unwrap_or_else(|e| {
        log::error!("ERROR: Could not create nextflow script!: {}", e);
        std::process::exit(1);
    });
    make_config(executor.clone(), queue, threads, queue_size).unwrap_or_else(|e| {
        log::error!("ERROR: Could not create nextflow config!: {}", e);
        std::process::exit(1);
    });

    let outdir = outdir.to_str().unwrap_or_else(|| {
        log::error!("ERROR: Invalid output directory!");
        std::process::exit(1);
    });

    std::fs::create_dir_all(&outdir).unwrap_or_else(|e| {
        log::error!("ERROR: Could not create output directory!: {}", e);
        std::process::exit(1);
    });
    std::env::set_var("NXF_WORK", outdir);

    let cmd = format!(
        "nextflow run {} --joblist {} --outdir {} --retriever {} -c {} -profile {}",
        NF_SCRIPT,
        JOBLIST,
        outdir,
        retriever.to_string(),
        NF_CONFIG,
        executor
    );

    log::info!("Running Nextflow command: {}", cmd);

    let job = std::process::Command::new("bash")
        .arg("-c")
        .arg(cmd)
        .status()
        .unwrap_or_else(|e| {
            log::error!("ERROR: Failed to run nextflow!: {}", e);
            std::process::exit(1);
        });

    if !job.success() {
        std::process::exit(1);
    }

    std::fs::remove_file(NF_SCRIPT).unwrap_or_else(|e| {
        log::error!("ERROR: Could not remove Nextflow script!: {}", e);
        std::process::exit(1);
    });
    std::fs::remove_file(NF_CONFIG).unwrap_or_else(|e| {
        log::error!("ERROR: Could not remove Nextflow config!: {}", e);
        std::process::exit(1);
    });
    std::fs::remove_file(JOBLIST).unwrap_or_else(|e| {
        log::error!("ERROR: Could not remove joblist file!: {}", e);
        std::process::exit(1);
    });
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
/// use rsfq::provs::Provider;
/// use std::path::PathBuf;
///
/// let max_attempts = 3;
/// let sleep = 5;
/// let target = PathBuf::from("target/release/rsfq");
///
/// make_script(target, max_attempts, sleep, Provider::ENA);
/// ```
pub fn make_script(
    target: PathBuf,
    max_attempts: usize,
    sleep: usize,
    provider: Provider,
) -> io::Result<()> {
    let script = format!(
        r#"#!/usr/bin/env nextflow

process GET {{
    input:
    val(run)
    val(outdir)
    val(retriever)

    script:
    """
    {target} -a ${{run}} --outdir ${{outdir}} --max-attempts {max_attempts} --sleep {sleep} -T ${{retriever}} -P {provider}
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
        sleep = sleep,
        provider = provider
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
/// let executor = "slurm".to_string();
/// let queue = "normal".to_string();
/// let threads = 4;
/// let queue_size = 10;
///
/// make_config(executor, queue, threads, queue_size);
/// ```
pub fn make_config(
    executor: String,
    queue: String,
    threads: usize,
    queue_size: usize,
) -> io::Result<()> {
    let config = format!(
        r#"
    process {{
        cpus = {threads}
        time = 24.h
        memory = 2.GB
    }}

    profiles {{
        {executor} {{
            process {{
                executor = '{executor}'
                queue = '{queue}'
                cpus = {threads}
                memory = 2.GB
                time = 24.h
            }}

            executor {{
                clusterOptions = null
                queueSize = {queue_size}
                array = null
            }}
        }}
    }}
    "#,
        executor = executor,
        queue = queue,
        threads = threads
    );

    let mut file = File::create(NF_CONFIG)?;
    file.write_all(config.as_bytes())?;

    Ok(())
}
