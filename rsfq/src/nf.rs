use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

const NF_SCRIPT: &str = "rsfq.nf";
const NF_CONFIG: &str = "nextflow.config";
const JOBLIST: &str = "joblist.txt";

pub fn distribute(
    accessions: Vec<String>,
    executor: String,
    attempts: usize,
    outdir: Option<PathBuf>,
    threads: usize,
    queue: String,
    sleep: usize,
) {
    let joblist = accessions.join("\n");
    std::fs::write(JOBLIST, &joblist).expect("ERROR: Could not create joblist file!");

    make_script(attempts, sleep).expect("ERROR: Could not create Nextflow script!");
    make_config(executor, queue, threads).expect("ERROR: Could not create Nextflow config!");

    let outdir = outdir
        .unwrap_or_else(|| PathBuf::from("DOWNLOADS"))
        .to_str()
        .expect("ERROR: Invalid output directory!")
        .to_string();

    let mut cmd = std::process::Command::new("nextflow");
    cmd.arg("run")
        .arg(NF_SCRIPT)
        .arg("--joblist")
        .arg(JOBLIST)
        .arg("--outdir")
        .arg(outdir);

    let status = cmd.status().expect("ERROR: Failed to run Nextflow!");
    if !status.success() {
        std::process::exit(1);
    }

    std::fs::remove_file(NF_SCRIPT).expect("ERROR: Could not remove Nextflow script!");
    std::fs::remove_file(NF_CONFIG).expect("ERROR: Could not remove Nextflow config!");
    std::fs::remove_file(JOBLIST).expect("ERROR: Could not remove joblist file!");
}

fn make_script(max_attempts: usize, sleep: usize) -> io::Result<()> {
    let script = format!(
        r#"#!/usr/bin/env nextflow

process GET {{
    input:
    val(run)
    val(outdir)

    script:
    """
    cargo run --release -- -a ${{run}} --outdir ${{outdir}} --max-attempts {max_attempts} --sleep {sleep}
    find . -name "*.fa*.gz" -print0 | xargs -0 -I {{}} mv {{}} "${{outdir}}"
    """

}}

workflow {{
    joblist = Channel.fromPath(params.joblist).splitText().map{{ it.trim() }}
    outdir = params.outdir ?: "DOWNLOADS"

    GET(joblist, outdir)
}}
"#,
        max_attempts = max_attempts,
        sleep = sleep
    );

    let mut file = File::create(NF_SCRIPT)?;
    file.write_all(script.as_bytes())?;
    Ok(())
}

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
