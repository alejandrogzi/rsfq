use once_cell::sync::Lazy;
use regex::Regex;

static PROJECT_STUDY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^PRJ[EDN][A-Z][0-9]+$|^[EDS]RP[0-9]{6,}$").unwrap());
static SAMPLE_BIOSAMPLE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^SAM[EDN][A-Z]?[0-9]+$|^[EDS]RS[0-9]{6,}$").unwrap());
static EXPERIMENT_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[EDS]RX[0-9]{6,}$").unwrap());
static RUN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[EDS]RR[0-9]{6,}$").unwrap());

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
