use reqwest::Client;
use std::collections::HashMap;

const ENA_URL: &str = "https://www.ebi.ac.uk/ena/portal/api/search?result=read_run&format=tsv";

pub enum ENAServerResponse {
    Success(Vec<HashMap<String, String>>),
    Error(u16, String),
}

/// Get run information from ENA.
///
/// # Arguments
///
/// * `query` - The query to search for.
/// * `max_attempts` - The maximum number of attempts to make when retrieving data.
/// * `sleep` - The number of seconds to sleep between attempts.
///
/// # Returns
///
/// A `Vec<HashMap<String, String>>` containing the run information.
///
/// # Examples
///
/// ```rust, no_run
/// use rsfq::provs::ena::get_run_info;
/// use std::collections::HashMap;
///
/// #[tokio::main]
/// async fn main() {
///     let query = "SRR123456".to_string();
///     let max_attempts = 3;
///     let sleep = 5;
///     let result = get_run_info(query, max_attempts, sleep).await;
///     println!("Run data: {:#?}", result);
/// }
/// ```
pub async fn get_run_info(
    query: String,
    max_attempts: usize,
    sleep: usize,
) -> Vec<HashMap<String, String>> {
    let mut attempts = 0;
    let mut result = vec![];
    while max_attempts >= attempts {
        let ena_data = get_ena_metadata(&query).await;
        match ena_data {
            ENAServerResponse::Success(data) => {
                log::info!("Total runs found: {}", data.len());
                result.extend(data);
                break;
            }
            ENAServerResponse::Error(status, message) => {
                attempts += 1;
                log::error!(
                    "ERROR: Request failed with status {}: {}. Attempts til now {} for query {}",
                    status,
                    message,
                    attempts,
                    query
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(sleep as u64)).await;
            }
        }
    }

    if result.is_empty() {
        log::error!(
            "ERROR: No data found after {} attempts for {}",
            max_attempts,
            query
        );
        std::process::exit(1);
    } else {
        result
    }
}

/// Get metadata from ENA.
///
/// # Arguments
///
/// * `query` - The query to search for.
///
/// # Returns
///
/// A `ENAServerResponse` containing the metadata.
///
/// # Examples
///
/// ```rust, no_run
/// use rsfq::provs::ena::{get_ena_metadata, ENAServerResponse};
///
/// #[tokio::main]
/// async fn main() {
///     let query = "SRR123456".to_string();
///     match get_ena_metadata(&query).await {
///         ENAServerResponse::Success(data) => println!("Metadata entries: {}", data.len()),
///         ENAServerResponse::Error(_, message) => println!("Failed: {}", message),
///     }
/// }
/// ```
pub async fn get_ena_metadata(query: &String) -> ENAServerResponse {
    let client = Client::new();
    let url = format!(r#"{}&query="{}"&fields=all"#, ENA_URL, query);
    log::debug!("Request URL: {}", url);

    let response = client
        .get(&url)
        .header("Content-type", "application/x-www-form-urlencoded")
        .send()
        .await;

    match response {
        Ok(resp) if resp.status().is_success() => {
            let text = resp.text().await.unwrap_or_default();
            log::debug!("Response text: {}", text);

            let mut lines = text.lines();

            if let Some(header_line) = lines.next() {
                let headers: Vec<&str> = header_line.split('\t').collect();
                let data: Vec<HashMap<String, String>> = lines
                    .filter(|line| !line.is_empty())
                    .map(|line| {
                        headers
                            .iter()
                            .zip(line.split('\t'))
                            .filter_map(|(key, value)| {
                                if value.is_empty() {
                                    None
                                } else {
                                    Some((key.to_string(), value.to_string()))
                                }
                            })
                            .collect()
                    })
                    .collect();

                if data.is_empty() {
                    log::warn!(
                        "ERROR: Query was successful, but received an empty response for query {}",
                        query
                    );
                    ENAServerResponse::Error(
                        200,
                        "ERROR: Query was successful, but received an empty response for query"
                            .to_string(),
                    )
                } else {
                    log::info!("Successfully retrieved data from ENA!");
                    ENAServerResponse::Success(data)
                }
            } else {
                log::warn!(
                    "WARN: Query was successful, but response was empty for query {}",
                    query
                );
                ENAServerResponse::Error(
                    200,
                    "ERROR: Query was successful, but response was empty".to_string(),
                )
            }
        }
        Ok(resp) => {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            log::error!("ERROR: Request failed with status {}: {}", status, text);
            ENAServerResponse::Error(status, text)
        }
        Err(err) => {
            log::error!("ERROR: Request failed: {}", err);
            ENAServerResponse::Error(500, err.to_string())
        }
    }
}
