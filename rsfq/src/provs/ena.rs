use reqwest::Client;
use std::collections::HashMap;

const ENA_URL: &str = "https://www.ebi.ac.uk/ena/portal/api/search?result=read_run&format=tsv";

pub enum ENAServerResponse {
    Success(Vec<HashMap<String, String>>),
    Error(u16, String),
}

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
                    "ERROR: Request failed with status {}: {}. Attempts til now {}",
                    status,
                    message,
                    attempts
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(sleep as u64)).await;
            }
        }
    }

    if result.is_empty() {
        log::error!("ERROR: No data found after {} attempts", max_attempts);
        std::process::exit(1);
    } else {
        result
    }
}

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
                    log::warn!("Query was successful, but received an empty response");
                    ENAServerResponse::Error(
                        200,
                        "Query was successful, but received an empty response".to_string(),
                    )
                } else {
                    log::info!("Successfully retrieved data from ENA!");
                    ENAServerResponse::Success(data)
                }
            } else {
                log::warn!("Query was successful, but response was empty");
                ENAServerResponse::Error(
                    200,
                    "Query was successful, but response was empty".to_string(),
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
