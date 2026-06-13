use futures::future::join_all;
use serde::Serialize;

use super::{ComponentDetails, ComponentHealth};

#[derive(Debug, Clone, Serialize)]
pub(super) struct HttpDetails {
    urls: Vec<HttpCheck>,
}

#[derive(Debug, Clone, Serialize)]
struct HttpCheck {
    url: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

pub(super) async fn check(client: &reqwest::Client, urls: &[String]) -> ComponentHealth {
    let results = join_all(urls.iter().cloned().map(|url| {
        let client = client.clone();

        async move {
            if let Some(result) = mock_check(&url) {
                return result;
            }

            match client.get(&url).send().await {
                Ok(response) if response.status().is_success() => HttpCheck {
                    url,
                    status: "UP",
                    http_status: Some(response.status().as_u16()),
                    error: None,
                },
                Ok(response) => HttpCheck {
                    url,
                    status: "DOWN",
                    http_status: Some(response.status().as_u16()),
                    error: None,
                },
                Err(err) => HttpCheck {
                    url,
                    status: "DOWN",
                    http_status: None,
                    error: Some(err.to_string()),
                },
            }
        }
    }))
    .await;

    ComponentHealth {
        status: if results.iter().all(|check| check.status == "UP") {
            "UP"
        } else {
            "DOWN"
        },
        details: ComponentDetails::Http(HttpDetails { urls: results }),
    }
}

fn mock_check(url: &str) -> Option<HttpCheck> {
    let suffix = url.strip_prefix("mock://")?;

    Some(match suffix {
        "up" => HttpCheck {
            url: url.to_string(),
            status: "UP",
            http_status: Some(200),
            error: None,
        },
        "down" => HttpCheck {
            url: url.to_string(),
            status: "DOWN",
            http_status: Some(503),
            error: None,
        },
        _ => HttpCheck {
            url: url.to_string(),
            status: "DOWN",
            http_status: None,
            error: Some(format!("unsupported mock url: {url}")),
        },
    })
}
