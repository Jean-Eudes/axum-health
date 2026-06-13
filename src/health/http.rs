use futures::FutureExt;
use futures::future::{BoxFuture, join_all};
use serde::Serialize;

use super::{ComponentDetails, ComponentHealth, HealthCheck};

#[derive(Debug, Clone, Serialize)]
pub(super) struct HttpDetails {
    urls: Vec<HttpCheck>,
}

pub(super) struct HttpHealthCheck {
    client: reqwest::Client,
    urls: Vec<String>,
}

impl HttpHealthCheck {
    pub(super) fn new(client: &reqwest::Client, config: &crate::HttpConfig) -> Self {
        Self {
            client: client.clone(),
            urls: config.urls.clone(),
        }
    }
}

impl HealthCheck for HttpHealthCheck {
    fn name(&self) -> &'static str {
        "http"
    }

    fn check(&self) -> BoxFuture<'static, ComponentHealth> {
        let client = self.client.clone();
        let urls = self.urls.clone();

        async move {
            let results = join_all(urls.into_iter().map(|url| {
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
        .boxed()
    }
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
