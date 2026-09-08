use futures::FutureExt;
use futures::future::{BoxFuture, join_all};
use serde::Serialize;
use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use super::{ComponentHealth, HealthCheck};

#[derive(Debug, Clone, Serialize)]
struct HttpDetails {
    urls: Vec<HttpCheck>,
}

pub(super) struct HttpHealthCheck {
    client: reqwest::Client,
    checks: Vec<HttpTarget>,
    timeout: Duration,
}

#[derive(Debug, Clone)]
struct HttpTarget {
    url: String,
    resolve: Option<String>,
}

impl HttpHealthCheck {
    pub(super) fn new(
        client: &reqwest::Client,
        checks: &[crate::HttpCheckConfig],
        timeout_seconds: u64,
    ) -> Self {
        Self {
            client: client.clone(),
            checks: checks
                .iter()
                .map(|check| HttpTarget {
                    url: check.url.clone(),
                    resolve: check.resolve.clone(),
                })
                .collect(),
            timeout: Duration::from_secs(timeout_seconds),
        }
    }
}

impl HealthCheck for HttpHealthCheck {
    fn name(&self) -> &'static str {
        "http"
    }

    fn check(&self) -> BoxFuture<'static, ComponentHealth> {
        let client = self.client.clone();
        let checks = self.checks.clone();
        let timeout = self.timeout;

        async move {
            let results = join_all(checks.into_iter().map(|target| {
                let client = client.clone();

                async move {
                    let url = target.url;
                    if let Some(result) = mock_check(&url) {
                        return result;
                    }

                    let client =
                        match client_for_target(&client, &url, target.resolve.as_deref()).await {
                            Ok(client) => client,
                            Err(error) => {
                                return HttpCheck {
                                    url,
                                    status: "DOWN",
                                    http_status: None,
                                    error: Some(error),
                                };
                            }
                        };

                    match tokio::time::timeout(timeout, client.get(&url).send()).await {
                        Err(_) => HttpCheck {
                            url,
                            status: "DOWN",
                            http_status: None,
                            error: Some(format!(
                                "request timed out after {} seconds",
                                timeout.as_secs()
                            )),
                        },
                        Ok(Err(err)) => HttpCheck {
                            url,
                            status: "DOWN",
                            http_status: None,
                            error: Some(err.to_string()),
                        },
                        Ok(Ok(response)) if response.status().is_success() => HttpCheck {
                            url,
                            status: "UP",
                            http_status: Some(response.status().as_u16()),
                            error: None,
                        },
                        Ok(Ok(response)) => HttpCheck {
                            url,
                            status: "DOWN",
                            http_status: Some(response.status().as_u16()),
                            error: None,
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
                details: serde_json::to_value(HttpDetails { urls: results })
                    .expect("HttpDetails is always serializable"),
            }
        }
        .boxed()
    }
}

async fn client_for_target(
    client: &reqwest::Client,
    url: &str,
    resolve: Option<&str>,
) -> Result<reqwest::Client, String> {
    let Some(resolve) = resolve else {
        return Ok(client.clone());
    };

    let parsed = reqwest::Url::parse(url).map_err(|err| err.to_string())?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "URL has no hostname".to_string())?;
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| "URL has no known default port".to_string())?;
    let addresses = if let Ok(ip) = resolve.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        tokio::net::lookup_host((resolve, port))
            .await
            .map_err(|err| format!("failed to resolve {resolve}: {err}"))?
            .collect()
    };

    if addresses.is_empty() {
        return Err(format!("failed to resolve {resolve}: no addresses found"));
    }

    reqwest::Client::builder()
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(|err| err.to_string())
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

#[cfg(test)]
mod tests {
    use super::client_for_target;

    #[tokio::test]
    async fn resolved_client_accepts_ip_override() {
        let client = reqwest::Client::new();
        assert!(
            client_for_target(
                &client,
                "https://internal.example.com/health",
                Some("127.0.0.1"),
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn resolved_client_accepts_hostname_override() {
        let client = reqwest::Client::new();
        assert!(
            client_for_target(
                &client,
                "https://internal.example.com/health",
                Some("localhost")
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn resolved_client_rejects_invalid_url() {
        let client = reqwest::Client::new();
        assert!(
            client_for_target(&client, "not a url", Some("127.0.0.1"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn resolved_client_rejects_unresolvable_hostname() {
        let client = reqwest::Client::new();
        assert!(
            client_for_target(
                &client,
                "https://internal.example.com/health",
                Some("does-not-exist.invalid"),
            )
            .await
            .is_err()
        );
    }
}
