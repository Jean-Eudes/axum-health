use futures::FutureExt;
use futures::future::{BoxFuture, join_all};
use serde::Serialize;

use super::{ComponentDetails, ComponentHealth, HealthCheck};

#[derive(Debug, Clone, Serialize)]
pub(super) struct DnsDetails {
    hosts: Vec<DnsCheck>,
}

pub(super) struct DnsHealthCheck {
    hosts: Vec<String>,
}

impl DnsHealthCheck {
    pub(super) fn new(config: &crate::DnsConfig) -> Self {
        Self {
            hosts: config.hosts.clone(),
        }
    }
}

impl HealthCheck for DnsHealthCheck {
    fn name(&self) -> &'static str {
        "dns"
    }

    fn check(&self) -> BoxFuture<'static, ComponentHealth> {
        let hosts = self.hosts.clone();

        async move {
            let results = join_all(hosts.into_iter().map(|host| async move {
                let host_for_lookup = host.clone();
                match tokio::net::lookup_host((host_for_lookup.as_str(), 0)).await {
                    Ok(lookup) => {
                        let addresses: Vec<String> =
                            lookup.map(|addr| addr.ip().to_string()).collect();

                        if addresses.is_empty() {
                            DnsCheck {
                                host,
                                status: "DOWN",
                                addresses: None,
                                error: Some("no addresses resolved".to_string()),
                            }
                        } else {
                            DnsCheck {
                                host,
                                status: "UP",
                                addresses: Some(addresses),
                                error: None,
                            }
                        }
                    }
                    Err(err) => DnsCheck {
                        host,
                        status: "DOWN",
                        addresses: None,
                        error: Some(err.to_string()),
                    },
                }
            }))
            .await;

            ComponentHealth {
                status: if results.iter().all(|check| check.status == "UP") {
                    "UP"
                } else {
                    "DOWN"
                },
                details: ComponentDetails::Dns(DnsDetails { hosts: results }),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Clone, Serialize)]
struct DnsCheck {
    host: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    addresses: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}
