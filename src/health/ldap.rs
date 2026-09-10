use futures::FutureExt;
use futures::future::{BoxFuture, join_all};
use ldap3::{LdapConnAsync, LdapConnSettings};
use serde::Serialize;
use std::time::Duration;

use super::{ComponentHealth, HealthCheck};

#[derive(Debug, Clone, Serialize)]
struct LdapDetails {
    servers: Vec<LdapCheck>,
}

pub(super) struct LdapHealthCheck {
    servers: Vec<LdapTarget>,
    timeout: Duration,
}

#[derive(Debug, Clone)]
struct LdapTarget {
    url: String,
    bind_dn: String,
    password: String,
}

impl LdapHealthCheck {
    pub(super) fn new(config: &[crate::LdapCheckConfig], timeout_seconds: u64) -> Self {
        Self {
            servers: config
                .iter()
                .map(|check| LdapTarget {
                    url: check.url.clone(),
                    bind_dn: check.bind_dn.clone(),
                    password: check.password.clone(),
                })
                .collect(),
            timeout: Duration::from_secs(timeout_seconds),
        }
    }
}

impl HealthCheck for LdapHealthCheck {
    fn check(&self) -> BoxFuture<'static, ComponentHealth> {
        let servers = self.servers.clone();
        let timeout = self.timeout;

        async move {
            let results = join_all(
                servers
                    .into_iter()
                    .map(|server| check_server(server, timeout)),
            )
            .await;

            ComponentHealth {
                status: if results.iter().all(|server| server.status == "UP") {
                    "UP"
                } else {
                    "DOWN"
                },
                details: serde_json::to_value(LdapDetails { servers: results })
                    .expect("LdapDetails is always serializable"),
            }
        }
        .boxed()
    }
}

async fn check_server(server: LdapTarget, timeout: Duration) -> LdapCheck {
    let url = server.url.clone();
    let check_url = url.clone();
    let connection_url = url.clone();
    let result = async move {
        if !check_url.starts_with("ldaps://") {
            return Err("LDAP URL must use the ldaps:// scheme".to_string());
        }

        let settings = LdapConnSettings::new().set_conn_timeout(timeout);
        let (connection, mut ldap) = LdapConnAsync::with_settings(settings, &connection_url)
            .await
            .map_err(|err| err.to_string())?;

        tokio::spawn(async move {
            if let Err(err) = connection.drive().await {
                tracing::debug!(%err, "LDAP connection driver stopped");
            }
        });

        let bind_result = ldap
            .with_timeout(timeout)
            .simple_bind(&server.bind_dn, &server.password)
            .await
            .map_err(|err| err.to_string())?
            .success()
            .map_err(|err| err.to_string());

        let unbind_result = ldap.unbind().await;
        bind_result?;
        unbind_result.map_err(|err| err.to_string())?;
        Ok::<(), String>(())
    }
    .await;

    match result {
        Ok(()) => LdapCheck {
            url,
            status: "UP",
            error: None,
        },
        Err(error) => LdapCheck {
            url,
            status: "DOWN",
            error: Some(error),
        },
    }
}

#[derive(Debug, Clone, Serialize)]
struct LdapCheck {
    url: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{LdapHealthCheck, LdapTarget, check_server};
    use std::time::Duration;

    #[tokio::test]
    async fn rejects_non_ldaps_urls() {
        let result = check_server(
            LdapTarget {
                url: "ldap://localhost:389".to_string(),
                bind_dn: "cn=healthcheck,dc=example,dc=com".to_string(),
                password: "secret".to_string(),
            },
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(result.status, "DOWN");
        assert_eq!(
            result.error.as_deref(),
            Some("LDAP URL must use the ldaps:// scheme")
        );
    }

    #[tokio::test]
    async fn reports_connection_failure_without_exposing_password() {
        let result = check_server(
            LdapTarget {
                url: "ldaps://localhost:636".to_string(),
                bind_dn: "cn=healthcheck,dc=example,dc=com".to_string(),
                password: "secret".to_string(),
            },
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(result.status, "DOWN");
        assert!(!result.error.unwrap().contains("secret"));
    }

    #[test]
    fn builds_multiple_targets() {
        let config = vec![
            crate::LdapCheckConfig {
                url: "ldaps://ldap-one.example.com:636".to_string(),
                bind_dn: "cn=healthcheck,dc=example,dc=com".to_string(),
                password: "secret-one".to_string(),
            },
            crate::LdapCheckConfig {
                url: "ldaps://ldap-two.example.com:636".to_string(),
                bind_dn: "cn=healthcheck,dc=example,dc=com".to_string(),
                password: "secret-two".to_string(),
            },
        ];

        let check = LdapHealthCheck::new(&config, 5);
        assert_eq!(check.servers.len(), 2);
        assert_eq!(check.servers[0].url, config[0].url);
        assert_eq!(check.servers[1].url, config[1].url);
    }
}
