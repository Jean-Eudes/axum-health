use futures::FutureExt;
use futures::future::{BoxFuture, join_all};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use super::{ComponentHealth, HealthCheck};

#[derive(Debug, Clone, Serialize)]
struct HttpDetails {
    urls: Vec<HttpCheck>,
}

pub(super) struct HttpHealthCheck {
    base: reqwest::Client,
    // Copié à chaque check : la future renvoyée est `'static` et ne peut donc pas
    // emprunter `&self`. Quelques allocations, au plus une fois par
    // `cache_ttl_seconds` puisque `get_with` sérialise l'agrégation.
    targets: Vec<String>,
    timeout: Duration,
}

impl HttpHealthCheck {
    pub(super) async fn new(config: &[crate::HttpCheckConfig], timeout_seconds: u64) -> Self {
        // L'installation doit précéder la construction du client : sinon
        // `Client::builder()` auto-installerait le provider via
        // `from_crate_features` avant notre `OnceLock`, et cet `ensure`
        // paniquerait à tort sur un conflit apparent.
        crate::ensure_rustls_ring_provider();
        let timeout = Duration::from_secs(timeout_seconds);

        Self {
            // Un seul client pour toutes les cibles : son pool de connexions et
            // son trust store sont chargés une fois, ici, au démarrage.
            base: build_client(config, timeout).await,
            targets: config.iter().map(|check| check.url.clone()).collect(),
            timeout,
        }
    }
}

async fn build_client(config: &[crate::HttpCheckConfig], timeout: Duration) -> reqwest::Client {
    let mut builder = reqwest::Client::builder().timeout(timeout);
    let mut mappings = BTreeMap::<String, Vec<SocketAddr>>::new();

    for check in config {
        let Some(resolve) = check.resolve.as_deref() else {
            continue;
        };

        // Seule la destination DNS est remplacée. L'URL garde son hostname pour
        // le Host HTTP, le SNI et la validation du certificat TLS.
        let host = url_host(&check.url);
        let port = url_port(&check.url);
        let mut addresses = if let Ok(ip) = resolve.parse::<IpAddr>() {
            vec![SocketAddr::new(ip, 0)]
        } else {
            tokio::net::lookup_host((resolve, port))
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "failed to resolve HTTP check override {resolve:?} for {:?}: {err}",
                        check.url
                    )
                })
                .map(|addr| SocketAddr::new(addr.ip(), 0))
                .collect()
        };
        addresses.sort_unstable();
        addresses.dedup();
        assert!(
            !addresses.is_empty(),
            "HTTP check override {resolve:?} for {:?} resolved to no addresses",
            check.url
        );

        if let Some(previous) = mappings.get(&host) {
            assert_eq!(
                previous, &addresses,
                "conflicting resolve overrides for HTTP hostname {host:?}"
            );
        } else {
            mappings.insert(host, addresses);
        }
    }

    for (host, addresses) in mappings {
        builder = builder.resolve_to_addrs(&host, &addresses);
    }

    builder.build().expect("failed to build HTTP client")
}

fn url_port(url: &str) -> u16 {
    reqwest::Url::parse(url)
        .unwrap_or_else(|err| panic!("invalid health check url {url:?}: {err}"))
        .port_or_known_default()
        .unwrap_or_else(|| panic!("health check url {url:?} has no known port"))
}

fn url_host(url: &str) -> String {
    let parsed = reqwest::Url::parse(url)
        .unwrap_or_else(|err| panic!("invalid health check url {url:?}: {err}"));

    parsed
        .host_str()
        .map(str::to_string)
        .unwrap_or_else(|| panic!("health check url {url:?} has no hostname"))
}

impl HealthCheck for HttpHealthCheck {
    fn check(&self) -> BoxFuture<'_, ComponentHealth> {
        // La future est empruntée à `self` (`BoxFuture<'_, _>`) : aucune donnée
        // n'est copiée ici, tout est lu par référence.
        async move {
            let results = join_all(self.targets.iter().map(|target| async move {
                let url = target.clone();
                if let Some(result) = mock_check(&url) {
                    return result;
                }

                match self.base.get(&url).send().await {
                    Err(err) if err.is_timeout() => HttpCheck {
                        url,
                        status: "DOWN",
                        http_status: None,
                        error: Some(format!(
                            "request timed out after {} seconds",
                            self.timeout.as_secs()
                        )),
                    },
                    Err(err) => HttpCheck {
                        url: url.clone(),
                        status: "DOWN",
                        http_status: None,
                        error: Some({
                            tracing::debug!(url = %url, %err, "HTTP request failed");
                            err.to_string()
                        }),
                    },
                    Ok(mut response) => {
                        let status = response.status();
                        drain(&mut response).await;
                        HttpCheck {
                            url,
                            status: if status.is_success() { "UP" } else { "DOWN" },
                            http_status: Some(status.as_u16()),
                            error: None,
                        }
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

/// Vide le corps de la réponse pour rendre la connexion au pool (keep-alive).
///
/// Sans cela, hyper abandonne la connexion à la fin de la réponse et chaque
/// check refait un handshake TCP + TLS complet par cible et par cycle.
/// `chunk()` ne bufferise qu'un chunk à la fois (contrairement à `bytes()`)
/// et n'est conditionné à aucune feature reqwest.
///
/// Le statut HTTP est déjà capturé par l'appelant : un drain interrompu (corps
/// tronqué, reset, timeout global pendant la lecture) est tracé mais ne change
/// pas le statut du check, qui mesure « la cible répond-elle en 2xx ? ».
async fn drain(response: &mut reqwest::Response) {
    if let Err(err) = drain_body(response).await {
        tracing::debug!(%err, "HTTP body drain interrupted, connection will not be reused");
    }
}

async fn drain_body(response: &mut reqwest::Response) -> Result<(), reqwest::Error> {
    while response.chunk().await?.is_some() {}
    Ok(())
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
    use super::HttpHealthCheck;
    use crate::{HttpCheckConfig, health::HealthCheck};
    use std::{
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // `HttpHealthCheck::new` installe le provider avant toute construction de
    // client, l'ordre qui évite le conflit avec `from_crate_features`.
    //
    // Hypothèse des tests réseau ci-dessous : aucun `HTTP_PROXY` dans
    // l'environnement, sinon reqwest routerait la requête vers le proxy et le
    // serveur local ne verrait rien.
    async fn check_for(config: &[HttpCheckConfig]) -> HttpHealthCheck {
        HttpHealthCheck::new(config, 5).await
    }

    //
    // Serveur HTTP de test : répond `body` en `Connection: keep-alive` et compte
    // les requêtes reçues ainsi que les connexions acceptées.
    async fn spawn_counting_server(
        body: Arc<Vec<u8>>,
    ) -> (SocketAddr, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let connections = Arc::new(AtomicUsize::new(0));
        let (requests_task, connections_task) = (Arc::clone(&requests), Arc::clone(&connections));

        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                connections_task.fetch_add(1, Ordering::SeqCst);
                let (body, requests) = (Arc::clone(&body), Arc::clone(&requests_task));

                tokio::spawn(async move {
                    let mut buffer = [0u8; 8192];

                    loop {
                        match socket.read(&mut buffer).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {
                                requests.fetch_add(1, Ordering::SeqCst);
                                let header = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                                    body.len()
                                );

                                if socket.write_all(header.as_bytes()).await.is_err()
                                    || socket.write_all(&body).await.is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });

        (addr, requests, connections)
    }

    // Serveur qui annonce un `Content-Length` puis ferme sans envoyer le corps.
    async fn spawn_truncating_server(announced_length: usize) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {announced_length}\r\nConnection: close\r\n\r\n"
                );
                let _ = socket.write_all(header.as_bytes()).await;
                // socket droppé ici : FIN avant la fin du corps annoncé
            }
        });

        addr
    }

    #[tokio::test]
    async fn check_reuses_the_same_connection_across_runs() {
        let (addr, requests, connections) =
            spawn_counting_server(Arc::new(vec![b'x'; 1024 * 1024])).await;
        let check = check_for(&[HttpCheckConfig {
            url: format!("http://{addr}/"),
            resolve: None,
        }])
        .await;

        for _ in 0..3 {
            let response = check.check().await;
            assert_eq!(response.status, "UP");
        }

        assert_eq!(requests.load(Ordering::SeqCst), 3);
        // Sans drain, hyper abandonne la connexion et chaque check en ouvre une
        // nouvelle : 3 au lieu de 1.
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn drain_tolerates_a_truncated_body() {
        let addr = spawn_truncating_server(4096).await;
        let check = check_for(&[HttpCheckConfig {
            url: format!("http://{addr}/"),
            resolve: None,
        }])
        .await;

        let response = check.check().await;

        // Le statut est mesuré sur la ligne de réponse : un corps tronqué ne
        // rétrograde pas le check en DOWN.
        assert_eq!(response.status, "UP");
        assert_eq!(response.details["urls"][0]["http_status"], 200);
    }

    // Serveur qui enregistre le `Host` de la première requête reçue.
    async fn spawn_host_recording_server() -> (SocketAddr, Arc<std::sync::Mutex<Option<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let recorded = Arc::new(std::sync::Mutex::new(None));

        let seen = Arc::clone(&recorded);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let seen = Arc::clone(&seen);

                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut chunk = [0u8; 1024];

                    // Lit jusqu'à la fin des en-têtes.
                    loop {
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => {
                                request.extend_from_slice(&chunk[..read]);
                                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                    break;
                                }
                            }
                        }
                    }

                    // hyper normalise les noms d'en-têtes en minuscules.
                    *seen.lock().unwrap() = String::from_utf8_lossy(&request)
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find(|(name, _)| name.trim().eq_ignore_ascii_case("host"))
                        .map(|(_, value)| value.trim().to_string());

                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
                        )
                        .await;
                });
            }
        });

        (addr, recorded)
    }

    #[tokio::test]
    async fn resolve_override_connects_to_the_configured_ip() {
        let (addr, requests, connections) =
            spawn_counting_server(Arc::new(vec![b'x'; 1024 * 1024])).await;
        // Hôte qui ne résout pas : seule l'override permet d'atteindre le
        // serveur local.
        let check = check_for(&[HttpCheckConfig {
            url: format!("http://internal.invalid:{}/", addr.port()),
            resolve: Some("127.0.0.1".to_string()),
        }])
        .await;

        let response = check.check().await;

        assert_eq!(response.status, "UP");
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn resolve_override_keeps_the_domain_in_the_host_header() {
        let (addr, recorded) = spawn_host_recording_server().await;
        let check = check_for(&[HttpCheckConfig {
            url: format!("http://internal.invalid:{}/", addr.port()),
            resolve: Some("localhost".to_string()),
        }])
        .await;

        let response = check.check().await;

        // L'override ne porte que sur l'adresse à contacter : le `Host` reste le
        // nom de domaine de l'URL (idem pour le SNI et le certificat en TLS).
        assert_eq!(response.status, "UP");
        assert_eq!(
            recorded.lock().unwrap().as_deref(),
            Some(format!("internal.invalid:{}", addr.port()).as_str())
        );
    }

    #[tokio::test]
    #[should_panic(expected = "conflicting resolve overrides")]
    async fn conflicting_resolve_overrides_for_same_url_hostname_are_rejected() {
        let _ = check_for(&[
            HttpCheckConfig {
                url: "http://internal.invalid/first".to_string(),
                resolve: Some("127.0.0.1".to_string()),
            },
            HttpCheckConfig {
                url: "http://internal.invalid/second".to_string(),
                resolve: Some("127.0.0.2".to_string()),
            },
        ])
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "failed to resolve HTTP check override")]
    async fn unresolved_hostname_override_fails_during_initialization() {
        let _ = check_for(&[HttpCheckConfig {
            url: "http://internal.invalid/".to_string(),
            resolve: Some("this-host-does-not-exist.invalid".to_string()),
        }])
        .await;
    }

    #[tokio::test]
    async fn check_reports_a_connection_failure_as_down() {
        // Port libre : la connexion est refusée avant toute ligne de réponse.
        let addr = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = addr.local_addr().unwrap();
        let check = check_for(&[HttpCheckConfig {
            url: format!("http://{addr}/"),
            resolve: None,
        }])
        .await;

        let response = check.check().await;

        assert_eq!(response.status, "DOWN");
        let url = &response.details["urls"][0];
        assert_eq!(url["url"], format!("http://{addr}/"));
        // `http_status` est omis quand aucune réponse n'a été reçue.
        assert!(url.get("http_status").is_none());
        assert!(!url["error"].as_str().unwrap().is_empty());
    }
}
