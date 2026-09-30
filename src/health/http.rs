use futures::FutureExt;
use futures::future::{BoxFuture, join_all};
use serde::Serialize;
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

use super::{ComponentHealth, HealthCheck};

#[derive(Debug, Clone, Serialize)]
struct HttpDetails {
    urls: Vec<HttpCheck>,
}

pub(super) struct HttpHealthCheck {
    base: reqwest::Client,
    resolve_clients: Arc<Mutex<HashMap<ResolveKey, ResolvedClient>>>,
    checks: Vec<HttpTarget>,
    timeout: Duration,
}

#[derive(Debug, Clone)]
struct HttpTarget {
    url: String,
    resolve: Option<String>,
}

/// Un client d'override est spécifique à un couple (hôte de l'URL, valeur de
/// `resolve`) : deux cibles partageant la même clé partagent le même client et
/// donc le même pool de connexions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ResolveKey {
    host: String,
    resolve: String,
}

#[derive(Debug, Clone)]
struct ResolvedClient {
    addrs: Vec<SocketAddr>,
    client: reqwest::Client,
}

impl HttpHealthCheck {
    pub(super) fn new(config: &[crate::HttpCheckConfig], timeout_seconds: u64) -> Self {
        // `ClientBuilder` n'est pas `Clone` : impossible de dériver un client
        // d'override depuis un `Client` déjà construit sans reconstruire un
        // builder, donc sans hériter des réglages du client de base. On construit
        // le client ici pour que les deux chemins (direct et `resolve`) partagent
        // exactement la même configuration.
        crate::ensure_rustls_ring_provider();
        let timeout = Duration::from_secs(timeout_seconds);
        let base = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build base HTTP client");

        Self {
            base,
            resolve_clients: Arc::new(Mutex::new(HashMap::new())),
            checks: config
                .iter()
                .map(|check| HttpTarget {
                    url: check.url.clone(),
                    resolve: check.resolve.clone(),
                })
                .collect(),
            timeout,
        }
    }
}

impl HealthCheck for HttpHealthCheck {
    fn check(&self) -> BoxFuture<'static, ComponentHealth> {
        let base = self.base.clone();
        let resolve_clients = Arc::clone(&self.resolve_clients);
        let checks = self.checks.clone();
        let timeout = self.timeout;

        async move {
            let results = join_all(checks.into_iter().map(|target| {
                let base = base.clone();
                let resolve_clients = Arc::clone(&resolve_clients);

                async move {
                    let url = target.url;
                    if let Some(result) = mock_check(&url) {
                        return result;
                    }

                    let client = match client_for_target(
                        &base,
                        &resolve_clients,
                        &url,
                        target.resolve.as_deref(),
                        timeout,
                    )
                    .await
                    {
                        Ok(client) => client,
                        Err(error) => {
                            tracing::debug!(url = %url, %error, "HTTP override client failed");
                            return HttpCheck {
                                url,
                                status: "DOWN",
                                http_status: None,
                                error: Some(error),
                            };
                        }
                    };

                    match client.get(&url).send().await {
                        Err(err) if err.is_timeout() => HttpCheck {
                            url,
                            status: "DOWN",
                            http_status: None,
                            error: Some(format!(
                                "request timed out after {} seconds",
                                timeout.as_secs()
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

/// Résout le client à utiliser pour une cible.
///
/// Sans `resolve`, c'est le client de base (un `Arc::clone`, coût nul).
/// Avec `resolve`, le client est mis en cache par couple (hôte, `resolve`) et
/// n'est reconstruit que si la résolution change : construire un client coûte
/// ~40 ms de chargement du trust store (verifier de plateforme / racines), et ce
/// chargement est de l'I/O bloquant dans une tâche async. La résolution, elle,
/// reste faite à chaque check : c'est elle qui permet le failover DNS.
async fn client_for_target(
    base: &reqwest::Client,
    resolve_clients: &Mutex<HashMap<ResolveKey, ResolvedClient>>,
    url: &str,
    resolve: Option<&str>,
    timeout: Duration,
) -> Result<reqwest::Client, String> {
    crate::ensure_rustls_ring_provider();
    let Some(resolve) = resolve else {
        return Ok(base.clone());
    };

    let parsed = reqwest::Url::parse(url).map_err(|err| err.to_string())?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "URL has no hostname".to_string())?
        .to_ascii_lowercase();
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| "URL has no known default port".to_string())?;
    let addresses = normalize_addresses(if let Ok(ip) = resolve.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        tokio::net::lookup_host((resolve, port))
            .await
            .map_err(|err| format!("failed to resolve {resolve}: {err}"))?
            .collect()
    });

    if addresses.is_empty() {
        return Err(format!("failed to resolve {resolve}: no addresses found"));
    }

    let key = ResolveKey {
        host,
        resolve: resolve.to_string(),
    };

    // Aucun await pendant le lock : std::sync::Mutex, pas tokio::sync::Mutex.
    let mut cache = resolve_clients
        .lock()
        .expect("HTTP resolve client cache is never poisoned");

    if let Some(cached) = cache.get(&key)
        && cached.addrs == addresses
    {
        return Ok(cached.client.clone());
    }

    let client = override_client(&key.host, &addresses, timeout)?;
    cache.insert(
        key,
        ResolvedClient {
            addrs: addresses,
            client: client.clone(),
        },
    );

    Ok(client)
}

fn override_client(
    host: &str,
    addresses: &[SocketAddr],
    timeout: Duration,
) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(timeout)
        .resolve_to_addrs(host, addresses)
        .build()
        .map_err(|err| err.to_string())
}

/// Tri + dédup : la comparaison avec l'entrée du cache doit être insensible à
/// l'ordre, sinon un DNS round-robin invaliderait le client à chaque check.
fn normalize_addresses(mut addresses: Vec<SocketAddr>) -> Vec<SocketAddr> {
    addresses.sort();
    addresses.dedup();
    addresses
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
    use super::{HttpHealthCheck, client_for_target};
    use crate::{HttpCheckConfig, health::HealthCheck};
    use std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // `ensure` doit précéder `Client::new()` : sans cela, `Client::new()`
    // auto-installerait le provider via `from_crate_features` avant notre
    // `OnceLock`, et l'`ensure` suivant paniquerait à tort (conflit apparent).
    #[tokio::test]
    async fn resolved_client_accepts_ip_override() {
        crate::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        assert!(
            client_for_target(
                &client,
                &resolve_clients(),
                "https://internal.example.com/health",
                Some("127.0.0.1"),
                Duration::from_secs(5),
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn resolved_client_accepts_hostname_override() {
        crate::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        assert!(
            client_for_target(
                &client,
                &resolve_clients(),
                "https://internal.example.com/health",
                Some("localhost"),
                Duration::from_secs(5),
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn resolved_client_rejects_invalid_url() {
        crate::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        assert!(
            client_for_target(
                &client,
                &resolve_clients(),
                "not a url",
                Some("127.0.0.1"),
                Duration::from_secs(5),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn resolved_client_rejects_unresolvable_hostname() {
        crate::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        assert!(
            client_for_target(
                &client,
                &resolve_clients(),
                "https://internal.example.com/health",
                Some("does-not-exist.invalid"),
                Duration::from_secs(5),
            )
            .await
            .is_err()
        );
    }

    // Hypothèse des tests réseau ci-dessous : aucun `HTTP_PROXY` dans
    // l'environnement, sinon reqwest routerait la requête vers le proxy et le
    // serveur local ne verrait rien.
    fn resolve_clients() -> Mutex<HashMap<super::ResolveKey, super::ResolvedClient>> {
        Mutex::new(HashMap::new())
    }

    fn check_for(config: &[HttpCheckConfig]) -> HttpHealthCheck {
        // `ensure` avant `Client::new()`, sinon le provider serait auto-installé
        // par `from_crate_features` et l'`ensure` suivant paniquerait.
        crate::ensure_rustls_ring_provider();
        HttpHealthCheck::new(config, 5)
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
        }]);

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
        }]);

        let response = check.check().await;

        // Le statut est mesuré sur la ligne de réponse : un corps tronqué ne
        // rétrograde pas le check en DOWN.
        assert_eq!(response.status, "UP");
        assert_eq!(response.details["urls"][0]["http_status"], 200);
    }

    #[tokio::test]
    async fn resolve_override_reaches_the_configured_address() {
        let (addr, requests, connections) =
            spawn_counting_server(Arc::new(vec![b'x'; 1024 * 1024])).await;
        // Hôte qui ne résout pas : seule l'override doit permettre d'atteindre
        // le serveur local.
        let check = check_for(&[HttpCheckConfig {
            url: format!("http://internal.invalid:{}/", addr.port()),
            resolve: Some("127.0.0.1".to_string()),
        }]);

        let response = check.check().await;

        assert_eq!(response.status, "UP");
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn override_client_is_cached_across_runs() {
        let (addr, requests, connections) =
            spawn_counting_server(Arc::new(vec![b'x'; 1024 * 1024])).await;
        let check = check_for(&[HttpCheckConfig {
            url: format!("http://internal.invalid:{}/", addr.port()),
            resolve: Some("127.0.0.1".to_string()),
        }]);

        for _ in 0..3 {
            let response = check.check().await;
            assert_eq!(response.status, "UP");
        }

        assert_eq!(requests.load(Ordering::SeqCst), 3);
        // Sans cache, chaque check reconstruit un client (donc un pool vide) et
        // ouvre une nouvelle connexion : 3 au lieu de 1.
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn addresses_are_normalized_before_cache_comparison() {
        let first = SocketAddr::from(([10, 0, 0, 1], 443));
        let second = SocketAddr::from(([10, 0, 0, 2], 443));

        // Un DNS round-robin peut renvoyer les mêmes adresses dans un ordre
        // différent, ou en répéter une : sans normalisation, le client caché
        // serait invalidé et reconstruit à chaque check (~40 ms par cible).
        assert_eq!(
            super::normalize_addresses(vec![second, first, second]),
            vec![first, second]
        );
    }
}
