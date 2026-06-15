mod health;
mod resources;

use serde::Deserialize;
use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::net::TcpListener;
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub health: HealthConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    pub tls: TlsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthConfig {
    pub cache: HealthCacheConfig,
    pub http: Option<HttpConfig>,
    pub dns: Option<DnsConfig>,
    pub disk: Option<Vec<DiskConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthCacheConfig {
    pub ttl_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    pub urls: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DnsConfig {
    pub hosts: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiskConfig {
    pub path: PathBuf,
    pub threshold: u8,
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub client: reqwest::Client,
    pub config: Config,
}

struct TlsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

#[tokio::main]
async fn main() {
    let config_path = env::var("AXUM_HEALTH_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
    let config = load_config(config_path);
    let port = config.server.port;
    let tls = load_tls_acceptor(&config.server.tls);
    let state = AppState {
        client: reqwest::Client::new(),
        config,
    };

    let app = resources::app(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");
    let listener = TlsListener {
        listener,
        acceptor: tls,
    };

    axum::serve(listener, app)
        .await
        .expect("server exited unexpectedly");
}

fn load_config(path: impl AsRef<Path>) -> Config {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("failed to read config file {}: {err}", path.display()));

    toml::from_str(&raw)
        .unwrap_or_else(|err| panic!("failed to parse config file {}: {err}", path.display()))
}

fn load_tls_acceptor(config: &TlsConfig) -> TlsAcceptor {
    let certs = load_certificates(&config.cert_path);
    let key = load_private_key(&config.key_path);

    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap_or_else(|err| {
            panic!(
                "failed to build TLS config from {} and {}: {err}",
                config.cert_path.display(),
                config.key_path.display()
            )
        });
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    TlsAcceptor::from(Arc::new(server_config))
}

fn load_certificates(path: impl AsRef<Path>) -> Vec<CertificateDer<'static>> {
    let path = path.as_ref();
    let raw = fs::read(path)
        .unwrap_or_else(|err| panic!("failed to read certificate file {}: {err}", path.display()));

    CertificateDer::pem_slice_iter(&raw)
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|err| panic!("failed to parse certificate file {}: {err}", path.display()))
}

fn load_private_key(path: impl AsRef<Path>) -> PrivateKeyDer<'static> {
    let path = path.as_ref();
    let file = fs::File::open(path)
        .unwrap_or_else(|err| panic!("failed to read private key file {}: {err}", path.display()));

    PrivateKeyDer::from_pem_reader(file)
        .unwrap_or_else(|err| panic!("failed to parse private key file {}: {err}", path.display()))
}

#[cfg(test)]
pub(crate) fn test_app_state(
    urls: Option<Vec<String>>,
    hosts: Option<Vec<String>>,
    disks: Option<Vec<DiskConfig>>,
) -> AppState {
    AppState {
        client: reqwest::Client::new(),
        config: Config {
            server: ServerConfig {
                port: 3000,
                tls: TlsConfig {
                    cert_path: "certs/localhost.crt.pem".into(),
                    key_path: "certs/localhost.key.pem".into(),
                },
            },
            health: HealthConfig {
                cache: HealthCacheConfig {
                    ttl_seconds: 5,
                },
                http: urls.map(|urls| HttpConfig { urls }),
                dns: hosts.map(|hosts| DnsConfig { hosts }),
                disk: disks,
            },
        },
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = match self.listener.accept().await {
                Ok(connection) => connection,
                Err(err) => {
                    eprintln!("accept error: {err}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            if let Err(err) = stream.set_nodelay(true) {
                eprintln!("failed to enable TCP_NODELAY on {addr}: {err}");
            }

            match self.acceptor.accept(stream).await {
                Ok(tls_stream) => return (tls_stream, addr),
                Err(err) => {
                    eprintln!("TLS accept error from {addr}: {err}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn loads_toml_config() {
        let config: Config = toml::from_str(
            r#"
                [server]
                port = 3000

                [server.tls]
                cert_path = "certs/localhost.crt.pem"
                key_path = "certs/localhost.key.pem"

                [health.http]
                urls = ["https://example.com", "https://example.org"]

                [health.cache]
                ttl_seconds = 10

                [health.dns]
                hosts = ["localhost", "example.com"]

                [[health.disk]]
                path = "/"
                threshold = 20

                [[health.disk]]
                path = "/tmp"
                threshold = 10
            "#,
        )
        .unwrap();

        assert_eq!(config.server.port, 3000);
        assert_eq!(
            config.server.tls.cert_path,
            PathBuf::from("certs/localhost.crt.pem")
        );
        assert_eq!(
            config.server.tls.key_path,
            PathBuf::from("certs/localhost.key.pem")
        );
        assert_eq!(config.health.http.as_ref().unwrap().urls.len(), 2);
        assert_eq!(
            config.health.http.as_ref().unwrap().urls[0],
            "https://example.com"
        );
        assert_eq!(config.health.cache.ttl_seconds, 10);
        assert_eq!(config.health.dns.as_ref().unwrap().hosts.len(), 2);
        assert_eq!(config.health.dns.as_ref().unwrap().hosts[0], "localhost");
        assert_eq!(config.health.disk.as_ref().unwrap().len(), 2);
        assert_eq!(
            config.health.disk.as_ref().unwrap()[0].path,
            PathBuf::from("/")
        );
        assert_eq!(config.health.disk.as_ref().unwrap()[0].threshold, 20);
    }
}
