mod health;
mod resources;

use serde::Deserialize;
use std::{
    env, fs,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
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
    server: ServerConfig,
    health: HealthConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    port: u16,
    tls: TlsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    cert_path: PathBuf,
    key_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthConfig {
    cache: HealthCacheConfig,
    http: Option<HttpConfig>,
    dns: Option<DnsConfig>,
    disk: Option<Vec<DiskConfig>>,
    plugins: Option<PluginsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginsConfig {
    timeout_seconds: u64,
    #[serde(default)]
    items: Vec<PluginConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginConfig {
    name: String,
    path: PathBuf,
    #[serde(default = "empty_toml_table")]
    config: toml::Value,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default)]
    permissions: PluginPermissions,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PluginPermissions {
    #[serde(default)]
    tcp: Vec<TcpPermission>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TcpPermission {
    host: Option<String>,
    cidr: Option<String>,
    ports: Vec<u16>,
}

fn empty_toml_table() -> toml::Value {
    toml::Value::Table(toml::map::Map::new())
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthCacheConfig {
    ttl_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    urls: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DnsConfig {
    hosts: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiskConfig {
    path: PathBuf,
    threshold: u8,
}

#[derive(Debug, Clone)]
pub struct AppState {
    client: reqwest::Client,
    config: Config,
    plugins: Arc<Vec<health::Plugin>>,
}

struct TlsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

#[tokio::main]
async fn main() {
    let config_path = env::var("AXUM_HEALTH_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
    let config = load_config(&config_path);
    let port = config.server.port;
    let tls = load_tls_acceptor(&config.server.tls);
    let state = AppState {
        client: reqwest::Client::new(),
        plugins: Arc::new(health::load_plugins(&config, &config_path)),
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

pub(crate) fn resolve_tcp_permissions(
    permissions: &[TcpPermission],
) -> Result<Vec<health::TcpPermissionRule>, String> {
    permissions
        .iter()
        .map(|permission| {
            if permission.ports.is_empty() {
                return Err("TCP permission must declare at least one port".to_string());
            }

            if permission.host.is_some() == permission.cidr.is_some() {
                return Err("TCP permission must declare exactly one of host or cidr".to_string());
            }

            let rule = if let Some(host) = &permission.host {
                let addresses = (host.as_str(), 0)
                    .to_socket_addrs()
                    .map_err(|err| format!("failed to resolve TCP permission host {host}: {err}"))?
                    .map(|address| address.ip())
                    .collect::<Vec<_>>();
                if addresses.is_empty() {
                    return Err(format!(
                        "TCP permission host resolved to no addresses: {host}"
                    ));
                }
                health::TcpPermissionRule::from_addresses(addresses, permission.ports.clone())
            } else {
                let (address, prefix) = parse_cidr(permission.cidr.as_deref().unwrap())?;
                health::TcpPermissionRule::from_cidr(address, prefix, permission.ports.clone())
            };

            Ok(rule)
        })
        .collect()
}

fn parse_cidr(value: &str) -> Result<(IpAddr, u8), String> {
    let (address, prefix) = value
        .split_once('/')
        .ok_or_else(|| format!("TCP permission CIDR must include a prefix: {value}"))?;
    let address: IpAddr = address
        .parse()
        .map_err(|err| format!("invalid TCP permission CIDR {value}: {err}"))?;
    let prefix: u8 = prefix
        .parse()
        .map_err(|err| format!("invalid TCP permission prefix {value}: {err}"))?;
    let max_prefix = match address {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    if prefix > max_prefix {
        return Err(format!("TCP permission prefix is out of range: {value}"));
    }
    Ok((address, prefix))
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
        plugins: Arc::new(vec![]),
        config: Config {
            server: ServerConfig {
                port: 3000,
                tls: TlsConfig {
                    cert_path: "certs/localhost.crt.pem".into(),
                    key_path: "certs/localhost.key.pem".into(),
                },
            },
            health: HealthConfig {
                cache: HealthCacheConfig { ttl_seconds: 5 },
                http: urls.map(|urls| HttpConfig { urls }),
                dns: hosts.map(|hosts| DnsConfig { hosts }),
                disk: disks,
                plugins: None,
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

                [health.plugins]
                timeout_seconds = 5

                [[health.plugins.items]]
                name = "postgres"
                path = "plugins/postgres-health.wasm"
                config = { database = "application" }
                timeout_seconds = 3

                [[health.plugins.items.permissions.tcp]]
                cidr = "10.0.0.0/8"
                ports = [5432]
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
        let plugins = config.health.plugins.as_ref().unwrap();
        assert_eq!(plugins.timeout_seconds, 5);
        assert_eq!(plugins.items[0].name, "postgres");
        assert_eq!(plugins.items[0].timeout_seconds, Some(3));
        assert_eq!(plugins.items[0].permissions.tcp[0].ports, vec![5432]);
    }

    #[test]
    fn tcp_permissions_allow_only_configured_cidr_and_port() {
        let rules = resolve_tcp_permissions(&[TcpPermission {
            host: None,
            cidr: Some("10.0.0.0/8".to_string()),
            ports: vec![5432],
        }])
        .unwrap();

        assert!(rules[0].allows("10.20.30.40:5432".parse().unwrap()));
        assert!(!rules[0].allows("10.20.30.40:5433".parse().unwrap()));
        assert!(!rules[0].allows("192.168.1.10:5432".parse().unwrap()));
    }
}
