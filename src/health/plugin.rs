use super::{ComponentHealth, HealthCheck};
use futures::{FutureExt, future::BoxFuture};
use serde_json::json;
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use wasmtime::{
    Config, Engine, Store,
    component::{Component, Linker, ResourceTable},
};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView, sockets::SocketAddrUse};

wasmtime::component::bindgen!({
    world: "health-plugin",
    path: "wit",
    exports: {
        default: async,
    },
});

#[derive(Clone)]
pub(crate) struct Plugin {
    name: &'static str,
    runtime: Arc<PluginRuntime>,
}

impl std::fmt::Debug for Plugin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Plugin")
            .field("name", &self.name)
            .finish()
    }
}

struct PluginRuntime {
    engine: Engine,
    component: Component,
    config_json: String,
    timeout: Duration,
    tcp_permissions: Vec<TcpPermissionRule>,
}

#[derive(Debug, Clone)]
pub(crate) struct TcpPermissionRule {
    destinations: Vec<TcpDestination>,
    ports: Vec<u16>,
}

#[derive(Debug, Clone)]
enum TcpDestination {
    Address(IpAddr),
    Cidr(IpAddr, u8),
}

impl TcpPermissionRule {
    pub(crate) fn from_addresses(addresses: Vec<IpAddr>, ports: Vec<u16>) -> Self {
        Self {
            destinations: addresses.into_iter().map(TcpDestination::Address).collect(),
            ports,
        }
    }

    pub(crate) fn from_cidr(address: IpAddr, prefix: u8, ports: Vec<u16>) -> Self {
        Self {
            destinations: vec![TcpDestination::Cidr(address, prefix)],
            ports,
        }
    }

    pub(crate) fn allows(&self, address: SocketAddr) -> bool {
        self.ports.contains(&address.port())
            && self
                .destinations
                .iter()
                .any(|destination| match destination {
                    TcpDestination::Address(allowed) => *allowed == address.ip(),
                    TcpDestination::Cidr(network, prefix) => {
                        cidr_contains(*network, *prefix, address.ip())
                    }
                })
    }
}

fn cidr_contains(network: IpAddr, prefix: u8, address: IpAddr) -> bool {
    match (network, address) {
        (IpAddr::V4(network), IpAddr::V4(address)) => {
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            (u32::from(network) & mask) == (u32::from(address) & mask)
        }
        (IpAddr::V6(network), IpAddr::V6(address)) => {
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            (u128::from(network) & mask) == (u128::from(address) & mask)
        }
        _ => false,
    }
}

struct PluginStore {
    ctx: WasiCtx,
    table: ResourceTable,
}

impl WasiView for PluginStore {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

pub(crate) fn load_plugins(config: &crate::Config, config_path: impl AsRef<Path>) -> Vec<Plugin> {
    let Some(plugins) = &config.health.plugins else {
        return vec![];
    };

    let config_dir = config_path
        .as_ref()
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let mut names = std::collections::BTreeSet::new();
    let engine = plugin_engine();

    plugins
        .items
        .iter()
        .map(|plugin| {
            if plugin.name.is_empty() || !names.insert(plugin.name.clone()) {
                panic!("plugin names must be non-empty and unique: {}", plugin.name);
            }
            if matches!(plugin.name.as_str(), "http" | "dns" | "disk") {
                panic!(
                    "plugin name collides with built-in health check: {}",
                    plugin.name
                );
            }

            let path = resolve_plugin_path(config_dir, &plugin.path);
            let component = Component::from_file(&engine, &path).unwrap_or_else(|err| {
                panic!(
                    "failed to load health plugin {} from {}: {err}",
                    plugin.name,
                    path.display()
                )
            });
            let tcp_permissions = crate::resolve_tcp_permissions(&plugin.permissions.tcp)
                .unwrap_or_else(|err| {
                    panic!("invalid permissions for plugin {}: {err}", plugin.name)
                });
            let config_json = serde_json::to_string(&plugin.config)
                .expect("TOML plugin configuration is always serializable");

            Plugin {
                name: Box::leak(plugin.name.clone().into_boxed_str()),
                runtime: Arc::new(PluginRuntime {
                    engine: engine.clone(),
                    component,
                    config_json,
                    timeout: Duration::from_secs(
                        plugin.timeout_seconds.unwrap_or(plugins.timeout_seconds),
                    ),
                    tcp_permissions,
                }),
            }
        })
        .collect()
}

fn resolve_plugin_path(config_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(path)
    }
}

fn plugin_engine() -> Engine {
    let mut config = Config::new();
    config.wasm_component_model_async(true);
    Engine::new(&config).expect("failed to create WebAssembly engine")
}

impl HealthCheck for Plugin {
    fn name(&self) -> &'static str {
        self.name
    }

    fn check(&self) -> BoxFuture<'static, ComponentHealth> {
        let runtime = Arc::clone(&self.runtime);
        async move {
            match tokio::time::timeout(runtime.timeout, execute_plugin(runtime)).await {
                Ok(Ok(component)) => component,
                Ok(Err(error)) => down(json!({ "error": error })),
                Err(_) => down(json!({ "error": "plugin health check timed out" })),
            }
        }
        .boxed()
    }
}

async fn execute_plugin(runtime: Arc<PluginRuntime>) -> Result<ComponentHealth, String> {
    let permissions = runtime.tcp_permissions.clone();
    let mut builder = WasiCtxBuilder::new();
    builder.allow_tcp(true);
    builder.allow_udp(false);
    builder.allow_ip_name_lookup(false);
    builder.socket_addr_check(move |address, use_| {
        let allowed = matches!(use_, SocketAddrUse::TcpConnect)
            && permissions.iter().any(|rule| rule.allows(address));
        Box::pin(async move { allowed })
            as Pin<Box<dyn std::future::Future<Output = bool> + Send + Sync>>
    });

    let mut store = Store::new(
        &runtime.engine,
        PluginStore {
            ctx: builder.build(),
            table: ResourceTable::new(),
        },
    );
    let mut linker = Linker::new(&runtime.engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(|err| err.to_string())?;
    let plugin = HealthPlugin::instantiate_async(&mut store, &runtime.component, &linker)
        .await
        .map_err(|err| err.to_string())?;
    let result = plugin
        .axum_health_health_health()
        .call_check(&mut store, &runtime.config_json)
        .await
        .map_err(|err| err.to_string())?
        .map_err(|err| format!("plugin returned an error: {err}"))?;
    let details = serde_json::from_str(&result.details_json)
        .map_err(|err| format!("plugin returned invalid details JSON: {err}"))?;

    Ok(ComponentHealth {
        status: match result.status {
            exports::axum_health::health::health::Status::Up => "UP",
            exports::axum_health::health::health::Status::Down => "DOWN",
        },
        details,
    })
}

fn down(details: serde_json::Value) -> ComponentHealth {
    ComponentHealth {
        status: "DOWN",
        details,
    }
}
