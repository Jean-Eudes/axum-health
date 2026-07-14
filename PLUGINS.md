# Health plugins

Plugins are WebAssembly components implementing the WIT world in
[`wit/health-plugin.wit`](wit/health-plugin.wit). Build plugins for WASI Preview 2 and
configure them under `health.plugins.items`.

```toml
[health.plugins]
timeout_seconds = 5

[[health.plugins.items]]
name = "postgres"
path = "plugins/postgres-health.wasm"
config = { database = "application" }

[[health.plugins.items.permissions.tcp]]
cidr = "10.0.0.0/8"
ports = [5432]
```

`path` is relative to the runtime TOML configuration. The plugin receives `config` as a
JSON string. It must return `up` or `down` plus a JSON object in `details-json`; its
component key is the configured `name`.

Each plugin gets a fresh WASI context for every check. It has no filesystem, environment,
process, standard-input/output, UDP, or DNS lookup access. TCP connections are permitted
only when their final IP address and port match a plugin rule. A `host` rule is resolved
when the service starts and authorizes the resulting IP addresses; a `cidr` rule matches
an IPv4 or IPv6 range directly.
