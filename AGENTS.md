# Repository Guidelines

## Project Structure & Module Organization
- `src/main.rs` holds configuration loading, the Tokio/TLS bootstrap, and config tests.
- `src/resources.rs` defines the Axum routes (`/actuator`, `/actuator/health`, liveness, readiness) and the health-response cache.
- `src/health/` holds the aggregation (`mod.rs`, always-on `ping`) plus one module per check type: `http.rs`, `dns.rs`, `disk.rs`, `ldap.rs`.
- `Cargo.toml` defines dependencies and build metadata.
- `Cargo.lock` is committed and should be updated when dependencies change.
- `config.toml.example` is the sample runtime configuration. The server reads `config.toml` by default, or the file pointed to by `AXUM_HEALTH_CONFIG`.
- Runtime config has `[server]` (port + `[server.tls]` cert/key paths), `[health.config]` (cache TTL and per-check timeouts), and `[[health.checks.http|dns|disk|ldap]]` lists.

## Build, Test, and Development Commands
- `cargo run` starts the server (bind `0.0.0.0`, port from config, TLS mandatory via `[server.tls]`).
- `cargo test` runs the unit tests and verifies the Spring-style actuator responses plus HTTP/DNS/disk/LDAP health aggregation.
- `cargo fmt` formats the Rust code with `rustfmt`.
- `cargo clippy -- -D warnings` runs lint checks; use it before larger changes.

## Coding Style & Naming Conventions
- Follow standard Rust formatting: 4-space indentation, `snake_case` for functions and variables, `PascalCase` for types.
- Keep handlers small and explicit. Prefer one function per endpoint or behavior.
- Preserve Spring-style JSON shapes in responses, especially `/actuator` and `/actuator/health`.
- Use descriptive config names that match the TOML sections, such as `[[health.checks.http]]` and `[[health.checks.dns]]`.
- Run checks in parallel per item and per component (`join_all`), but keep response ordering stable and deterministic (`BTreeMap`).

## Testing Guidelines
- Unit tests live next to the code under `#[cfg(test)]` (`src/main.rs`, `src/resources.rs`, `src/health/{mod,http,ldap}.rs`).
- Test names should describe behavior, for example `health_endpoint_reports_http_statuses`.
- Prefer request/response assertions against the router rather than testing helpers in isolation.
- When adding a new check type, include tests that cover success and failure cases, and keep the serialized Spring payload shape explicit.

## Commit & Pull Request Guidelines
- Use one commit per fix with a short, imperative message, for example `Add DNS health check`.
- Pull requests should explain the behavior change, list the commands run, and mention any config updates. Include sample request/response payloads when the API shape changes.

## Security & Configuration Tips
- Do not commit production URLs or secrets. `config.toml` and `certs/` are git-ignored; only `config.toml.example` is versioned.
- Keep `config.toml.example` aligned with the runtime schema. It documents that `/actuator*` exposes check targets and error details, so serve it on a trusted network only.
- LDAP passwords are `secrecy::SecretString`: `Debug` is redacted, `expose_secret()` is only called at `simple_bind`. Never log the secret; full errors go to `tracing::debug`.
- New health checks should fail closed: if a dependency cannot be checked, report `DOWN` in the aggregated health response.
- LDAP unbind uses `with_timeout()` and is best-effort: a failed unbind after a successful bind stays `UP`.
- If you add more parallel checks, keep an eye on outbound load and prefer bounded concurrency when the input list can grow large.

## Test LDAP Service
- Definition: `.opencode/podman.json` (service `ldap`), entrypoint `.opencode/ldap-entry.sh`.
- Launch sequence (order matters): first run `./.opencode/ldap-host-prepare.sh` to create the host bind-mount sources (podman fails with 125 otherwise), then call `run_ldap` with `name: "ldap-test"` so `podman exec`/logs/commands stay stable across relaunches.
- LDAPS trust: the container entrypoint generates a local test CA (`Test LDAP CA`, 3-day validity) at `/tmp/opencode/ldap-test/simple-certs/ca.crt`. `ldap3` verifies LDAPS against the OS trust store via `rustls-native-certs`, which does not contain this CA, so run `./.opencode/ldap-trust-bundle.sh` after the first boot to build `/tmp/opencode/ldap-test/ca-bundle.crt` (system roots + test CA) and launch the server with `SSL_CERT_FILE=/tmp/opencode/ldap-test/ca-bundle.crt`. Re-run the script whenever the CA is regenerated (expiry or wiped certs volume). TLS verification stays enabled (fail-closed).
- Credentials: `cn=admin,dc=example,dc=org` / `admin`, suffix `dc=example,dc=org`. The entrypoint auto-seeds `dc=example,dc=org` + `ou=users` via offline `slapadd` when the DB is empty, and skips seeding when `data.mdb` already exists.
