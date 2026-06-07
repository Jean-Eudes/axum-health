# Repository Guidelines

## Project Structure & Module Organization
- `src/main.rs` contains the full Axum application, configuration loading, health handlers, and tests.
- `Cargo.toml` defines dependencies and build metadata.
- `Cargo.lock` is committed and should be updated when dependencies change.
- `config.toml.example` is the sample runtime configuration. The server reads `config.toml` by default, or the file pointed to by `AXUM_HEALTH_CONFIG`.
- Runtime config currently has two sections: `[http]` with URL checks and `[dns]` with host resolution checks.

## Build, Test, and Development Commands
- `cargo run` starts the server on `0.0.0.0:3000`.
- `cargo test` runs the unit tests and verifies the Spring-style actuator responses plus HTTP/DNS health aggregation.
- `cargo fmt` formats the Rust code with `rustfmt`.
- `cargo clippy` runs lint checks; use it before larger changes.

## Coding Style & Naming Conventions
- Follow standard Rust formatting: 4-space indentation, `snake_case` for functions and variables, `PascalCase` for types.
- Keep handlers small and explicit. Prefer one function per endpoint or behavior.
- Preserve Spring-style JSON shapes in responses, especially `/actuator` and `/actuator/health`.
- Use descriptive config names that match the TOML sections, such as `[http]` and `[dns]`.
- Run HTTP and DNS checks in parallel per item, but keep response ordering stable and deterministic.

## Testing Guidelines
- Unit tests live in `src/main.rs` under `#[cfg(test)]`.
- Test names should describe behavior, for example `health_endpoint_reports_http_statuses`.
- Prefer request/response assertions against the router rather than testing helpers in isolation.
- When adding a new check type, include tests that cover success and failure cases, and keep the serialized Spring payload shape explicit.

## Commit & Pull Request Guidelines
- This repository has no commit history yet, so no local convention can be inferred.
- Use short, imperative commit messages, for example `Add DNS health check`.
- Pull requests should explain the behavior change, list the commands run, and mention any config updates. Include sample request/response payloads when the API shape changes.

## Security & Configuration Tips
- Do not commit production URLs or secrets in `config.toml`.
- Keep `config.toml.example` aligned with the runtime schema.
- New health checks should fail closed: if a dependency cannot be checked, report `DOWN` in the aggregated health response.
- If you add more parallel checks, keep an eye on outbound load and prefer bounded concurrency when the input list can grow large.
