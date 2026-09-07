use axum::Json;
use futures::future::{BoxFuture, join_all};
use serde::Serialize;
use std::collections::BTreeMap;

use crate::{AppState, HealthConfig};

mod disk;
mod dns;
mod http;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct HealthResponse {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    components: Option<BTreeMap<&'static str, ComponentHealth>>,
}

#[derive(Debug, Clone, Serialize)]
struct ComponentHealth {
    status: &'static str,
    details: serde_json::Value,
}

trait HealthCheck: Send + Sync {
    fn name(&self) -> &'static str;
    fn check(&self) -> BoxFuture<'static, ComponentHealth>;
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Link {
    pub(crate) href: String,
    pub(crate) templated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ActuatorLinksResponse {
    #[serde(rename = "_links")]
    pub(crate) links: ActuatorLinks,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ActuatorLinks {
    #[serde(rename = "self")]
    pub(crate) self_link: Link,
    pub(crate) health: Link,
}

pub(crate) async fn aggregate_response(state: AppState) -> HealthResponse {
    let components = collect_components(&state.config.health, &state.client).await;

    let overall_status = if components
        .values()
        .all(|component| component.status == "UP")
    {
        "UP"
    } else {
        "DOWN"
    };

    HealthResponse {
        status: overall_status,
        components: Some(components),
    }
}

async fn collect_components(
    health: &HealthConfig,
    client: &reqwest::Client,
) -> BTreeMap<&'static str, ComponentHealth> {
    let checks = configured_checks(health, client);
    let components = join_all(checks.into_iter().map(|check| async move {
        let name = check.name();
        let component = check.check().await;
        (name, component)
    }))
    .await;

    components.into_iter().collect()
}

fn configured_checks(health: &HealthConfig, client: &reqwest::Client) -> Vec<Box<dyn HealthCheck>> {
    let mut checks: Vec<Box<dyn HealthCheck>> = Vec::new();

    if let Some(http) = &health.checks.http {
        checks.push(Box::new(http::HttpHealthCheck::new(
            client,
            http,
            health.config.http_timeout_seconds,
        )));
    }

    if let Some(dns) = &health.checks.dns {
        checks.push(Box::new(dns::DnsHealthCheck::new(dns)));
    }

    if let Some(disks) = &health.checks.disk {
        checks.push(Box::new(disk::DiskHealthCheck::new(disks)));
    }

    checks
}

pub async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "UP",
        components: None,
    })
}

pub async fn readiness() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "UP",
        components: None,
    })
}

#[cfg(test)]
mod tests {
    use super::{HealthResponse, configured_checks};
    use crate::{DiskConfig, resources, test_app_state};
    use axum::{body::Body, http::StatusCode};
    use moka::future::Cache;
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_endpoint_reports_http_statuses() {
        let ok_url = "mock://up".to_string();
        let down_url = "mock://down".to_string();
        let urls = vec![ok_url.clone(), down_url.clone()];

        let response = resources::app(test_app_state(Some(urls), Some(vec![]), Some(vec![])))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/actuator/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "application/json"
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        let expected = format!(
            r#"{{"status":"DOWN","components":{{"disk":{{"status":"UP","details":{{"disks":[]}}}},"dns":{{"status":"UP","details":{{"hosts":[]}}}},"http":{{"status":"DOWN","details":{{"urls":[{{"url":"{ok_url}","status":"UP","http_status":200}},{{"url":"{down_url}","status":"DOWN","http_status":503}}]}}}}}}}}"#
        );

        assert_eq!(body.as_ref(), expected.as_bytes());
    }

    #[tokio::test]
    async fn health_endpoint_reports_dns_resolution_statuses() {
        let response = resources::app(test_app_state(
            Some(vec![]),
            Some(vec![
                "localhost".to_string(),
                "no-such-host.invalid".to_string(),
            ]),
            Some(vec![]),
        ))
        .oneshot(
            axum::http::Request::builder()
                .uri("/actuator/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains(r#""dns":{"status":"DOWN""#));
        assert!(body.contains(r#""host":"localhost""#));
        assert!(body.contains(r#""host":"no-such-host.invalid""#));
    }

    #[tokio::test]
    async fn health_endpoint_reports_disk_statuses() {
        let response = resources::app(test_app_state(
            Some(vec![]),
            Some(vec![]),
            Some(vec![
                DiskConfig {
                    path: std::env::temp_dir(),
                    threshold: 0,
                },
                DiskConfig {
                    path: std::env::temp_dir(),
                    threshold: 100,
                },
            ]),
        ))
        .oneshot(
            axum::http::Request::builder()
                .uri("/actuator/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains(r#""disk":{"status":"DOWN""#));
        assert!(body.contains(r#""path":""#));
        assert!(body.contains(r#""threshold":0"#));
        assert!(body.contains(r#""threshold":100"#));
    }

    #[tokio::test]
    async fn liveness_endpoint_stays_up() {
        let response = resources::app(test_app_state(Some(vec![]), Some(vec![]), Some(vec![])))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/actuator/health/liveness")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        assert_eq!(body.as_ref(), br#"{"status":"UP"}"#);
    }

    #[tokio::test]
    async fn readiness_endpoint_stays_up() {
        let response = resources::app(test_app_state(Some(vec![]), Some(vec![]), Some(vec![])))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/actuator/health/readiness")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        assert_eq!(body.as_ref(), br#"{"status":"UP"}"#);
    }

    #[test]
    fn configured_checks_only_include_present_sections() {
        let state = test_app_state(Some(vec!["mock://up".to_string()]), None, Some(vec![]));
        let checks = configured_checks(&state.config.health, &state.client);
        let names: Vec<_> = checks.iter().map(|check| check.name()).collect();

        assert_eq!(names, vec!["http", "disk"]);
    }

    fn test_health_cache(ttl: Duration) -> Cache<u8, HealthResponse> {
        Cache::builder().time_to_live(ttl).max_capacity(1).build()
    }

    fn next_health_response(
        calls: Arc<AtomicUsize>,
    ) -> impl Future<Output = HealthResponse> + Send + 'static {
        async move {
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            HealthResponse {
                status: if call == 1 { "UP" } else { "DOWN" },
                components: None,
            }
        }
    }

    #[tokio::test]
    async fn health_cache_returns_cached_values_until_ttl_expires() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = test_health_cache(Duration::from_secs(60));

        let first = cache
            .get_with(0, next_health_response(Arc::clone(&calls)))
            .await;
        let second = cache
            .get_with(0, next_health_response(Arc::clone(&calls)))
            .await;

        assert_eq!(first.status, "UP");
        assert_eq!(second.status, "UP");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn health_cache_recomputes_after_ttl_expires() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = test_health_cache(Duration::from_millis(10));

        let first = cache
            .get_with(0, next_health_response(Arc::clone(&calls)))
            .await;
        let second = cache
            .get_with(0, next_health_response(Arc::clone(&calls)))
            .await;
        assert_eq!(first.status, "UP");
        assert_eq!(second.status, "UP");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::sleep(Duration::from_millis(20)).await;

        let third = cache
            .get_with(0, next_health_response(Arc::clone(&calls)))
            .await;
        assert_eq!(third.status, "DOWN");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
