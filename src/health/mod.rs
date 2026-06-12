use axum::{Json, extract::State};
use serde::Serialize;
use std::collections::BTreeMap;

use crate::AppState;

mod disk;
mod dns;
mod http;

#[derive(Debug, Serialize)]
pub(crate) struct HealthResponse {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    components: Option<BTreeMap<&'static str, ComponentHealth>>,
}

#[derive(Debug, Serialize)]
struct ComponentHealth {
    status: &'static str,
    details: ComponentDetails,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ComponentDetails {
    Http(http::HttpDetails),
    Dns(dns::DnsDetails),
    Disk(disk::DiskDetails),
}

#[derive(Debug, Serialize)]
pub(crate) struct Link {
    pub(crate) href: String,
    pub(crate) templated: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ActuatorLinksResponse {
    #[serde(rename = "_links")]
    pub(crate) links: ActuatorLinks,
}

#[derive(Debug, Serialize)]
pub(crate) struct ActuatorLinks {
    #[serde(rename = "self")]
    pub(crate) self_link: Link,
    pub(crate) health: Link,
}

pub async fn aggregate(State(state): State<AppState>) -> Json<HealthResponse> {
    let http = http::check(&state.client, &state.config.health.http.urls).await;
    let dns = dns::check(&state.config.health.dns.hosts).await;
    let disk = disk::check(&state.config.health.disk).await;
    let overall_status = if http.status == "UP" && dns.status == "UP" && disk.status == "UP" {
        "UP"
    } else {
        "DOWN"
    };

    let mut components = BTreeMap::new();
    components.insert("dns", dns);
    components.insert("disk", disk);
    components.insert("http", http);

    Json(HealthResponse {
        status: overall_status,
        components: Some(components),
    })
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
    use crate::{AppState, Config, DiskConfig, DnsConfig, HttpConfig, resources};
    use axum::{body::Body, http::StatusCode};
    use tower::ServiceExt;

    fn test_app(urls: Vec<String>, hosts: Vec<String>, disks: Vec<DiskConfig>) -> axum::Router {
        resources::app(AppState {
            client: reqwest::Client::new(),
            config: Config {
                server: crate::ServerConfig {
                    port: 3000,
                    tls: crate::TlsConfig {
                        cert_path: "certs/localhost.crt.pem".into(),
                        key_path: "certs/localhost.key.pem".into(),
                    },
                },
                health: crate::HealthConfig {
                    http: HttpConfig { urls },
                    dns: DnsConfig { hosts },
                    disk: disks,
                },
            },
        })
    }

    #[tokio::test]
    async fn health_endpoint_reports_http_statuses() {
        let ok_url = "mock://up".to_string();
        let down_url = "mock://down".to_string();
        let urls = vec![ok_url.clone(), down_url.clone()];

        let response = test_app(urls, vec![], vec![])
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
        let response = test_app(
            vec![],
            vec!["localhost".to_string(), "no-such-host.invalid".to_string()],
            vec![],
        )
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
        let response = test_app(
            vec![],
            vec![],
            vec![
                DiskConfig {
                    path: std::env::temp_dir(),
                    threshold: 0,
                },
                DiskConfig {
                    path: std::env::temp_dir(),
                    threshold: 100,
                },
            ],
        )
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
        let response = test_app(vec![], vec![], vec![])
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
        let response = test_app(vec![], vec![], vec![])
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
}
