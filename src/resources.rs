use axum::{
    Router,
    http::{HeaderValue, header},
    response::IntoResponse,
    routing::get,
};

use crate::{AppState, health};

pub(crate) fn app(state: AppState) -> Router {
    Router::new()
        .route("/actuator", get(actuator_root))
        .route("/actuator/health", get(health::aggregate))
        .route("/actuator/health/liveness", get(health::liveness))
        .route("/actuator/health/readiness", get(health::readiness))
        .with_state(state)
}

async fn actuator_root() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.spring-boot.actuator.v3+json"),
        )],
        axum::Json(crate::health::ActuatorLinksResponse {
            links: crate::health::ActuatorLinks {
                self_link: crate::health::Link {
                    href: "/actuator".to_string(),
                    templated: false,
                },
                health: crate::health::Link {
                    href: "/actuator/health".to_string(),
                    templated: false,
                },
            },
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::StatusCode};
    use tower::ServiceExt;

    fn test_app(urls: Vec<String>, hosts: Vec<String>) -> Router {
        app(AppState {
            client: reqwest::Client::new(),
            config: crate::Config {
                server: crate::ServerConfig {
                    port: 3000,
                    tls: crate::TlsConfig {
                        cert_path: "certs/localhost.crt.pem".into(),
                        key_path: "certs/localhost.key.pem".into(),
                    },
                },
                health: crate::HealthConfig {
                    http: crate::HttpConfig { urls },
                    dns: crate::DnsConfig { hosts },
                },
            },
        })
    }

    #[tokio::test]
    async fn actuator_root_returns_spring_style_links() {
        let response = test_app(vec![], vec![])
            .oneshot(
                axum::http::Request::builder()
                    .uri("/actuator")
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
            "application/vnd.spring-boot.actuator.v3+json"
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        assert_eq!(
            body.as_ref(),
            br#"{"_links":{"self":{"href":"/actuator","templated":false},"health":{"href":"/actuator/health","templated":false}}}"#
        );
    }
}
