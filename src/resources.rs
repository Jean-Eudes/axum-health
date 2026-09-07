use axum::{
    Router,
    http::{HeaderValue, header},
    response::IntoResponse,
    routing::get,
};
use moka::future::Cache;
use std::time::Duration;

use crate::{AppState, health};

pub(crate) fn app(state: AppState) -> Router {
    let health_cache = health_response_cache(state.config.health.config.cache_ttl_seconds);

    Router::new()
        .route("/actuator", get(actuator_root))
        .route(
            "/actuator/health",
            get({
                let state = state.clone();
                let health_cache = health_cache.clone();
                move || {
                    let state = state.clone();
                    let health_cache = health_cache.clone();
                    async move {
                        let response = health_cache
                            .get_with(0, async move { health::aggregate_response(state).await })
                            .await;

                        axum::Json(response)
                    }
                }
            }),
        )
        .route("/actuator/health/liveness", get(health::liveness))
        .route("/actuator/health/readiness", get(health::readiness))
}

fn health_response_cache(ttl_seconds: u64) -> Cache<u8, health::HealthResponse> {
    Cache::builder()
        .time_to_live(Duration::from_secs(ttl_seconds))
        .max_capacity(1)
        .build()
}

async fn actuator_root() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.spring-boot.actuator.v3+json"),
        )],
        axum::Json(health::ActuatorLinksResponse {
            links: health::ActuatorLinks {
                self_link: health::Link {
                    href: "/actuator".to_string(),
                    templated: false,
                },
                health: health::Link {
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
    use crate::test_app_state;
    use axum::{body::Body, http::StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn actuator_root_returns_spring_style_links() {
        let response = app(test_app_state(Some(vec![]), Some(vec![]), Some(vec![])))
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
