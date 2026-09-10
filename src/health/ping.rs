use futures::FutureExt;
use futures::future::BoxFuture;

use super::{ComponentHealth, HealthCheck};

pub(super) struct PingHealthCheck;

impl HealthCheck for PingHealthCheck {
    fn check(&self) -> BoxFuture<'static, ComponentHealth> {
        async {
            ComponentHealth {
                status: "UP",
                details: serde_json::json!({}),
            }
        }
        .boxed()
    }
}
