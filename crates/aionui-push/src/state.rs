use std::sync::Arc;

use crate::service::PushDeliveryService;

#[derive(Clone)]
pub struct PushRouterState {
    pub service: Arc<PushDeliveryService>,
    pub public_vapid_key: Option<String>,
}

impl PushRouterState {
    pub fn new(service: Arc<PushDeliveryService>, public_vapid_key: Option<String>) -> Self {
        Self {
            service,
            public_vapid_key,
        }
    }
}
