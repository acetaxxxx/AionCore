use std::sync::Arc;

use aionui_conversation::ConversationService;

use crate::browser_session::{
    BrowserCapabilityKeyProvider, BrowserPrivateRelay, BrowserScopeAuthorizer, BrowserSessionControlPlane,
    FailClosedBrowserScopeAuthorizer,
};
use crate::service::CronService;

/// Browser dependencies are injected at the application boundary. The
/// default application state is deliberately unavailable until a configured
/// sidecar, relay and signing-key provider are ready.
#[derive(Clone)]
pub struct BrowserRouterState {
    pub control_plane: Arc<BrowserSessionControlPlane>,
    pub capability_keys: Option<Arc<BrowserCapabilityKeyProvider>>,
    /// The relay is optional at construction time so the application can
    /// remain available while browser capability is unavailable. Routes must
    /// require both this port and the readiness flag before upgrading.
    pub relay: Option<Arc<dyn BrowserPrivateRelay>>,
    pub relay_ready: bool,
    pub scope_authorizer: Arc<dyn BrowserScopeAuthorizer>,
}

impl BrowserRouterState {
    pub fn fail_closed(control_plane: Arc<BrowserSessionControlPlane>) -> Self {
        Self {
            control_plane,
            capability_keys: None,
            relay: None,
            relay_ready: false,
            scope_authorizer: Arc::new(FailClosedBrowserScopeAuthorizer),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.relay_ready && self.capability_keys.is_some() && self.relay.is_some()
    }
}

#[derive(Clone)]
pub struct CronRouterState {
    pub cron_service: Arc<CronService>,
    pub conversation_service: ConversationService,
    pub browser: BrowserRouterState,
}
