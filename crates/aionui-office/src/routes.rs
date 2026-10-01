#![allow(clippy::disallowed_types)]

use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Json, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use std::path::{Path as FsPath, PathBuf};

use aionui_api_types::{
    ApiResponse, ChatFileRef, DocumentConversionRequest, PreviewUrlResponse, RefreshPreviewRequest,
    RefreshPreviewResponse, StartPreviewRequest, StopPreviewRequest,
};
use aionui_auth::CurrentUser;
use aionui_common::ApiError;
use aionui_file::{FileError, path_safety::validate_path_with_extra_root};

use crate::error::OfficeError;
use crate::proxy::ProxyError;
use crate::state::OfficeRouterState;
use crate::types::DocType;

impl From<OfficeError> for ApiError {
    fn from(err: OfficeError) -> Self {
        match err {
            OfficeError::OfficecliNotFound => ApiError::BadRequest("officecli not found".into()),
            OfficeError::InstallFailed(_) => ApiError::Internal("officecli install failed".into()),
            OfficeError::StartFailed(msg) => ApiError::Internal(format!("preview start failed: {msg}")),
            OfficeError::PortTimeout(path) => {
                ApiError::Internal(format!("preview service readiness timeout for {path}"))
            }
            OfficeError::Io(e) => ApiError::Internal(format!("IO error: {e}")),
            OfficeError::Snapshot(msg) => ApiError::Internal(format!("snapshot error: {msg}")),
            OfficeError::Json(e) => ApiError::Internal(format!("JSON error: {e}")),
            OfficeError::Conversion(msg) => ApiError::Internal(format!("conversion error: {msg}")),
            OfficeError::ToolNotFound(tool) => ApiError::BadRequest(format!("{tool} is not installed")),
        }
    }
}

impl From<ProxyError> for ApiError {
    fn from(err: ProxyError) -> Self {
        match err {
            ProxyError::PortNotActive(_) => ApiError::Forbidden(err.to_string()),
            ProxyError::Timeout => ApiError::Timeout(err.to_string()),
            ProxyError::ConnectionFailed(msg) => ApiError::BadGateway(msg),
            ProxyError::RequestFailed(msg) => ApiError::BadGateway(msg),
        }
    }
}

pub fn office_routes(state: OfficeRouterState) -> Router {
    Router::new()
        .route("/api/word-preview/start", post(start_word_preview))
        .route("/api/word-preview/stop", post(stop_word_preview))
        .route("/api/word-preview/refresh", post(refresh_word_preview))
        .route("/api/excel-preview/start", post(start_excel_preview))
        .route("/api/excel-preview/stop", post(stop_excel_preview))
        .route("/api/excel-preview/refresh", post(refresh_excel_preview))
        .route("/api/ppt-preview/start", post(start_ppt_preview))
        .route("/api/ppt-preview/stop", post(stop_ppt_preview))
        .route("/api/ppt-preview/refresh", post(refresh_ppt_preview))
        .route("/api/document/convert", post(convert_document))
        .with_state(state)
}

pub fn office_proxy_routes(state: OfficeRouterState) -> Router {
    Router::new()
        .route("/api/ppt-proxy/{port}", get(ppt_proxy))
        .route("/api/ppt-proxy/{port}/{*path}", get(ppt_proxy))
        .route("/api/office-watch-proxy/{port}", get(office_watch_proxy))
        .route("/api/office-watch-proxy/{port}/{*path}", get(office_watch_proxy))
        .with_state(state)
}

#[derive(serde::Deserialize)]
struct ProxyPortPath {
    port: u16,
    path: Option<String>,
}

// -- Preview start/stop/refresh handlers ----------------------------------

async fn start_word_preview(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<StartPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<PreviewUrlResponse>>, ApiError> {
    start_preview(state, &user, body, DocType::Word).await
}

async fn stop_word_preview(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<StopPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    stop_preview(state, &user, body, DocType::Word).await
}

async fn refresh_word_preview(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<RefreshPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RefreshPreviewResponse>>, ApiError> {
    refresh_preview(state, &user, body, DocType::Word).await
}

async fn start_excel_preview(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<StartPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<PreviewUrlResponse>>, ApiError> {
    start_preview(state, &user, body, DocType::Excel).await
}

async fn stop_excel_preview(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<StopPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    stop_preview(state, &user, body, DocType::Excel).await
}

async fn refresh_excel_preview(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<RefreshPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RefreshPreviewResponse>>, ApiError> {
    refresh_preview(state, &user, body, DocType::Excel).await
}

async fn start_ppt_preview(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<StartPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<PreviewUrlResponse>>, ApiError> {
    start_preview(state, &user, body, DocType::Ppt).await
}

async fn stop_ppt_preview(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<StopPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    stop_preview(state, &user, body, DocType::Ppt).await
}

async fn refresh_ppt_preview(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<RefreshPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RefreshPreviewResponse>>, ApiError> {
    refresh_preview(state, &user, body, DocType::Ppt).await
}

async fn start_preview(
    state: OfficeRouterState,
    user: &CurrentUser,
    body: Result<Json<StartPreviewRequest>, JsonRejection>,
    doc_type: DocType,
) -> Result<Json<ApiResponse<PreviewUrlResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    // Prefer the ChatFileRef identity (resolved server-side, already
    // containment-checked per variant); legacy paths are resolved through the
    // authenticated caller's tenant scope below.
    let validated_path = match &req.file {
        Some(file) => {
            let upload_root = std::env::temp_dir().join("aionui");
            state
                .project
                .resolve_chat_file_ref_with_local_admin(
                    &user.id,
                    user.is_local_admin(),
                    file,
                    &upload_root,
                    aionui_project::FileOp::Read,
                )
                .await
                .map_err(office_file_resolve_error)?
        }
        None => {
            resolve_legacy_office_path(&state, user, &req.file_path, req.workspace.as_deref()).await?
        }
    };

    let result = state
        .watch_manager
        .start_for_user(&user.id, &validated_path, doc_type)
        .await;

    let resp = match result {
        Ok(port) => {
            let url = format!("/api/{}/{}", doc_type.proxy_prefix(), port);
            PreviewUrlResponse { url, error: None }
        }
        Err(e) => PreviewUrlResponse {
            url: String::new(),
            error: Some(preview_error_code(&e).to_owned()),
        },
    };

    Ok(Json(ApiResponse::ok(resp)))
}

/// How long to wait on officecli's `/api/switch`. The switch re-renders the
/// document in a child process before answering, so this is a render budget, not
/// a network one; the proxy's own timeout is far shorter.
const SWITCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// `POST /api/{word|excel|ppt}-preview/refresh` — make the running watch server
/// re-read this document from disk.
///
/// officecli does not watch the filesystem (its own help says `external edits are
/// not detected`), and nothing on our side reloads office documents either, so an
/// externally-edited office file is the one preview type that would otherwise never
/// update. Refresh has to be pushed.
///
/// Re-issuing `POST /api/switch` for the document already loaded is what does it:
/// officecli treats a same-file switch as a supported force-refresh, re-renders in
/// a child process, then broadcasts `doc-switched`, on which the embedded page
/// reloads itself. Simply reloading the webview would not work — `GET /` serves the
/// server's cached HTML, so the reload could return the same stale document and the
/// button would appear to do nothing.
///
/// Reaching officecli is the backend's job, not the client's: under web deployment
/// the frontend and officecli are on different hosts, and the existing office proxy
/// is GET-only (its `forward` takes no method or body), so it structurally cannot
/// carry this POST.
async fn refresh_preview(
    state: OfficeRouterState,
    user: &CurrentUser,
    body: Result<Json<RefreshPreviewRequest>, JsonRejection>,
    doc_type: DocType,
) -> Result<Json<ApiResponse<RefreshPreviewResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    // Resolve exactly as start/stop do, so the session key derived below is the one
    // the watch was registered under. Prefer the ChatFileRef; tenant legacy paths
    // are constrained to the caller's persisted data tree.
    let target_path = match &req.file {
        Some(file) => {
            let upload_root = std::env::temp_dir().join("aionui");
            state
                .project
                .resolve_chat_file_ref_with_local_admin(
                    &user.id,
                    user.is_local_admin(),
                    file,
                    &upload_root,
                    aionui_project::FileOp::Read,
                )
                .await
                .map_err(office_file_resolve_error)?
        }
        None => {
            resolve_legacy_office_path(&state, user, &req.file_path, req.workspace.as_deref()).await?
        }
    };

    let Some(port) = state.watch_manager.active_port_for(&user.id, &target_path, doc_type) else {
        // Nothing is serving this document, so there is nothing to refresh. Not an
        // HTTP error: the tab may simply have been closed, and the client's own
        // recovery is to start a preview rather than to surface a failure.
        return Ok(Json(ApiResponse::ok(RefreshPreviewResponse {
            ok: false,
            error: Some("OFFICECLI_NO_ACTIVE_PREVIEW".to_owned()),
        })));
    };

    let response = state
        .proxy_service
        .force_reload(port, &target_path, SWITCH_TIMEOUT)
        .await;
    Ok(Json(ApiResponse::ok(match response {
        Ok(()) => RefreshPreviewResponse { ok: true, error: None },
        Err(code) => RefreshPreviewResponse {
            ok: false,
            error: Some(code.to_owned()),
        },
    })))
}

async fn stop_preview(
    state: OfficeRouterState,
    user: &CurrentUser,
    body: Result<Json<StopPreviewRequest>, JsonRejection>,
    doc_type: DocType,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    // Resolve the same identity `start_preview` used so `stop_for_user` derives
    // the same session key (watch_manager re-canonicalizes internally). Prefer
    // the ChatFileRef; fall back to the legacy device path. Post-migration the
    // explorer office tab has only a ChatFileRef (no device path), so without
    // this branch stop can't match the watch and the officecli subprocess leaks.
    let target_path = match &req.file {
        Some(file) => {
            let upload_root = std::env::temp_dir().join("aionui");
            state
                .project
                .resolve_chat_file_ref_with_local_admin(
                    &user.id,
                    user.is_local_admin(),
                    file,
                    &upload_root,
                    aionui_project::FileOp::Read,
                )
                .await
                .map_err(office_file_resolve_error)?
        }
        // Stop only addresses a session keyed by this authenticated user. Keep
        // the local-admin legacy behavior: the watch manager canonicalizes the
        // path, and no document contents are read by this operation.
        None if user.is_local_admin() => req.file_path.clone(),
        None => resolve_legacy_office_path(&state, user, &req.file_path, None).await?,
    };
    state.watch_manager.stop_for_user(&user.id, &target_path, doc_type).await;
    Ok(Json(ApiResponse::success()))
}

// -- Document conversion --------------------------------------------------

async fn convert_document(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<DocumentConversionRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<aionui_api_types::DocumentConversionResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let validated_path =
        resolve_legacy_office_path(&state, &user, &req.file_path, req.workspace.as_deref()).await?;
    let resp = state
        .conversion_service
        .convert(&validated_path, req.to)
        .await?;
    Ok(Json(ApiResponse::ok(resp)))
}

fn validate_office_path(
    state: &OfficeRouterState,
    file_path: &str,
    workspace: Option<&str>,
) -> Result<PathBuf, ApiError> {
    let allowed_roots: Vec<&FsPath> = state.allowed_roots.iter().map(PathBuf::as_path).collect();
    validate_path_with_extra_root(file_path, &allowed_roots, workspace.map(FsPath::new))
        .map_err(file_error_to_api_error)
}

/// Resolve a legacy absolute path without letting a web caller expand its own
/// authorization root by supplying `workspace`. Local-admin callers retain the
/// historical office-root/workspace behavior; tenant callers are restricted to
/// their persisted user tree by the same canonical Local-ref resolver used by
/// file endpoints. Shared Team paths therefore remain denied until a trusted
/// membership-aware resolver is available.
async fn resolve_legacy_office_path(
    state: &OfficeRouterState,
    user: &CurrentUser,
    file_path: &str,
    workspace: Option<&str>,
) -> Result<String, ApiError> {
    if user.is_local_admin() {
        return validate_office_path(state, file_path, workspace)
            .map(|path| path.to_string_lossy().into_owned());
    }

    if let Some(workspace) = workspace {
        state
            .project
            .authorize_local_workspace_with_local_admin(&user.id, false, FsPath::new(workspace))
            .map_err(office_file_resolve_error)?;
    }

    state
        .project
        .resolve_chat_file_ref_with_local_admin(
            &user.id,
            false,
            &ChatFileRef::Local {
                path: file_path.to_owned(),
            },
            &std::env::temp_dir().join("aionui"),
            aionui_project::FileOp::Read,
        )
        .await
        .map_err(office_file_resolve_error)
}

/// Office preview targets arrive either as a client-supplied legacy path or a
/// server-resolved ChatFileRef. Keep resolution failures path-free at this
/// boundary; in particular, never serialize an absolute host path on denial.
fn office_file_resolve_error(error: aionui_project::ProjectError) -> ApiError {
    let code = error.code();
    tracing::warn!(target: "office_file", error = %error, code, "could not resolve office file");
    match error {
        aionui_project::ProjectError::Database(_) => {
            ApiError::Internal("failed to resolve office file".into())
        }
        aionui_project::ProjectError::LocalPathForbidden => {
            ApiError::Forbidden("local file access is not authorized".into())
        }
        _ => ApiError::coded(
            StatusCode::NOT_FOUND,
            "FILE_NOT_FOUND",
            "The requested file is unavailable.",
            None::<serde_json::Value>,
        ),
    }
}

fn file_error_to_api_error(error: FileError) -> ApiError {
    match error {
        FileError::BadRequest(message) => ApiError::BadRequest(message),
        FileError::Forbidden(message) => ApiError::Forbidden(message),
        FileError::PathOutsideSandbox {
            message,
            field,
            operation,
        } => ApiError::PathOutsideSandbox {
            message,
            field,
            operation,
        },
        FileError::NotFound(message) => ApiError::NotFound(message),
        FileError::Internal(message) => ApiError::Internal(message),
        // Not reachable from office path-validation; the mapping must be total.
        // Mirrors the file crate: the cause is logged at its origin, not forwarded.
        FileError::RevealFailed(_) => ApiError::coded(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "REVEAL_FAILED",
            "Could not open the system file manager.",
            None::<serde_json::Value>,
        ),
        // Not reachable from office path-validation; the mapping must be total.
        FileError::TargetNotFound => ApiError::coded(
            axum::http::StatusCode::NOT_FOUND,
            "FILE_NOT_FOUND",
            "The requested file no longer exists.",
            None::<serde_json::Value>,
        ),
    }
}

fn preview_error_code(error: &OfficeError) -> &'static str {
    match error {
        OfficeError::OfficecliNotFound => "OFFICECLI_NOT_FOUND",
        OfficeError::InstallFailed(_) => "OFFICECLI_INSTALL_FAILED",
        OfficeError::PortTimeout(_) => "OFFICECLI_PORT_TIMEOUT",
        OfficeError::StartFailed(_)
        | OfficeError::Io(_)
        | OfficeError::Snapshot(_)
        | OfficeError::Json(_)
        | OfficeError::Conversion(_)
        | OfficeError::ToolNotFound(_) => "OFFICECLI_START_FAILED",
    }
}

// -- Reverse proxy handlers -----------------------------------------------

async fn ppt_proxy(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(params): Path<ProxyPortPath>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let path = params.path.as_deref().unwrap_or("/");
    proxy_forward(state, &user.id, params.port, path, DocType::Ppt, &headers).await
}

async fn office_watch_proxy(
    State(state): State<OfficeRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(params): Path<ProxyPortPath>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let path = params.path.as_deref().unwrap_or("/");
    let request_headers: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|val| (k.as_str().to_owned(), val.to_owned())))
        .collect();

    let proxy_resp = state
        .proxy_service
        .forward_watch_for_user(&user.id, params.port, path, &request_headers)
        .await?;

    let status = StatusCode::from_u16(proxy_resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = axum::response::Response::builder().status(status);

    for (key, value) in &proxy_resp.headers {
        response = response.header(key.as_str(), value.as_str());
    }

    Ok(response
        .body(axum::body::Body::from(proxy_resp.body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()))
}

async fn proxy_forward(
    state: OfficeRouterState,
    user_id: &str,
    port: u16,
    path: &str,
    doc_type: DocType,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let request_headers: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|val| (k.as_str().to_owned(), val.to_owned())))
        .collect();

    let proxy_resp = state
        .proxy_service
        .forward_for_user(user_id, port, path, doc_type, &request_headers)
        .await?;

    let status = StatusCode::from_u16(proxy_resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = axum::response::Response::builder().status(status);

    for (key, value) in &proxy_resp.headers {
        response = response.header(key.as_str(), value.as_str());
    }

    Ok(response
        .body(axum::body::Body::from(proxy_resp.body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use aionui_api_types::{
        ConversionTarget, DocumentConversionRequest, RefreshPreviewRequest, StartPreviewRequest,
        StopPreviewRequest,
    };
    use aionui_auth::CurrentUser;
    use aionui_db::{UserStatus, UserType};
    use aionui_file::FileError;

    use crate::conversion::ConversionService;
    use crate::error::OfficeError;
    use crate::proxy::{ProxyError, ProxyService};
    use crate::state::OfficeRouterState;
    use crate::types::DocType;
    use crate::watch_manager::{OfficecliWatchManager, ProcessHandle, ProcessSpawner};

    use super::{
        ApiError, convert_document, file_error_to_api_error, office_proxy_routes, office_routes,
        refresh_preview, resolve_legacy_office_path, start_preview, stop_preview,
    };

    fn tenant_user(id: &str) -> CurrentUser {
        CurrentUser {
            id: id.to_owned(),
            username: id.to_owned(),
            user_type: UserType::Aionpro,
            status: UserStatus::Active,
        }
    }

    fn make_user_data_tree(root: &std::path::Path, user_id: &str) -> (PathBuf, PathBuf) {
        let file = root.join("users").join(user_id).join("documents").join("report.docx");
        let workspace = root
            .join("conversations")
            .join("users")
            .join(user_id)
            .join("workspace");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(&file, b"doc").unwrap();
        (file, workspace)
    }

    #[tokio::test]
    async fn office_routes_builds_without_panic() {
        let state = build_test_state().await;
        let _router = office_routes(state);
    }

    #[tokio::test]
    async fn office_proxy_routes_builds_without_panic() {
        let state = build_test_state().await;
        let _router = office_proxy_routes(state);
    }

    #[tokio::test]
    async fn legacy_office_path_allows_callers_own_persisted_file() {
        let data_root = tempfile::tempdir().unwrap();
        let (file, workspace) = make_user_data_tree(data_root.path(), "alice");
        let state = build_test_state_with_user_data_root(Some(data_root.path().to_path_buf())).await;

        let resolved = resolve_legacy_office_path(
            &state,
            &tenant_user("alice"),
            file.to_str().unwrap(),
            Some(workspace.to_str().unwrap()),
        )
        .await
        .unwrap();

        assert_eq!(
            std::path::Path::new(&resolved),
            std::fs::canonicalize(file).unwrap()
        );
    }

    #[tokio::test]
    async fn legacy_office_path_rejects_untrusted_workspace_root() {
        let data_root = tempfile::tempdir().unwrap();
        let (file, _) = make_user_data_tree(data_root.path(), "alice");
        let outside_workspace = tempfile::tempdir().unwrap();
        let state = build_test_state_with_user_data_root(Some(data_root.path().to_path_buf())).await;

        let result = resolve_legacy_office_path(
            &state,
            &tenant_user("alice"),
            file.to_str().unwrap(),
            Some(outside_workspace.path().to_str().unwrap()),
        )
        .await;

        assert!(matches!(result, Err(ApiError::Forbidden(message)) if message == "local file access is not authorized"));
    }

    #[tokio::test]
    async fn legacy_office_path_rejects_other_users_file_even_with_that_workspace() {
        let data_root = tempfile::tempdir().unwrap();
        let (file, workspace) = make_user_data_tree(data_root.path(), "bob");
        let state = build_test_state_with_user_data_root(Some(data_root.path().to_path_buf())).await;

        let result = resolve_legacy_office_path(
            &state,
            &tenant_user("alice"),
            file.to_str().unwrap(),
            Some(workspace.to_str().unwrap()),
        )
        .await;

        assert!(matches!(result, Err(ApiError::Forbidden(_))));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn legacy_office_path_rejects_symlink_escape_from_callers_tree() {
        use std::os::unix::fs::symlink;

        let data_root = tempfile::tempdir().unwrap();
        let (own_file, workspace) = make_user_data_tree(data_root.path(), "alice");
        let outside = tempfile::tempdir().unwrap();
        let secret_file = outside.path().join("outside.docx");
        std::fs::write(&secret_file, b"secret").unwrap();
        let link = own_file.parent().unwrap().join("escape.docx");
        symlink(&secret_file, &link).unwrap();
        let workspace_link = data_root.path().join("users/alice/workspace-link");
        symlink(outside.path(), &workspace_link).unwrap();
        let state = build_test_state_with_user_data_root(Some(data_root.path().to_path_buf())).await;

        let result = resolve_legacy_office_path(
            &state,
            &tenant_user("alice"),
            link.to_str().unwrap(),
            Some(workspace.to_str().unwrap()),
        )
        .await;

        assert!(matches!(result, Err(ApiError::Forbidden(_))));

        let workspace_result = resolve_legacy_office_path(
            &state,
            &tenant_user("alice"),
            own_file.to_str().unwrap(),
            Some(workspace_link.to_str().unwrap()),
        )
        .await;
        assert!(matches!(workspace_result, Err(ApiError::Forbidden(_))));
    }

    #[tokio::test]
    async fn local_admin_keeps_legacy_workspace_root_support() {
        let workspace = tempfile::tempdir().unwrap();
        let file = workspace.path().join("report.docx");
        std::fs::write(&file, b"doc").unwrap();
        let state = build_test_state().await;

        let resolved = resolve_legacy_office_path(
            &state,
            &CurrentUser::local_default(),
            file.to_str().unwrap(),
            Some(workspace.path().to_str().unwrap()),
        )
        .await
        .unwrap();

        assert_eq!(
            std::path::Path::new(&resolved),
            std::fs::canonicalize(file).unwrap()
        );
    }

    #[tokio::test]
    async fn legacy_office_handlers_reject_workspace_based_authorization() {
        let data_root = tempfile::tempdir().unwrap();
        let (file, _) = make_user_data_tree(data_root.path(), "alice");
        let outside_workspace = tempfile::tempdir().unwrap();
        let state = build_test_state_with_user_data_root(Some(data_root.path().to_path_buf())).await;
        let user = tenant_user("alice");
        let file_path = file.to_str().unwrap().to_owned();
        let workspace = outside_workspace.path().to_str().unwrap().to_owned();

        let start = start_preview(
            state.clone(),
            &user,
            Ok(axum::Json(StartPreviewRequest {
                file_path: file_path.clone(),
                workspace: Some(workspace.clone()),
                file: None,
            })),
            DocType::Word,
        )
        .await;
        assert!(matches!(start, Err(ApiError::Forbidden(_))));

        let refresh = refresh_preview(
            state.clone(),
            &user,
            Ok(axum::Json(RefreshPreviewRequest {
                file_path: file_path.clone(),
                workspace: Some(workspace.clone()),
                file: None,
            })),
            DocType::Word,
        )
        .await;
        assert!(matches!(refresh, Err(ApiError::Forbidden(_))));

        let convert = convert_document(
            axum::extract::State(state.clone()),
            axum::Extension(user.clone()),
            Ok(axum::Json(DocumentConversionRequest {
                file_path: file_path.clone(),
                to: ConversionTarget::Markdown,
                workspace: Some(workspace),
            })),
        )
        .await;
        assert!(matches!(convert, Err(ApiError::Forbidden(_))));

        let other_root = tempfile::tempdir().unwrap();
        let (other_file, _) = make_user_data_tree(other_root.path(), "bob");
        let stop = stop_preview(
            state,
            &user,
            Ok(axum::Json(StopPreviewRequest {
                file_path: other_file.to_str().unwrap().to_owned(),
                file: None,
            })),
            DocType::Word,
        )
        .await;
        assert!(matches!(stop, Err(ApiError::Forbidden(_))));
    }

    #[test]
    fn officecli_not_found_maps_to_bad_request() {
        let err = ApiError::from(OfficeError::OfficecliNotFound);
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[test]
    fn install_failed_maps_to_internal() {
        let err = ApiError::from(OfficeError::InstallFailed("installer stderr".into()));
        assert!(matches!(err, ApiError::Internal(msg) if msg == "officecli install failed"));
    }

    #[test]
    fn start_failed_maps_to_internal() {
        let err = ApiError::from(OfficeError::StartFailed("spawn error".into()));
        assert!(matches!(err, ApiError::Internal(msg) if msg.contains("spawn error")));
    }

    #[test]
    fn port_timeout_maps_to_internal() {
        let err = ApiError::from(OfficeError::PortTimeout("/a.docx".into()));
        assert!(matches!(err, ApiError::Internal(msg) if msg.contains("/a.docx")));
    }

    #[test]
    fn io_error_maps_to_internal() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = ApiError::from(OfficeError::Io(io_err));
        assert!(matches!(err, ApiError::Internal(msg) if msg.contains("file missing")));
    }

    #[test]
    fn conversion_error_maps_to_internal() {
        let err = ApiError::from(OfficeError::Conversion("bad format".into()));
        assert!(matches!(err, ApiError::Internal(msg) if msg.contains("bad format")));
    }

    #[test]
    fn file_path_outside_sandbox_maps_to_explicit_api_code() {
        let err = file_error_to_api_error(FileError::PathOutsideSandbox {
            message: "path is outside allowed roots".into(),
            field: Some("file_path"),
            operation: Some("preview"),
        });

        assert!(matches!(
            err,
            ApiError::PathOutsideSandbox {
                message,
                field: Some("file_path"),
                operation: Some("preview"),
            } if message == "path is outside allowed roots"
        ));
    }

    #[test]
    fn tool_not_found_maps_to_bad_request() {
        let err = ApiError::from(OfficeError::ToolNotFound("pandoc".into()));
        assert!(matches!(err, ApiError::BadRequest(msg) if msg.contains("pandoc")));
    }

    #[test]
    fn proxy_error_port_not_active_maps_to_forbidden() {
        let err = ApiError::from(ProxyError::PortNotActive(8080));
        assert!(matches!(err, ApiError::Forbidden(_)));
    }

    #[test]
    fn proxy_error_timeout_maps_to_timeout() {
        let err = ApiError::from(ProxyError::Timeout);
        assert!(matches!(err, ApiError::Timeout(_)));
    }

    #[test]
    fn proxy_error_connection_failed_maps_to_bad_gateway() {
        let err = ApiError::from(ProxyError::ConnectionFailed("refused".into()));
        assert!(matches!(err, ApiError::BadGateway(_)));
    }

    #[test]
    fn proxy_error_request_failed_maps_to_bad_gateway() {
        let err = ApiError::from(ProxyError::RequestFailed("network error".into()));
        assert!(matches!(err, ApiError::BadGateway(_)));
    }

    async fn build_test_state() -> OfficeRouterState {
        build_test_state_with_user_data_root(None).await
    }

    async fn build_test_state_with_user_data_root(
        user_data_root: Option<PathBuf>,
    ) -> OfficeRouterState {
        struct NoopSpawner;

        #[async_trait::async_trait]
        impl ProcessSpawner for NoopSpawner {
            async fn spawn_officecli(
                &self,
                _file_path: &str,
                _port: u16,
                _doc_type: DocType,
            ) -> Result<Box<dyn ProcessHandle>, OfficeError> {
                Err(OfficeError::OfficecliNotFound)
            }
            async fn install_officecli(&self) -> Result<(), OfficeError> {
                Err(OfficeError::InstallFailed("noop".into()))
            }
            async fn is_officecli_installed(&self) -> bool {
                false
            }
            async fn check_update(&self, _doc_type: DocType) -> Result<(), OfficeError> {
                Ok(())
            }
        }

        struct NoopBroadcaster;
        impl aionui_realtime::EventBroadcaster for NoopBroadcaster {
            fn broadcast(&self, _msg: aionui_api_types::WebSocketMessage<serde_json::Value>) {}
        }

        let spawner = Arc::new(NoopSpawner);
        let bc: Arc<dyn aionui_realtime::EventBroadcaster> = Arc::new(NoopBroadcaster);
        let wm = Arc::new(OfficecliWatchManager::new(spawner, bc));

        let conversion = Arc::new(ConversionService::new(None));
        let proxy = Arc::new(ProxyService::new(wm.clone()));

        let db = aionui_db::init_database_memory().await.unwrap();
        let store: Arc<dyn aionui_db::IProjectStore> = Arc::new(aionui_db::SqliteProjectStore::new(db.pool().clone()));
        let mut project = aionui_project::ProjectService::new(store, std::env::temp_dir());
        if let Some(root) = user_data_root {
            project = project.with_user_data_root(root);
        }

        OfficeRouterState {
            watch_manager: wm,
            conversion_service: conversion,
            proxy_service: proxy,
            allowed_roots: vec![std::env::temp_dir()],
            project: Arc::new(project),
        }
    }
}
