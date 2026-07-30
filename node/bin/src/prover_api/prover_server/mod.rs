//! Prover server module for handling proof generation requests.
//!
//! This module provides an HTTP server that manages proof generation jobs
//! and proof storage.
mod v1;

use std::{net::SocketAddr, sync::Arc};

use crate::{
    config::ProverApiAuthConfig,
    prover_api::{
        fri_job_manager::FriJobManager, proof_storage::ProofStorage, prover_server::v1::v1_routes,
        snark_job_manager::SnarkJobManager,
    },
};

use axum::{
    Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{
        HeaderMap, StatusCode,
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose};
use reth_tasks::shutdown::GracefulShutdown;
use smart_config::value::ExposeSecret;
use tokio::net::TcpListener;

/// Application state shared across all request handlers.
#[derive(Clone)]
pub(in crate::prover_api::prover_server) struct AppState {
    fri_job_manager: Arc<FriJobManager>,
    snark_job_manager: Arc<SnarkJobManager>,
    proof_storage: ProofStorage,
}

/// Entry point for prover API server.
/// Starts an HTTP server listening on the specified bind address.
pub async fn run(
    fri_job_manager: Arc<FriJobManager>,
    snark_job_manager: Arc<SnarkJobManager>,
    proof_storage: ProofStorage,
    bind_address: String,
    auth: ProverApiAuthConfig,
    shutdown: GracefulShutdown,
) {
    let app_state = AppState {
        fri_job_manager,
        snark_job_manager,
        proof_storage,
    };

    let routes = v1_routes().route_layer(middleware::from_fn_with_state(auth, require_prover_auth));
    let app = Router::new()
        .nest("/prover-jobs/v1", routes)
        .with_state(app_state)
        // Set the request body limit to 10MiB
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024));

    let bind_address: SocketAddr = bind_address.parse().expect("failed to parse bind address");
    tracing::info!("starting proof data server on {bind_address}");

    let listener = TcpListener::bind(bind_address)
        .await
        .expect("failed to bind");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.ignore_guard())
        .await
        .expect("never errors according to doc");
}

async fn require_prover_auth(
    State(auth): State<ProverApiAuthConfig>,
    request: Request,
    next: Next,
) -> Response {
    if auth.is_enabled() && !is_authorized(&auth, request.headers()) {
        return (
            StatusCode::UNAUTHORIZED,
            [(WWW_AUTHENTICATE, "Basic realm=\"prover-api\"")],
            "unauthorized",
        )
            .into_response();
    }

    next.run(request).await
}

fn is_authorized(auth: &ProverApiAuthConfig, headers: &HeaderMap) -> bool {
    let (Some(expected_username), Some(expected_password)) =
        (auth.username.as_deref(), auth.password.as_ref())
    else {
        return true;
    };

    let Some(header) = headers.get(AUTHORIZATION) else {
        return false;
    };
    let Ok(header) = header.to_str() else {
        return false;
    };
    let Some(encoded_credentials) = header.strip_prefix("Basic ") else {
        return false;
    };
    let Ok(credentials) = general_purpose::STANDARD.decode(encoded_credentials) else {
        return false;
    };
    let Ok(credentials) = std::str::from_utf8(&credentials) else {
        return false;
    };
    let Some((username, password)) = credentials.split_once(':') else {
        return false;
    };

    username == expected_username && password == expected_password.expose_secret()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use smart_config::value::SecretString;

    fn auth_config() -> ProverApiAuthConfig {
        ProverApiAuthConfig {
            username: Some("prover".to_owned()),
            password: Some(SecretString::from("secret")),
        }
    }

    fn authorization_header(username: &str, password: &str) -> HeaderValue {
        HeaderValue::from_str(&format!(
            "Basic {}",
            general_purpose::STANDARD.encode(format!("{username}:{password}"))
        ))
        .unwrap()
    }

    #[test]
    fn prover_auth_accepts_requests_when_auth_is_not_configured() {
        let auth = ProverApiAuthConfig::default();
        let headers = HeaderMap::new();

        assert!(is_authorized(&auth, &headers));
    }

    #[test]
    fn prover_auth_rejects_requests_without_authorization_header() {
        let auth = auth_config();
        let headers = HeaderMap::new();

        assert!(!is_authorized(&auth, &headers));
    }

    #[test]
    fn prover_auth_rejects_wrong_credentials() {
        let auth = auth_config();
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, authorization_header("prover", "wrong"));

        assert!(!is_authorized(&auth, &headers));
    }

    #[test]
    fn prover_auth_accepts_matching_credentials() {
        let auth = auth_config();
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, authorization_header("prover", "secret"));

        assert!(is_authorized(&auth, &headers));
    }

    #[test]
    fn prover_auth_rejects_malformed_basic_auth() {
        let auth = auth_config();
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Basic not-base64"));

        assert!(!is_authorized(&auth, &headers));
    }
}
