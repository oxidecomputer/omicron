/*!
 * Handler functions (entrypoints) for console-related routes.
 */
use super::ServerContext;

use crate::authn::external::{
    cookies::Cookies,
    session_cookie::{
        clear_session_cookie_header_value, session_cookie_header_value,
        SessionStore, SESSION_COOKIE_COOKIE_NAME,
    },
};
use crate::authn::TEST_USER_UUID_PRIVILEGED;
use crate::context::OpContext;
use dropshot::{
    endpoint, ApiDescription, HttpError, Path, RequestContext, TypedBody,
};
use http::{header, Response, StatusCode};
use hyper::Body;
use mime_guess;
use schemars::JsonSchema;
use serde::Deserialize;
use std::{env::current_dir, path::PathBuf, sync::Arc};

type NexusApiDescription = ApiDescription<Arc<ServerContext>>;

/**
 * Returns a description of the part of the nexus API dedicated to the web console
 */
pub fn api() -> NexusApiDescription {
    fn register_endpoints(api: &mut NexusApiDescription) -> Result<(), String> {
        api.register(login)?;
        api.register(logout)?;
        api.register(console_page)?;
        api.register(asset)?;
        Ok(())
    }

    let mut api = NexusApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoginParams {
    username: String,
    password: String,
}

// for now this is just for testing purposes, and the username and password are
// ignored. we will probably end up with a real username/password login
// endpoint, but I think it will only be for use while setting up the rack
#[endpoint {
     method = POST,
     path = "/login",
 }]
async fn login(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _params: TypedBody<LoginParams>,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let session = nexus
        // TODO: obviously
        .session_create(TEST_USER_UUID_PRIVILEGED.parse().unwrap())
        .await?;
    Ok(Response::builder()
        .status(200)
        .header(
            header::SET_COOKIE,
            session_cookie_header_value(
                &session.token,
                apictx.session_idle_timeout(),
            ),
        )
        .body("ok".into())
        .unwrap())
}

/**
 * Log user out of web console by deleting session.
 */
#[endpoint {
     method = POST,
     path = "/logout",
 }]
async fn logout(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    cookies: Cookies,
) -> Result<Response<Body>, HttpError> {
    let nexus = &rqctx.context().nexus;
    let opctx = OpContext::for_external_api(&rqctx).await;
    let token = cookies.get(SESSION_COOKIE_COOKIE_NAME);

    if opctx.is_ok() && token.is_some() {
        nexus.session_hard_delete(token.unwrap().value().to_string()).await?;
    }

    // If user's session was already expired, they failed auth and their session
    // was automatically deleted by the auth scheme. If they have no session
    // (e.g., they cleared their cookies while sitting on the page) they will
    // also fail auth.

    // Even if the user failed auth, we don't want to send them back a 401 like
    // we would for a normal request. They are in fact logged out like they
    // intended, and we should send the standard success response.

    Ok(Response::builder()
        .status(200)
        .header(header::SET_COOKIE, clear_session_cookie_header_value())
        .body("".into())?)
}

#[derive(Deserialize, JsonSchema)]
struct RestPathParam {
    path: Vec<String>,
}

// TODO: /c/ prefix is def not what we want long-term but it makes things easy for now
#[endpoint {
     method = GET,
     path = "/c/{path:.*}",
 }]
async fn console_page(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path_params: Path<RestPathParam>,
) -> Result<Response<Body>, HttpError> {
    let opctx = OpContext::for_external_api(&rqctx).await;

    // if authed, serve HTML page with bundle in script tag

    // HTML doesn't need to be static -- we'll probably find a reason to do some minimal
    // templating, e.g., putting a CSRF token in the page

    // amusingly, at least to start out, I don't think we care about the path
    // because the real routing is all client-side. we serve the same HTML
    // regardless, the app starts on the client and renders the right page and
    // makes the right API requests.
    if let Ok(opctx) = opctx {
        if opctx.authn.actor().is_some() {
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, "text/html; charset=UTF-8")
                .body("".into())?);
        }
    }

    // otherwise redirect to idp
    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header(http::header::LOCATION, "idp.com/login")
        .body("".into())?)
}

/// Return a static asset from the configured assets directory. 404 on virtually
/// any error for now.
#[endpoint {
     method = GET,
     path = "/assets/{path:.*}",
     // don't want this to show up in the OpenAPI spec (which we don't generate anyway)
     unpublished = true,
 }]
async fn asset(
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<RestPathParam>,
) -> Result<Response<Body>, HttpError> {
    // we *could* auth gate the static assets but it would be kind of weird.
    // prob not necessary. one way would be to have two directories, one that
    // requires auth and one that doesn't. I'd rather not

    let path = path_params.into_inner().path;

    // TODO: make path configurable, confirm existence at startup (though it can
    // always be deleted later, so maybe confirming existence isn't that
    // important)
    let mut assets_dir = current_dir().map_err(|_| {
        HttpError::for_internal_error(
            "couldn't pull current execution directory".to_string(),
        )
    })?;
    assets_dir.push("tests");
    assets_dir.push("fixtures");

    fn not_found() -> HttpError {
        HttpError::for_not_found(None, "File not found".to_string())
    }

    let file = find_file(path, assets_dir).ok_or_else(not_found)?;
    let file_contents =
        tokio::fs::read(&file).await.map_err(|_| not_found())?;

    // Derive the MIME type from the file name
    let content_type = mime_guess::from_path(&file)
        .first()
        .map_or("text/plain".to_string(), |m| m.to_string());

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, &content_type)
        .body(file_contents.into())?)
}

/// Starting from `root_dir`, follow the segments of `path` down the file tree
/// until we find a file (or not). Do not follow symlinks.
fn find_file(path: Vec<String>, root_dir: PathBuf) -> Option<PathBuf> {
    let mut current = root_dir; // start from `root_dir`
    for segment in &path {
        // If we hit a non-directory thing already and we still have segments
        // left in the path, bail. We have nowhere to go.
        if !current.is_dir() {
            return None;
        }

        current.push(segment);

        // don't follow symlinks
        if is_symlink_or_metadata_error(&current) {
            return None;
        }
    }

    // can't serve a directory
    if current.is_dir() {
        return None;
    }

    Some(current)
}

fn is_symlink_or_metadata_error(path: &PathBuf) -> bool {
    match path.symlink_metadata() {
        Ok(m) => m.file_type().is_symlink(),
        // Error means either the user doesn't have permission to pull metadata
        // or the path doesn't exist.
        Err(_) => true,
    }
}
