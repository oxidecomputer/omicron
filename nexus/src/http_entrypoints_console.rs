/*!
 * Handler functions (entrypoints) for console-related routes.
 */
use super::ServerContext;

use crate::authn::external::cookies::Cookies;
use crate::context::OpContext;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseUpdatedNoContent, Path,
    RequestContext,
};
use http::{Response, StatusCode};
use hyper::Body;
use mime_guess;
use schemars::JsonSchema;
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};

type NexusApiDescription = ApiDescription<Arc<ServerContext>>;

/**
 * Returns a description of the part of the nexus API dedicated to the web console
 */
pub fn api() -> NexusApiDescription {
    fn register_endpoints(api: &mut NexusApiDescription) -> Result<(), String> {
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
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let opctx = OpContext::for_external_api(&rqctx).await;
    if let Ok(_opctx) = opctx {
        // if they have a session, look it up by token and delete it

        // TODO: how do we get the token in order to look up the session and
        // delete it? inside the cookie auth scheme, we pull it from the cookie,
        // but we don't return it from the scheme. The result of that scheme
        // only tells us the Actor, i.e., the user ID. We can't just delete all
        // sessions for that user ID because a user could have sessions going in
        // multiple browsers and only want to log out one.
    }

    // If user's session was already expired, they fail auth and their session
    // gets automatically deleted by the auth scheme. If they have no session
    // (e.g., they cleared their cookies while sitting on the page) they will
    // also fail auth. However, even though they failed auth, we probably don't
    // want to send them back a 401 like we would for a normal request. They are
    // in fact logged out like they intended, and we want to send them the
    // response that will clear their cookie in the browser.

    // TODO: Set-Cookie with empty value and expiration date in the past so it
    // gets deleted by the browser
    Ok(HttpResponseUpdatedNoContent())
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
    let _opctx = OpContext::for_external_api(&rqctx).await;
    // redirect to IdP if not logged in

    // otherwise serve HTML page with bundle in script tag

    // HTML doesn't need to be static -- we'll probably find a reason to do some minimal
    // templating, e.g., putting a CSRF token in the page

    // amusingly, at least to start out, I don't think we care about the path
    // because the real routing is all client-side. we serve the same HTML
    // regardless, the app starts on the client and renders the right page and
    // makes the right API requests.
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/html; charset=UTF-8")
        .body("".into())?)
}

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
    // we *could* auth gate the static assets but it would be kind of weird. prob not necessary
    let path = path_params.into_inner().path;
    let file = find_file(path, PathBuf::new());

    if file.is_none() {
        // 404
    }

    let file = file.unwrap();

    // Derive the MIME type from the file name
    let content_type = mime_guess::from_path(&file) // should be the actual file
        .first()
        .map_or("text/plain".to_string(), |m| m.to_string());

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, &content_type)
        .body("".into())?)
}

// This is a bit stripped down from the dropshot example. For example, we don't
// want to tell the user why the request failed, we just 404 on everything.

// TODO: we should probably return a Result so we can at least log the problems
// internally. it would also clean up the control flow
fn find_file(path: Vec<String>, root_dir: PathBuf) -> Option<PathBuf> {
    // start from `root_dir`
    let mut current = root_dir;
    for component in &path {
        // The previous iteration needs to have resulted in a directory.
        if !current.is_dir() {
            return None;
        }
        // Dropshot won't ever give us dot-components.
        assert_ne!(component, ".");
        assert_ne!(component, "..");
        current.push(component);

        // block symlinks
        match current.symlink_metadata() {
            Ok(m) => {
                if m.file_type().is_symlink() {
                    return None;
                }
            }
            Err(_) => return None,
        }
    }
    // if we've landed on a directory, we can't serve that so it's a 404 again
    if current.is_dir() {
        return None;
    }

    Some(current)
}
