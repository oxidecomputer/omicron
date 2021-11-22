/*!
 * Handler functions (entrypoints) for console-related routes.
 */
use crate::authn::external::{
    cookies::Cookies,
    session_cookie::{
        clear_session_cookie_header_value, session_cookie_header_value,
        SessionStore, SESSION_COOKIE_COOKIE_NAME,
    },
};
use crate::authn::TEST_USER_UUID_PRIVILEGED;
use crate::context::OpContext;
use crate::ServerContext;
use dropshot::{
    endpoint, ApiDescription, HttpError, Path, RequestContext, TypedBody,
};
use http::{header, Response, StatusCode};
use hyper::Body;
use mime_guess;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};

type NexusApiDescription = ApiDescription<Arc<ServerContext>>;

/**
 * Returns a description of the part of the nexus API dedicated to the web console
 */
pub fn console_api() -> NexusApiDescription {
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

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoginParams {
    pub username: String,
    pub password: String,
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
        .status(StatusCode::OK)
        .header(
            header::SET_COOKIE,
            session_cookie_header_value(
                &session.token,
                apictx.session_idle_timeout(),
            ),
        )
        .body("ok".into()) // TODO: what do we return from login?
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
        .status(StatusCode::NO_CONTENT)
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
            let apictx = rqctx.context();
            let file =
                &apictx.assets_directory.join(PathBuf::from("index.html"));
            let file_contents =
                tokio::fs::read(&file).await.map_err(|_| not_found("EBADF"))?;
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, "text/html; charset=UTF-8")
                .body(file_contents.into())?);
        }
    }

    // otherwise redirect to idp
    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header(http::header::LOCATION, "https://idp.com/login")
        .body("".into())?)
}

/// Fetch a static asset from the configured assets directory. 404 on virtually
/// all errors. No auth. NO SENSITIVE FILES.
#[endpoint {
     method = GET,
     path = "/assets/{path:.*}",
     // don't want this to show up in the OpenAPI spec (which we don't generate anyway)
     unpublished = true,
 }]
async fn asset(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<RestPathParam>,
) -> Result<Response<Body>, HttpError> {
    let apictx = rqctx.context();
    let path = path_params.into_inner().path;

    let file = find_file(path, &apictx.assets_directory)?;
    let file_contents =
        tokio::fs::read(&file).await.map_err(|_| not_found("EBADF"))?;

    // Derive the MIME type from the file name
    let content_type = mime_guess::from_path(&file)
        .first()
        .map_or("text/plain".to_string(), |m| m.to_string());

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, &content_type)
        .body(file_contents.into())?)
}

fn not_found(internal_msg: &str) -> HttpError {
    HttpError::for_not_found(None, internal_msg.to_string())
}

/// Starting from `root_dir`, follow the segments of `path` down the file tree
/// until we find a file (or not). Do not follow symlinks.
fn find_file(
    path: Vec<String>,
    root_dir: &PathBuf,
) -> Result<PathBuf, HttpError> {
    let mut current = root_dir.to_owned(); // start from `root_dir`
    for segment in &path {
        // If we hit a non-directory thing already and we still have segments
        // left in the path, bail. We have nowhere to go.
        if !current.is_dir() {
            return Err(not_found("ENOENT"));
        }

        current.push(segment);

        // Don't follow symlinks.
        // Error means either the user doesn't have permission to pull
        // metadata or the path doesn't exist.
        let m = current.symlink_metadata().map_err(|_| not_found("ENOENT"))?;
        if m.file_type().is_symlink() {
            return Err(not_found("EMLINK"));
        }
    }

    // can't serve a directory
    if current.is_dir() {
        return Err(not_found("EISDIR"));
    }

    Ok(current)
}

#[cfg(test)]
mod test {
    use super::find_file;
    use http::StatusCode;
    use std::{env::current_dir, path::PathBuf};

    fn get_path(path_str: &str) -> Vec<String> {
        path_str.split("/").map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_find_file_finds_file() {
        let root = current_dir().unwrap();
        let file = find_file(get_path("tests/fixtures/hello.txt"), &root);
        assert!(file.is_ok());
    }

    #[test]
    fn test_find_file_404_on_nonexistent() {
        let root = current_dir().unwrap();
        let error =
            find_file(get_path("tests/fixtures/nonexistent.svg"), &root)
                .unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "ENOENT".to_string());
    }

    #[test]
    fn test_find_file_404_on_nonexistent_nested() {
        let root = current_dir().unwrap();
        let error =
            find_file(get_path("tests/fixtures/a/b/c/nonexistent.svg"), &root)
                .unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "ENOENT".to_string());
    }

    #[test]
    fn test_find_file_404_on_directory() {
        let root = current_dir().unwrap();
        let error = find_file(get_path("tests/fixtures/a_directory"), &root)
            .unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "EISDIR".to_string());
    }

    #[test]
    fn test_find_file_404_on_symlink() {
        let root = current_dir().unwrap();
        let path_str = "tests/fixtures/a_symlink";

        // the file in question does exist and is a symlink
        assert!(root
            .join(PathBuf::from(path_str))
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink());

        // so we 404
        let error = find_file(get_path(path_str), &root).unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "EMLINK".to_string());
    }

    #[test]
    fn test_find_file_wont_follow_symlink() {
        let root = current_dir().unwrap();
        let path_str = "tests/fixtures/a_symlink/another_file.txt";

        // the file in question does exist
        assert!(root.join(PathBuf::from(path_str)).exists());

        // but it 404s because the path goes through a symlink
        let error = find_file(get_path(path_str), &root).unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        assert_eq!(error.internal_message, "EMLINK".to_string());
    }
}
