use super::ServerContext;

use crate::context::OpContext;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseUpdatedNoContent,
    RequestContext,
};
use std::sync::Arc;

type NexusApiDescription = ApiDescription<Arc<ServerContext>>;

/**
 * Returns a description of the part of the nexus API dedicated to the web console
 */
pub fn api() -> NexusApiDescription {
    fn register_endpoints(api: &mut NexusApiDescription) -> Result<(), String> {
        api.register(logout)?;
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

// TODO:
// - POST login
// - /* route for serving bundle (redirect to configured IdP if unauthed)
// - /assets/* for images and fonts
