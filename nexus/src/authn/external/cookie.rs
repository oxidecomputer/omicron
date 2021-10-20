//! authn scheme for console that looks up cookie values in a session table

use super::super::Details;
use super::HttpAuthnScheme;
use super::Reason;
use super::SchemeResult;
use crate::authn;
use crate::authn::Actor;
use anyhow::anyhow;
use anyhow::Context;
use cookie::{Cookie, CookieJar, ParseError};
use uuid::Uuid;

pub const COOKIE_NAME_OXIDE_AUTHN_COOKIE: &str = "oxide-authn-cookie";
pub const COOKIE_SCHEME_NAME: authn::SchemeName = authn::SchemeName("cookie");

/// Implements an authentication scheme where we check the DB to see if we have
/// a session matching the token in a cookie ([`COOKIE_NAME_OXIDE_AUTHN_COOKIE`])
/// on the request. This is meant to be used by the web console.
#[derive(Debug)]
pub struct HttpAuthnCookie;

impl<T> HttpAuthnScheme<T> for HttpAuthnCookie
where
    T: Send + Sync + 'static,
{
    fn name(&self) -> authn::SchemeName {
        COOKIE_SCHEME_NAME
    }

    fn authn(
        &self,
        _ctx: &T,
        _log: &slog::Logger,
        request: &http::Request<hyper::Body>,
    ) -> SchemeResult {
        let headers = request.headers();
        let cookies = match parse_cookies(headers) {
            Ok(cookies) => cookies,
            Err(_) => return SchemeResult::NotRequested,
        };
        authn_cookie(cookies.get(COOKIE_NAME_OXIDE_AUTHN_COOKIE))
    }
}

fn parse_cookies(
    headers: &http::HeaderMap<http::HeaderValue>,
) -> Result<CookieJar, ParseError> {
    let mut cookies = CookieJar::new();
    for header in headers.get_all("Cookie") {
        let raw_str =
            match header.to_str().context("parsing Cookie header as UTF-8") {
                Ok(string) => string,
                Err(_) => continue,
            };
        for chunk in raw_str.split(';').map(|s| s.trim()) {
            if let Ok(cookie) = Cookie::parse(chunk) {
                cookies.add_original(cookie.into_owned());
            }
        }
    }
    Ok(cookies)
}

fn authn_cookie(cookie: Option<&Cookie>) -> SchemeResult {
    if let Some(cookie) = cookie {
        let cookie_value = cookie.value();

        // check expiration
        // match cookie.expires_datetime() {
        //     Some(exp) if exp < Utc::now() => {
        //         // return expired error
        //     }
        //     None => {
        //         // should probably error if there's no expiration
        //     }
        //     Some(exp) => {
        //         // continue
        //     }
        // }

        // look up the cookie in the session table and pull the corresponding user
        let session_exists = true;
        if session_exists {
            let actor = Actor(Uuid::new_v4());
            let session_expired = false;
            if session_expired {
                return SchemeResult::Failed(Reason::BadCredentials {
                    actor,
                    source: anyhow!("expired session"),
                });
            } else {
                return SchemeResult::Authenticated(Details { actor });
            }
        } else {
            return SchemeResult::Failed(Reason::UnknownActor {
                actor: cookie_value.to_owned(),
            });
        }
    } else {
        SchemeResult::NotRequested
    }
}

#[cfg(test)]
mod test {
    use super::super::super::{Actor, Details, Reason};
    use super::super::SchemeResult;
    use super::{authn_cookie, COOKIE_NAME_OXIDE_AUTHN_COOKIE};
    use cookie::{Cookie, CookieJar};


    #[test]
    fn test_cookie_missing() {
        // The client provided no credentials.
        assert!(matches!(authn_cookie(None), SchemeResult::NotRequested));
    }

    // test: expired cookie

    // test: cookie valid, session in DB but expired

    // test: cookie valid, session in DB and not expired

    // test: unparseable Cookie header value (NotRequested, not failed)
}
