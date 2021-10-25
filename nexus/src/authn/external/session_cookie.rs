//! authn scheme for console that looks up cookie values in a session table

use super::super::Details;
use super::HttpAuthnScheme;
use super::Reason;
use super::SchemeResult;
use crate::authn;
use crate::authn::Actor;
use crate::ServerContext;
use anyhow::anyhow;
use anyhow::Context;
use async_trait::async_trait;
use chrono::Utc;
use cookie::{Cookie, CookieJar, ParseError};
use std::sync::Arc;

// many parts of the implementation will reference this OWASP guide
// https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html

// generic cookie name is recommended by OWASP
pub const SESSION_COOKIE_COOKIE_NAME: &str = "session";
pub const SESSION_COOKIE_SCHEME_NAME: authn::SchemeName =
    authn::SchemeName("session_cookie");

/// Implements an authentication scheme where we check the DB to see if we have
/// a session matching the token in a cookie ([`COOKIE_NAME_OXIDE_AUTHN_COOKIE`])
/// on the request. This is meant to be used by the web console.
#[derive(Debug)]
pub struct HttpAuthnSessionCookie;

#[async_trait]
impl HttpAuthnScheme<Arc<ServerContext>> for HttpAuthnSessionCookie {
    fn name(&self) -> authn::SchemeName {
        SESSION_COOKIE_SCHEME_NAME
    }

    async fn authn(
        &self,
        ctx: &Arc<ServerContext>,
        _log: &slog::Logger,
        request: &http::Request<hyper::Body>,
    ) -> SchemeResult {
        let headers = request.headers();

        // TODO: control flow here is clearly not idiomatic, get some help
        // TODO: logging
        // TODO: pass in session lookup function from outside

        let cookies = match parse_cookies(headers) {
            Ok(cookies) => cookies,
            Err(_) => return SchemeResult::NotRequested,
        };
        let token = match cookies.get(SESSION_COOKIE_COOKIE_NAME) {
            Some(cookie) => cookie.value(),
            None => return SchemeResult::NotRequested,
        };
        println!("\n=============\ntoken: \"{}\"", token);

        let session = match ctx.nexus.session_fetch(token.to_string()).await {
            Ok(session) => session,
            Err(_) => {
                println!("session not found");
                return SchemeResult::Failed(Reason::UnknownActor {
                    actor: token.to_owned(),
                });
            }
        };
        println!("session: {:?}", session);

        let actor = Actor(session.user_id);
        if session.time_expires < Utc::now() {
            return SchemeResult::Failed(Reason::BadCredentials {
                actor,
                source: anyhow!("expired session"),
            });
        }

        SchemeResult::Authenticated(Details { actor })
    }
}

// TODO: could live in its own file along with its tests, but where
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

#[cfg(test)]
mod test {
    use super::parse_cookies;
    use http::{
        header::{ACCEPT, COOKIE},
        HeaderMap,
    };

    #[test]
    fn test_parse_cookies_empty_headers() {
        let headers = HeaderMap::new();
        let cookies = parse_cookies(&headers).unwrap();
        assert_eq!(cookies.iter().count(), 0);
    }

    #[test]
    fn test_parse_cookies_one_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, "session=abc".parse().unwrap());
        let cookies = parse_cookies(&headers).unwrap();

        assert_eq!(cookies.iter().count(), 1);

        let cookie = cookies.get("session").unwrap();
        assert_eq!(cookie.name(), "session");
        assert_eq!(cookie.value(), "abc");
    }

    #[test]
    fn test_parse_cookies_two_cookies() {
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, "cookie1=abc; cookie2=def".parse().unwrap());
        let cookies = parse_cookies(&headers).unwrap();

        assert_eq!(cookies.iter().count(), 2);

        let cookie1 = cookies.get("cookie1").unwrap();
        assert_eq!(cookie1.name(), "cookie1");
        assert_eq!(cookie1.value(), "abc");

        let cookie2 = cookies.get("cookie2").unwrap();
        assert_eq!(cookie2.name(), "cookie2");
        assert_eq!(cookie2.value(), "def");
    }

    #[test]
    fn test_parse_cookies_two_cookie_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, "cookie1=abc".parse().unwrap());
        headers.append(COOKIE, "cookie2=def".parse().unwrap());

        let cookies = parse_cookies(&headers).unwrap();

        assert_eq!(cookies.iter().count(), 2);

        let cookie1 = cookies.get("cookie1").unwrap();
        assert_eq!(cookie1.name(), "cookie1");
        assert_eq!(cookie1.value(), "abc");

        let cookie2 = cookies.get("cookie2").unwrap();
        assert_eq!(cookie2.name(), "cookie2");
        assert_eq!(cookie2.value(), "def");
    }

    #[test]
    fn test_parse_cookies_two_cookie_headers_same_name() {
        // when two cookies in two separate cookie headers have the same name,
        // the second one should override the first
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, "cookie=abc".parse().unwrap());
        headers.append(COOKIE, "cookie=def".parse().unwrap());

        let cookies = parse_cookies(&headers).unwrap();

        assert_eq!(cookies.iter().count(), 1);

        let cookie = cookies.get("cookie").unwrap();
        assert_eq!(cookie.name(), "cookie");
        assert_eq!(cookie.value(), "def");
    }

    #[test]
    fn test_parse_cookies_ignore_other_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, "session=abc".parse().unwrap());
        headers.insert(ACCEPT, "application/json".parse().unwrap());

        let cookies = parse_cookies(&headers).unwrap();

        assert_eq!(cookies.iter().count(), 1);

        let cookie = cookies.get("session").unwrap();
        assert_eq!(cookie.name(), "session");
        assert_eq!(cookie.value(), "abc");
    }

    // test: missing cookie

    // test: expired cookie

    // test: cookie valid, session in DB but expired

    // test: cookie valid, session in DB and not expired

    // test: unparseable Cookie header value (NotRequested, not failed)
}
