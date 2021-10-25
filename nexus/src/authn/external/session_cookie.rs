//! authn scheme for console that looks up cookie values in a session table

use super::super::Details;
use super::HttpAuthnScheme;
use super::Reason;
use super::SchemeResult;
use crate::authn;
use crate::authn::Actor;
use crate::context::SessionBackend;
use anyhow::anyhow;
use anyhow::Context;
use async_trait::async_trait;
use chrono::{Duration, Utc};
use cookie::{Cookie, CookieJar, ParseError};
use lazy_static::lazy_static;

// many parts of the implementation will reference this OWASP guide
// https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html

// generic cookie name is recommended by OWASP
pub const SESSION_COOKIE_COOKIE_NAME: &str = "session";
pub const SESSION_COOKIE_SCHEME_NAME: authn::SchemeName =
    authn::SchemeName("session_cookie");

// TODO: put these in a config
lazy_static! {
    pub static ref SESSION_IDLE_TTL: Duration = Duration::seconds(3600);
    pub static ref SESSION_ABS_TTL: Duration = Duration::seconds(8 * 3600);
}

/// Implements an authentication scheme where we check the DB to see if we have
/// a session matching the token in a cookie ([`COOKIE_NAME_OXIDE_AUTHN_COOKIE`])
/// on the request. This is meant to be used by the web console.
#[derive(Debug)]
pub struct HttpAuthnSessionCookie;

#[async_trait]
impl<T> HttpAuthnScheme<T> for HttpAuthnSessionCookie
where
    T: Send + Sync + 'static + SessionBackend,
{
    fn name(&self) -> authn::SchemeName {
        SESSION_COOKIE_SCHEME_NAME
    }

    async fn authn(
        &self,
        ctx: &T,
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

        let session = match ctx.session_fetch(token.to_string()).await {
            Ok(session) => session,
            Err(_) => {
                println!("session not found"); // TODO: log this
                return SchemeResult::Failed(Reason::UnknownActor {
                    actor: token.to_owned(),
                });
            }
        };

        let actor = Actor(session.user_id);

        let now = Utc::now();
        if now - session.last_used > *SESSION_IDLE_TTL {
            return SchemeResult::Failed(Reason::BadCredentials {
                actor,
                source: anyhow!(
                    "session expired due to idle timeout. last used: {}. time checked: {}. TTL: {}",
                    session.last_used,
                    now,
                    *SESSION_IDLE_TTL
                ),
            });
        }

        // if the user is still within the idle timeout, but the session has existed longer
        // than the absolute timeout, we can no longer extend the session

        if now - session.time_created > *SESSION_ABS_TTL {
            return SchemeResult::Failed(Reason::BadCredentials {
                actor,
                source: anyhow!(
                    "session expired due to absolute timeout. created: {}. last used: {}. time checked: {}. TTL: {}",
                    session.time_created,
                    session.last_used,
                    now,
                    *SESSION_ABS_TTL
                ),
            });
        }

        // TODO: update last used to now

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

    // struct CookieAuthServerContext;

    // impl SessionBackend for CookieAuthServerContext {
    //     async fn session_fetch(
    //         &self,
    //         token: String,
    //     ) -> LookupResult<db::model::ConsoleSession> {
    //         Ok("hello")
    //     }
    // }

    // test: missing cookie

    // test: expired cookie

    // test: cookie valid, session in DB but expired

    // test: cookie valid, session in DB and not expired

    // test: unparseable Cookie header value (NotRequested, not failed)
}
