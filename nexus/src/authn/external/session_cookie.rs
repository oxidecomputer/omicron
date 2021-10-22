//! authn scheme for console that looks up cookie values in a session table

use super::super::Details;
use super::HttpAuthnScheme;
use super::Reason;
use super::SchemeResult;
use crate::authn;
use crate::authn::Actor;
use anyhow::anyhow;
use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use cookie::{Cookie, CookieJar, ParseError};
use uuid::Uuid;

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

impl<T> HttpAuthnScheme<T> for HttpAuthnSessionCookie
where
    T: Send + Sync + 'static,
{
    fn name(&self) -> authn::SchemeName {
        SESSION_COOKIE_SCHEME_NAME
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
        let result = authn_cookie(cookies.get(SESSION_COOKIE_COOKIE_NAME));
        println!("\n=============\nresult: {:?}\n=============\n", result);
        result
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

fn mock_lookup(token: &str) -> Option<(&str, Uuid, DateTime<Utc>)> {
    match token {
        "good" => {
            Some((token, Uuid::new_v4(), Utc::now() + Duration::seconds(30)))
        }
        "expired" => {
            Some((token, Uuid::new_v4(), Utc::now() - Duration::seconds(300)))
        }
        _ => None,
    }
}

fn authn_cookie(cookie: Option<&Cookie>) -> SchemeResult {
    if let Some(cookie) = cookie {
        let token = cookie.value();
        println!("\n=============\ntoken: {}\n=============\n", token);

        // look up the cookie in the session table and pull the corresponding user
        let session = mock_lookup(token);
        println!("\n=============\nsession: {:?}\n=============\n", session);

        if let Some(session) = session {
            let actor = Actor(session.1);
            if session.2 < Utc::now() {
                return SchemeResult::Failed(Reason::BadCredentials {
                    actor,
                    source: anyhow!("expired session"),
                });
            } else {
                return SchemeResult::Authenticated(Details { actor });
            }
        } else {
            return SchemeResult::Failed(Reason::UnknownActor {
                actor: token.to_owned(),
            });
        }
    } else {
        SchemeResult::NotRequested
    }
}

#[cfg(test)]
mod test {
    use super::super::SchemeResult;
    use super::{authn_cookie, parse_cookies};
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
