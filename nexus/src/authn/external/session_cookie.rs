//! authn scheme for console that looks up cookie values in a session table

use super::{HttpAuthnScheme, Reason, SchemeResult};
use crate::authn;
use crate::authn::{Actor, Details};
use anyhow::anyhow;
use anyhow::Context;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use cookie::{Cookie, CookieJar, ParseError};
use lazy_static::lazy_static;
use uuid::Uuid;

// many parts of the implementation will reference this OWASP guide
// https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html

pub trait Session {
    fn user_id(&self) -> Uuid;
    fn time_last_used(&self) -> DateTime<Utc>;
    fn time_created(&self) -> DateTime<Utc>;
}

#[async_trait]
pub trait SessionStore {
    type SessionModel;

    // TODO: these should return results, it was just a lot easier to
    // write the tests with Option. will change it back
    async fn session_fetch(&self, token: String) -> Option<Self::SessionModel>;

    async fn session_update_last_used(
        &self,
        token: String,
    ) -> Option<Self::SessionModel>;
}

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
    T: Send + Sync + 'static + SessionStore,
    T::SessionModel: Send + Sync + 'static + Session,
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
        let cookies = match parse_cookies(request.headers()) {
            Ok(cookies) => cookies,
            Err(_) => return SchemeResult::NotRequested,
        };
        let token = match cookies.get(SESSION_COOKIE_COOKIE_NAME) {
            Some(cookie) => cookie.value(),
            None => return SchemeResult::NotRequested,
        };

        let session = match ctx.session_fetch(token.to_string()).await {
            Some(session) => session,
            None => {
                println!("session not found"); // TODO: log this
                return SchemeResult::Failed(Reason::UnknownActor {
                    actor: token.to_owned(),
                });
            }
        };

        let actor = Actor(session.user_id());

        // TODO: could move this logic into methods on the session trait
        let now = Utc::now();
        if session.time_last_used() + *SESSION_IDLE_TTL < now {
            // TODO: hard delete the session?
            return SchemeResult::Failed(Reason::BadCredentials {
                actor,
                source: anyhow!(
                    "session expired due to idle timeout. last used: {}. time checked: {}. TTL: {}",
                    session.time_last_used(),
                    now,
                    *SESSION_IDLE_TTL
                ),
            });
        }

        // if the user is still within the idle timeout, but the session has existed longer
        // than the absolute timeout, we can no longer extend the session

        if session.time_last_used() + *SESSION_ABS_TTL < now {
            // TODO: hard delete the session?
            return SchemeResult::Failed(Reason::BadCredentials {
                actor,
                source: anyhow!(
                    "session expired due to absolute timeout. created: {}. last used: {}. time checked: {}. TTL: {}",
                    session.time_created(),
                    session.time_last_used(),
                    now,
                    *SESSION_ABS_TTL
                ),
            });
        }

        match ctx.session_update_last_used(token.to_string()).await {
            Some(_) => {}
            None => {
                // couldn't renew session wtf
            }
        };

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

// TODO: these unit tests for the scheme are pretty concise and get the idea across, but
// still feel a little off

#[cfg(test)]
mod test_session_cookie_scheme {
    use super::{
        Details, HttpAuthnScheme, HttpAuthnSessionCookie, Reason, SchemeResult,
        Session, SessionStore,
    };
    use async_trait::async_trait;
    use chrono::{DateTime, Duration, Utc};
    use http;
    use hyper;
    use slog;
    use uuid::Uuid;

    struct TestServerContext {
        // this global to the context instance. it is the response to every session fetch
        global_session: Option<FakeSession>,
    }

    #[derive(Clone, Copy)]
    struct FakeSession {
        time_created: DateTime<Utc>,
        time_last_used: DateTime<Utc>,
    }

    impl Session for FakeSession {
        fn user_id(&self) -> Uuid {
            Uuid::new_v4()
        }
        fn time_created(&self) -> DateTime<Utc> {
            self.time_created
        }
        fn time_last_used(&self) -> DateTime<Utc> {
            self.time_last_used
        }
    }

    #[async_trait]
    impl SessionStore for TestServerContext {
        type SessionModel = FakeSession;

        async fn session_fetch(
            &self,
            _token: String,
        ) -> Option<Self::SessionModel> {
            self.global_session
        }

        async fn session_update_last_used(
            &self,
            _token: String,
        ) -> Option<Self::SessionModel> {
            self.global_session
        }
    }

    async fn authn_with_cookie_header(
        context: TestServerContext,
        cookie: Option<&str>,
    ) -> SchemeResult {
        let scheme = HttpAuthnSessionCookie {};
        let log = slog::Logger::root(slog::Discard, o!());
        let mut request = http::Request::new(hyper::Body::from("hi"));
        if let Some(cookie) = cookie {
            let headers = request.headers_mut();
            headers.insert(http::header::COOKIE, cookie.parse().unwrap());
        }
        scheme.authn(&context, &log, &request).await
    }

    #[tokio::test]
    async fn test_missing_cookie() {
        let context = TestServerContext { global_session: None };
        let result = authn_with_cookie_header(context, None).await;
        assert!(matches!(result, SchemeResult::NotRequested));
    }

    #[tokio::test]
    async fn test_other_cookie() {
        let context = TestServerContext { global_session: None };
        let result = authn_with_cookie_header(context, Some("other=def")).await;
        assert!(matches!(result, SchemeResult::NotRequested));
    }

    #[tokio::test]
    async fn test_expired_cookie() {
        let context = TestServerContext {
            global_session: Some(FakeSession {
                time_last_used: Utc::now() - Duration::minutes(70),
                time_created: Utc::now() - Duration::minutes(70),
            }),
        };
        let result =
            authn_with_cookie_header(context, Some("session=abc")).await;
        assert!(matches!(
            result,
            SchemeResult::Failed(Reason::BadCredentials {
                actor: _,
                source: _
            })
        ));
    }

    #[tokio::test]
    async fn test_valid_cookie() {
        let context = TestServerContext {
            global_session: Some(FakeSession {
                time_last_used: Utc::now(),
                time_created: Utc::now(),
            }),
        };
        let result =
            authn_with_cookie_header(context, Some("session=abc")).await;
        assert!(matches!(
            result,
            SchemeResult::Authenticated(Details { actor: _ })
        ));
    }

    #[tokio::test]
    async fn test_garbage_cookie() {
        let context = TestServerContext { global_session: None };
        let result = authn_with_cookie_header(
            context,
            Some("unparseable garbage!!!!!1"),
        )
        .await;
        assert!(matches!(result, SchemeResult::NotRequested));
    }
}

#[cfg(test)]
mod test_parse_cookies {
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
}
