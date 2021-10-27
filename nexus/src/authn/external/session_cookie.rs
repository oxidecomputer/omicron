//! authn scheme for console that looks up cookie values in a session table

use super::cookies::parse_cookies;
use super::{HttpAuthnScheme, Reason, SchemeResult};
use crate::authn;
use crate::authn::{Actor, Details};
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
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

    // TODO: these should all return results, it was just a lot easier to
    // write the tests with Option. will change it back
    async fn session_fetch(&self, token: String) -> Option<Self::SessionModel>;

    async fn session_update_last_used(
        &self,
        token: String,
    ) -> Option<Self::SessionModel>;

    async fn session_expire(&self, token: String) -> Option<()>;

    fn idle_timeout(&self) -> Duration;
    fn absolute_timeout(&self) -> Duration;
}

// generic cookie name is recommended by OWASP
pub const SESSION_COOKIE_COOKIE_NAME: &str = "session";
pub const SESSION_COOKIE_SCHEME_NAME: authn::SchemeName =
    authn::SchemeName("session_cookie");

/// Implements an authentication scheme where we check the DB to see if we have
/// a session matching the token in a cookie ([`SESSION_COOKIE_COOKIE_NAME`]) on
/// the request. This is meant to be used by the web console.
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
        let token = match get_token_from_cookie(request.headers()) {
            Some(token) => token,
            None => return SchemeResult::NotRequested,
        };

        let session = match ctx.session_fetch(token.clone()).await {
            Some(session) => session,
            None => {
                return SchemeResult::Failed(Reason::UnknownActor {
                    actor: token.to_owned(),
                });
            }
        };

        let actor = Actor(session.user_id());

        // if the session has gone unused for longer than idle_timeout, it is expired
        let now = Utc::now();
        if session.time_last_used() + ctx.idle_timeout() < now {
            ctx.session_expire(token.clone()).await;
            return SchemeResult::Failed(Reason::BadCredentials {
                actor,
                source: anyhow!(
                    "session expired due to idle timeout. last used: {}. time checked: {}. TTL: {}",
                    session.time_last_used(),
                    now,
                    ctx.idle_timeout()
                ),
            });
        }

        // if the user is still within the idle timeout, but the session has existed longer
        // than absolute_timeout, it is expired and we can no longer extend the session
        if session.time_created() + ctx.absolute_timeout() < now {
            ctx.session_expire(token.clone()).await;
            return SchemeResult::Failed(Reason::BadCredentials {
                actor,
                source: anyhow!(
                    "session expired due to absolute timeout. created: {}. last used: {}. time checked: {}. TTL: {}",
                    session.time_created(),
                    session.time_last_used(),
                    now,
                    ctx.absolute_timeout()
                ),
            });
        }

        match ctx.session_update_last_used(token.clone()).await {
            Some(_) => {}
            None => {
                // couldn't renew session wtf
            }
        };

        SchemeResult::Authenticated(Details { actor })
    }
}

fn get_token_from_cookie(
    headers: &http::HeaderMap<http::HeaderValue>,
) -> Option<String> {
    parse_cookies(headers).ok().and_then(|cs| {
        cs.get(SESSION_COOKIE_COOKIE_NAME).map(|c| c.value().to_string())
    })
}

#[cfg(test)]
mod test {
    use super::{
        get_token_from_cookie, Details, HttpAuthnScheme,
        HttpAuthnSessionCookie, Reason, SchemeResult, Session, SessionStore,
    };
    use async_trait::async_trait;
    use chrono::{DateTime, Duration, Utc};
    use http;
    use hyper;
    use slog;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use uuid::Uuid;

    // the mutex is annoying, but we need it in order to mutate the hashmap
    // without passing TestServerContext around as mutable
    struct TestServerContext {
        sessions: Mutex<HashMap<String, FakeSession>>,
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
            token: String,
        ) -> Option<Self::SessionModel> {
            self.sessions.lock().unwrap().get(&token).map(|s| *s)
        }

        async fn session_update_last_used(
            &self,
            token: String,
        ) -> Option<Self::SessionModel> {
            let mut sessions = self.sessions.lock().unwrap();
            let session = *sessions.get(&token).unwrap();
            let new_session =
                FakeSession { time_last_used: Utc::now(), ..session };
            (*sessions).insert(token, new_session)
        }

        async fn session_expire(&self, token: String) -> Option<()> {
            let mut sessions = self.sessions.lock().unwrap();
            (*sessions).remove(&token);
            Some(())
        }

        fn idle_timeout(&self) -> Duration {
            Duration::hours(1)
        }

        fn absolute_timeout(&self) -> Duration {
            Duration::hours(8)
        }
    }

    async fn authn_with_cookie(
        context: &TestServerContext,
        cookie: Option<&str>,
    ) -> SchemeResult {
        let scheme = HttpAuthnSessionCookie {};
        let log = slog::Logger::root(slog::Discard, o!());
        let mut request = http::Request::new(hyper::Body::from("hi"));
        if let Some(cookie) = cookie {
            let headers = request.headers_mut();
            headers.insert(http::header::COOKIE, cookie.parse().unwrap());
        }
        scheme.authn(context, &log, &request).await
    }

    #[tokio::test]
    async fn test_missing_cookie() {
        let context =
            TestServerContext { sessions: Mutex::new(HashMap::new()) };
        let result = authn_with_cookie(&context, None).await;
        assert!(matches!(result, SchemeResult::NotRequested));
    }

    #[tokio::test]
    async fn test_other_cookie() {
        let context =
            TestServerContext { sessions: Mutex::new(HashMap::new()) };
        let result = authn_with_cookie(&context, Some("other=def")).await;
        assert!(matches!(result, SchemeResult::NotRequested));
    }

    #[tokio::test]
    async fn test_expired_cookie_idle() {
        let context = TestServerContext {
            sessions: Mutex::new(HashMap::from([(
                "abc".to_string(),
                FakeSession {
                    time_last_used: Utc::now() - Duration::hours(2),
                    time_created: Utc::now() - Duration::hours(2),
                },
            )])),
        };
        let result = authn_with_cookie(&context, Some("session=abc")).await;
        assert!(matches!(
            result,
            SchemeResult::Failed(Reason::BadCredentials {
                actor: _,
                source: _
            })
        ));

        // key should be removed from sessions dict, i.e., session deleted
        assert!(!context.sessions.lock().unwrap().contains_key("abc"))
    }

    #[tokio::test]
    async fn test_expired_cookie_absolute() {
        let context = TestServerContext {
            sessions: Mutex::new(HashMap::from([(
                "abc".to_string(),
                FakeSession {
                    time_last_used: Utc::now(),
                    time_created: Utc::now() - Duration::hours(20),
                },
            )])),
        };
        let result = authn_with_cookie(&context, Some("session=abc")).await;
        assert!(matches!(
            result,
            SchemeResult::Failed(Reason::BadCredentials {
                actor: _,
                source: _
            })
        ));

        // key should be removed from sessions dict, i.e., session deleted
        let sessions = context.sessions.lock().unwrap();
        assert!(!sessions.contains_key("abc"))
    }

    #[tokio::test]
    async fn test_valid_cookie() {
        let time_last_used = Utc::now() - Duration::seconds(5);
        let context = TestServerContext {
            sessions: Mutex::new(HashMap::from([(
                "abc".to_string(),
                FakeSession { time_last_used, time_created: Utc::now() },
            )])),
        };
        let result = authn_with_cookie(&context, Some("session=abc")).await;
        assert!(matches!(
            result,
            SchemeResult::Authenticated(Details { actor: _ })
        ));

        // valid cookie should have updated time_last_used
        let sessions = context.sessions.lock().unwrap();
        assert!(sessions.get("abc").unwrap().time_last_used > time_last_used)
    }

    #[tokio::test]
    async fn test_garbage_cookie() {
        let context =
            TestServerContext { sessions: Mutex::new(HashMap::new()) };
        let result =
            authn_with_cookie(&context, Some("unparseable garbage!!!!!1"))
                .await;
        assert!(matches!(result, SchemeResult::NotRequested));
    }

    #[test]
    fn test_get_token() {
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::COOKIE, "session=abc".parse().unwrap());
        let token = get_token_from_cookie(&headers);
        assert_eq!(token, Some("abc".to_string()));
    }

    #[test]
    fn test_get_token_no_header() {
        let headers = http::HeaderMap::new();
        let token = get_token_from_cookie(&headers);
        assert_eq!(token, None);
    }

    #[test]
    fn test_get_token_other_cookie_present() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::COOKIE,
            "other_cookie=value".parse().unwrap(),
        );
        let token = get_token_from_cookie(&headers);
        assert_eq!(token, None);
    }
}
