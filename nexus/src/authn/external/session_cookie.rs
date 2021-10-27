//! authn scheme for console that looks up cookie values in a session table

use super::cookies::parse_cookies;
use super::{HttpAuthnScheme, Reason, SchemeResult};
use crate::authn;
use crate::authn::{Actor, Details};
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
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

    // TODO: these should all return results, it was just a lot easier to
    // write the tests with Option. will change it back
    async fn session_fetch(&self, token: String) -> Option<Self::SessionModel>;

    async fn session_update_last_used(
        &self,
        token: String,
    ) -> Option<Self::SessionModel>;

    async fn session_expire(&self, token: String) -> Option<()>;
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

        // TODO: could move this logic into methods on the session trait
        let now = Utc::now();
        if session.time_last_used() + *SESSION_IDLE_TTL < now {
            ctx.session_expire(token.clone()).await;
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

        if session.time_created() + *SESSION_ABS_TTL < now {
            ctx.session_expire(token.clone()).await;
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

// TODO: these unit tests for the scheme are pretty concise and get the idea across, but
// still feel a little off

#[cfg(test)]
mod test {
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

        async fn session_expire(&self, _token: String) -> Option<()> {
            Some(())
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
        // TODO: find a way to assert that it deletes the session?
    }

    // TODO: test session expired due to absolute TTL

    // TODO: test get_token_from_cookies

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
