// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! authn scheme for console that looks up cookie values in a session table

use super::{HttpAuthnScheme, Reason, SchemeResult};
use crate::authn;
use crate::authn::{Actor, Details};
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use dropshot::HttpError;
use http::HeaderValue;
use nexus_types::authn::cookies::parse_cookies;
use omicron_uuid_kinds::ConsoleSessionUuid;
use omicron_uuid_kinds::SiloUserUuid;
use slog::debug;
use uuid::Uuid;

// many parts of the implementation will reference this OWASP guide
// https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html

pub trait Session {
    fn id(&self) -> ConsoleSessionUuid;
    fn silo_user_id(&self) -> SiloUserUuid;
    fn silo_id(&self) -> Uuid;
    fn silo_name(&self) -> &str;
    fn time_last_used(&self) -> DateTime<Utc>;
    fn time_created(&self) -> DateTime<Utc>;
}

#[async_trait]
pub trait SessionStore {
    type SessionModel;

    // TODO: these should all return results, it was just a lot easier to
    // write the tests with Option. will change it back

    /// Retrieve session from store by token
    async fn session_fetch(&self, token: String) -> Option<Self::SessionModel>;

    /// Extend session by updating time_last_used to now
    async fn session_update_last_used(
        &self,
        id: ConsoleSessionUuid,
    ) -> Option<Self::SessionModel>;

    /// Mark session expired
    async fn session_expire(&self, token: String) -> Option<()>;

    /// Maximum time session can remain idle before expiring
    fn session_idle_timeout(&self) -> Duration;

    /// Maximum lifetime of session including extensions
    fn session_absolute_timeout(&self) -> Duration;
}

// generic cookie name is recommended by OWASP
pub const SESSION_COOKIE_COOKIE_NAME: &str = "session";
pub const SESSION_COOKIE_SCHEME_NAME: authn::SchemeName =
    authn::SchemeName("session_cookie");

/// Generate session cookie header
pub fn session_cookie_header_value(
    token: &str,
    max_age: Duration,
    secure: bool,
) -> Result<HeaderValue, HttpError> {
    let value = format!(
        "{}={}; Path=/; HttpOnly; SameSite=Lax;{} Max-Age={}",
        SESSION_COOKIE_COOKIE_NAME,
        token,
        if secure { " Secure;" } else { "" },
        max_age.num_seconds()
    );
    // this will only fail if we accidentally stick a \n in there or something
    http::HeaderValue::from_str(&value).map_err(|_e| {
        HttpError::for_internal_error(format!(
            "unsupported cookie value: {:#}",
            value
        ))
    })
}

/// Generate session cookie with empty token and max-age=0 so browser deletes it
pub fn clear_session_cookie_header_value(
    secure: bool,
) -> Result<HeaderValue, HttpError> {
    session_cookie_header_value("", Duration::zero(), secure)
}

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
        log: &slog::Logger,
        request: &dropshot::RequestInfo,
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

        let actor = Actor::SiloUser {
            silo_user_id: session.silo_user_id(),
            silo_id: session.silo_id(),
            silo_name: session.silo_name().parse().unwrap(),
        };

        // if the session has gone unused for longer than idle_timeout, it is
        // expired
        let now = Utc::now();
        if session.time_last_used() + ctx.session_idle_timeout() < now {
            let expired_session = ctx.session_expire(token).await;
            if expired_session.is_none() {
                debug!(log, "failed to expire session")
            }
            return SchemeResult::Failed(Reason::BadCredentials {
                actor,
                source: anyhow!(
                    "session expired due to idle timeout. last used: {}. \
                    time checked: {}. TTL: {}",
                    session.time_last_used(),
                    now,
                    ctx.session_idle_timeout()
                ),
            });
        }

        // if the user is still within the idle timeout, but the session has
        // existed longer than absolute_timeout, it is expired and we can no
        // longer extend the session
        if session.time_created() + ctx.session_absolute_timeout() < now {
            let expired_session = ctx.session_expire(token).await;
            if expired_session.is_none() {
                debug!(log, "failed to expire session")
            }
            return SchemeResult::Failed(Reason::BadCredentials {
                actor,
                source: anyhow!(
                    "session expired due to absolute timeout. created: {}. \
                    last used: {}. time checked: {}. TTL: {}",
                    session.time_created(),
                    session.time_last_used(),
                    now,
                    ctx.session_absolute_timeout()
                ),
            });
        }

        // we don't want to 500 on error here because the user is legitimately
        // authenticated for this request at this point. The next request might
        // be wrongly considered idle, but that's a problem for the next
        // request.
        let updated_session = ctx.session_update_last_used(session.id()).await;
        if updated_session.is_none() {
            debug!(log, "failed to extend session")
        }

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
        Details, HttpAuthnScheme, HttpAuthnSessionCookie, Reason, SchemeResult,
        Session, SessionStore, get_token_from_cookie,
        session_cookie_header_value,
    };
    use async_trait::async_trait;
    use chrono::{DateTime, Duration, Utc};
    use http;
    use omicron_uuid_kinds::ConsoleSessionUuid;
    use omicron_uuid_kinds::SiloUserUuid;
    use slog;
    use std::sync::Mutex;
    use uuid::Uuid;

    // the mutex is annoying, but we need it in order to mutate the hashmap
    // without passing TestServerContext around as mutable
    struct TestServerContext {
        sessions: Mutex<Vec<FakeSession>>,
    }

    #[derive(Clone)]
    struct FakeSession {
        id: ConsoleSessionUuid,
        token: String,
        silo_user_id: SiloUserUuid,
        silo_id: Uuid,
        silo_name: &'static str,
        time_created: DateTime<Utc>,
        time_last_used: DateTime<Utc>,
    }

    impl Session for FakeSession {
        fn id(&self) -> ConsoleSessionUuid {
            self.id
        }
        fn silo_user_id(&self) -> SiloUserUuid {
            self.silo_user_id
        }
        fn silo_id(&self) -> Uuid {
            self.silo_id
        }
        fn silo_name(&self) -> &str {
            self.silo_name
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
            self.sessions
                .lock()
                .unwrap()
                .iter()
                .find(|s| s.token == token)
                .map(|s| s.clone())
        }

        async fn session_update_last_used(
            &self,
            id: ConsoleSessionUuid,
        ) -> Option<Self::SessionModel> {
            let mut sessions = self.sessions.lock().unwrap();
            if let Some(pos) = sessions.iter().position(|s| s.id == id) {
                let new_session = FakeSession {
                    time_last_used: Utc::now(),
                    ..sessions[pos].clone()
                };
                sessions[pos] = new_session.clone();
                Some(new_session)
            } else {
                None
            }
        }

        async fn session_expire(&self, token: String) -> Option<()> {
            let mut sessions = self.sessions.lock().unwrap();
            sessions.retain(|s| s.token != token);
            Some(())
        }

        fn session_idle_timeout(&self) -> Duration {
            Duration::hours(1)
        }

        fn session_absolute_timeout(&self) -> Duration {
            Duration::hours(8)
        }
    }

    async fn authn_with_cookie(
        context: &TestServerContext,
        cookie: Option<&str>,
    ) -> SchemeResult {
        let scheme = HttpAuthnSessionCookie {};
        let log = slog::Logger::root(slog::Discard, o!());
        let mut request = http::Request::new(dropshot::Body::from("hi"));
        if let Some(cookie) = cookie {
            let headers = request.headers_mut();
            headers.insert(http::header::COOKIE, cookie.parse().unwrap());
        }
        scheme
            .authn(
                context,
                &log,
                &dropshot::RequestInfo::new(
                    &request,
                    "0.0.0.0:0".parse().unwrap(),
                ),
            )
            .await
    }

    #[tokio::test]
    async fn test_missing_cookie() {
        let context = TestServerContext { sessions: Mutex::new(Vec::new()) };
        let result = authn_with_cookie(&context, None).await;
        assert!(matches!(result, SchemeResult::NotRequested));
    }

    #[tokio::test]
    async fn test_other_cookie() {
        let context = TestServerContext { sessions: Mutex::new(Vec::new()) };
        let result = authn_with_cookie(&context, Some("other=def")).await;
        assert!(matches!(result, SchemeResult::NotRequested));
    }

    #[tokio::test]
    async fn test_expired_cookie_idle() {
        let context = TestServerContext {
            sessions: Mutex::new(vec![FakeSession {
                id: ConsoleSessionUuid::new_v4(),
                token: "abc".to_string(),
                silo_user_id: SiloUserUuid::new_v4(),
                silo_id: Uuid::new_v4(),
                silo_name: "test-silo",
                time_last_used: Utc::now() - Duration::hours(2),
                time_created: Utc::now() - Duration::hours(2),
            }]),
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
        assert!(!sessions.iter().any(|s| s.token == "abc"))
    }

    #[tokio::test]
    async fn test_expired_cookie_absolute() {
        let context = TestServerContext {
            sessions: Mutex::new(vec![FakeSession {
                id: ConsoleSessionUuid::new_v4(),
                token: "abc".to_string(),
                silo_user_id: SiloUserUuid::new_v4(),
                silo_id: Uuid::new_v4(),
                silo_name: "test-silo",
                time_last_used: Utc::now(),
                time_created: Utc::now() - Duration::hours(20),
            }]),
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
        assert!(!sessions.iter().any(|s| s.token == "abc"))
    }

    #[tokio::test]
    async fn test_valid_cookie() {
        let time_last_used = Utc::now() - Duration::seconds(5);
        let context = TestServerContext {
            sessions: Mutex::new(vec![FakeSession {
                id: ConsoleSessionUuid::new_v4(),
                token: "abc".to_string(),
                silo_user_id: SiloUserUuid::new_v4(),
                silo_id: Uuid::new_v4(),
                silo_name: "test-silo",
                time_last_used,
                time_created: Utc::now(),
            }]),
        };
        let result = authn_with_cookie(&context, Some("session=abc")).await;
        assert!(matches!(
            result,
            SchemeResult::Authenticated(Details { actor: _ })
        ));

        // valid cookie should have updated time_last_used
        let sessions = context.sessions.lock().unwrap();
        let session = sessions.iter().find(|s| s.token == "abc").unwrap();
        assert!(session.time_last_used > time_last_used)
    }

    #[tokio::test]
    async fn test_garbage_cookie() {
        let context = TestServerContext { sessions: Mutex::new(Vec::new()) };
        let result =
            authn_with_cookie(&context, Some("unparsable garbage!!!!!1")).await;
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

    #[test]
    fn test_session_cookie_value() {
        assert_eq!(
            session_cookie_header_value("abc", Duration::seconds(5), true)
                .unwrap(),
            "session=abc; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=5"
        );

        assert_eq!(
            session_cookie_header_value("abc", Duration::seconds(-5), true)
                .unwrap(),
            "session=abc; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=-5"
        );

        assert_eq!(
            session_cookie_header_value("", Duration::zero(), true).unwrap(),
            "session=; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=0"
        );

        // secure=false leaves out "Secure;"
        assert_eq!(
            session_cookie_header_value("abc", Duration::seconds(5), false)
                .unwrap(),
            "session=abc; Path=/; HttpOnly; SameSite=Lax; Max-Age=5"
        );
    }

    #[test]
    fn test_session_cookie_value_error() {
        assert_eq!(
            session_cookie_header_value("abc\ndef", Duration::seconds(5), true)
                .unwrap_err()
                .internal_message,
            "unsupported cookie value: session=abc\ndef; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=5".to_string(),
        );
    }
}
