// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use nexus_db_errors::{ErrorHandler, OptionalError, public_error_from_diesel};
use nexus_db_model::ServiceAccountToken;
use nexus_db_schema::schema::service_account;
use nexus_db_schema::schema::service_account_token::dsl;
use nexus_types::external_api::{
    service_account as account, service_account_token as api,
};
use nexus_types::identity::Resource;
use omicron_common::api::external::{
    DataPageParams, Error, LookupType, ResourceType,
};
use std::num::NonZeroU32;
use uuid::Uuid;

fn token_expiration(
    now: DateTime<Utc>,
    ttl_seconds: Option<NonZeroU32>,
    caller_expiration: Option<DateTime<Utc>>,
) -> Result<Option<DateTime<Utc>>, Error> {
    let expires = match ttl_seconds {
        Some(ttl) => {
            let expires = now + Duration::seconds(i64::from(ttl.get()));
            if caller_expiration.is_some_and(|limit| expires > limit) {
                return Err(Error::invalid_request(
                    "Requested token TTL would exceed the expiration time of the \
                     authenticating token. Omit ttl_seconds to inherit its expiration.",
                ));
            }
            Some(expires)
        }
        None => caller_expiration,
    };
    if expires.is_some_and(|expires| expires <= now) {
        return Err(Error::invalid_request(
            "token expiration must be in the future",
        ));
    }
    Ok(expires)
}

impl DataStore {
    pub async fn service_account_token_create(
        &self,
        opctx: &OpContext,
        path: account::ServiceAccountPath,
        parent: account::ServiceAccountParentSelector,
        params: api::ServiceAccountTokenCreate,
    ) -> Result<ServiceAccountToken, Error> {
        let account = self
            .service_account_lookup_for(
                opctx,
                path,
                parent,
                authz::Action::Modify,
            )
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("service_account_token_create")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let account_id = account.id();
                async move {
                    service_account::table
                        .filter(service_account::id.eq(account_id))
                        .filter(service_account::time_deleted.is_null())
                        .select(service_account::id)
                        .first_async::<Uuid>(&conn)
                        .await?;
                    let time_expires = token_expiration(
                        Utc::now(),
                        params.ttl_seconds,
                        opctx.authn.token_expiration(),
                    )
                    .map_err(|e| err.bail(e))?;
                    let token =
                        ServiceAccountToken::new(account_id, time_expires);
                    diesel::insert_into(dsl::service_account_token)
                        .values(token)
                        .returning(ServiceAccountToken::as_returning())
                        .get_result_async(&conn)
                        .await
                }
            })
            .await
            .map_err(|e| {
                err.take().unwrap_or_else(|| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByLookup(
                            ResourceType::ServiceAccount,
                            LookupType::ById(account.id()),
                        ),
                    )
                })
            })
    }

    pub async fn service_account_token_list(
        &self,
        opctx: &OpContext,
        path: account::ServiceAccountPath,
        parent: account::ServiceAccountParentSelector,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> Result<Vec<api::ServiceAccountToken>, Error> {
        let account = self
            .service_account_lookup_for(
                opctx,
                path,
                parent,
                authz::Action::ListChildren,
            )
            .await?;
        let tokens = paginated(dsl::service_account_token, dsl::id, pagparams)
            .filter(dsl::service_account_id.eq(account.id()))
            .filter(dsl::time_deleted.is_null())
            .select(ServiceAccountToken::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(tokens.into_iter().map(Into::into).collect())
    }

    pub async fn service_account_token_view(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountTokenPath,
        parent: account::ServiceAccountParentSelector,
    ) -> Result<api::ServiceAccountToken, Error> {
        let account = self
            .service_account_lookup_for(
                opctx,
                path.account_path(),
                parent,
                authz::Action::Read,
            )
            .await?;
        let token = dsl::service_account_token
            .filter(dsl::id.eq(path.token_id))
            .filter(dsl::service_account_id.eq(account.id()))
            .filter(dsl::time_deleted.is_null())
            .select(ServiceAccountToken::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::ServiceAccountToken,
                        LookupType::ById(path.token_id),
                    ),
                )
            })?;
        Ok(token.into())
    }

    pub async fn service_account_token_delete(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountTokenPath,
        parent: account::ServiceAccountParentSelector,
    ) -> Result<(), Error> {
        let account = self
            .service_account_lookup_for(
                opctx,
                path.account_path(),
                parent,
                authz::Action::Modify,
            )
            .await?;
        let deleted = diesel::update(dsl::service_account_token)
            .filter(dsl::id.eq(path.token_id))
            .filter(dsl::service_account_id.eq(account.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if deleted == 0 {
            return Err(Error::not_found_by_id(
                ResourceType::ServiceAccountToken,
                &path.token_id,
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_account_token_expiration_bounds() {
        let now = Utc::now();
        let limit = now + Duration::seconds(60);
        assert_eq!(token_expiration(now, None, None).unwrap(), None);
        assert_eq!(
            token_expiration(now, None, Some(limit)).unwrap(),
            Some(limit)
        );
        assert_eq!(
            token_expiration(now, NonZeroU32::new(60), Some(limit)).unwrap(),
            Some(limit)
        );
        assert_eq!(
            token_expiration(now, NonZeroU32::new(30), Some(limit)).unwrap(),
            Some(now + Duration::seconds(30))
        );
        assert!(
            token_expiration(now, NonZeroU32::new(61), Some(limit)).is_err()
        );
        for expired in [now, now - Duration::seconds(1)] {
            assert!(token_expiration(now, None, Some(expired)).is_err());
            assert!(
                token_expiration(now, NonZeroU32::new(1), Some(expired))
                    .is_err()
            );
        }
        assert_eq!(
            token_expiration(now, NonZeroU32::new(u32::MAX), None).unwrap(),
            Some(now + Duration::seconds(i64::from(u32::MAX)))
        );
    }
}
