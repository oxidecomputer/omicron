// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Database Metadata.

use super::DataStore;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::Error;
use omicron_common::api::external::SemverVersion;
use std::str::FromStr;

impl DataStore {
    pub async fn database_schema_version(
        &self,
    ) -> Result<SemverVersion, Error> {
        use db::schema::db_metadata::dsl;

        let version: String = dsl::db_metadata
            .filter(dsl::name.eq("schema_version"))
            .select(dsl::value)
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

        SemverVersion::from_str(&version).map_err(|e| {
            Error::internal_error(&format!("Invalid schema version: {e}"))
        })
    }
}
