// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Setting`]s.

// Settings are dynamically controlled values for Nexus

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::ErrorHandler;
use crate::db::datastore::public_error_from_diesel;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_lookup::DbConnection;
use nexus_db_model::Setting;
use nexus_db_model::SettingName;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Error;

impl DataStore {
    pub(crate) async fn set_control_plane_storage_buffer_gib_impl(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        gibibytes: u32,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        use nexus_db_schema::schema::setting::dsl;

        let name = SettingName::ControlPlaneStorageBuffer;
        let value: i64 = ByteCount::from_gibibytes_u32(gibibytes).into();

        let maybe_existing =
            Self::get_control_plane_storage_buffer_impl(opctx, conn).await?;

        if maybe_existing.is_some() {
            diesel::update(dsl::setting)
                .filter(dsl::name.eq(name))
                .set(dsl::int_value.eq(value))
                .execute_async(conn)
                .await
                .map(|_| ())
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
        } else {
            let setting = Setting { name, int_value: Some(value) };

            diesel::insert_into(dsl::setting)
                .values(setting)
                .execute_async(conn)
                .await
                .map(|_| ())
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
        }
    }

    pub(crate) async fn get_control_plane_storage_buffer_impl(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<ByteCount>, Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        use nexus_db_schema::schema::setting::dsl;

        let name = SettingName::ControlPlaneStorageBuffer;

        let result = dsl::setting
            .filter(dsl::name.eq(name))
            .select(Setting::as_select())
            .first_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(match result {
            Some(setting) => {
                // A row exists with this setting's name
                match setting.int_value {
                    Some(value) => Some(ByteCount::try_from(value).unwrap()),

                    None => None,
                }
            }

            None => None,
        })
    }

    pub async fn get_control_plane_storage_buffer(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<ByteCount>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::get_control_plane_storage_buffer_impl(opctx, &conn).await
    }
}
