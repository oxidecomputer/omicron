// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use omicron_common::api::external::Error;
use omicron_common::api::internal;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    DatasetKindEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum DatasetKind;

    // Enum values
    Crucible => b"crucible"
    Cockroach => b"cockroach"
    Clickhouse => b"clickhouse"
    ClickhouseKeeper => b"clickhouse_keeper"
    ClickhouseServer => b"clickhouse_server"
    ExternalDns => b"external_dns"
    InternalDns => b"internal_dns"
    TransientZoneRoot => b"zone_root"
    TransientZone => b"zone"
    Debug => b"debug"
    Update => b"update"
    LocalStorage => b"local_storage"
);

impl DatasetKind {
    pub fn try_into_api(
        self,
        zone_name: Option<String>,
    ) -> Result<internal::shared::DatasetKind, Error> {
        use internal::shared::DatasetKind as ApiKind;
        let k = match (self, zone_name) {
            (Self::Crucible, None) => ApiKind::Crucible,
            (Self::Cockroach, None) => ApiKind::Cockroach,
            (Self::Clickhouse, None) => ApiKind::Clickhouse,
            (Self::ClickhouseKeeper, None) => ApiKind::ClickhouseKeeper,
            (Self::ClickhouseServer, None) => ApiKind::ClickhouseServer,
            (Self::ExternalDns, None) => ApiKind::ExternalDns,
            (Self::InternalDns, None) => ApiKind::InternalDns,
            (Self::TransientZoneRoot, None) => ApiKind::TransientZoneRoot,
            (Self::LocalStorage, None) => ApiKind::LocalStorage,
            (Self::TransientZone, Some(name)) => {
                ApiKind::TransientZone { name }
            }
            (Self::Debug, None) => ApiKind::Debug,
            (Self::TransientZone, None) => {
                return Err(Error::internal_error("Zone kind needs name"));
            }
            (_, Some(_)) => {
                return Err(Error::internal_error("Only zone kind needs name"));
            }
            // TODO-cleanup We should remove `Self::Update` entirely, but that's
            // a lot of work for not a lot of gain, so we filed that away for
            // our future selves as
            // https://github.com/oxidecomputer/omicron/issues/8268.
            (Self::Update, None) => {
                return Err(Error::internal_error(
                    "Unexpected `update` dataset kind",
                ));
            }
        };

        Ok(k)
    }
}

impl From<&internal::shared::DatasetKind> for DatasetKind {
    fn from(k: &internal::shared::DatasetKind) -> Self {
        match k {
            internal::shared::DatasetKind::Crucible => DatasetKind::Crucible,
            internal::shared::DatasetKind::Cockroach => DatasetKind::Cockroach,
            internal::shared::DatasetKind::Clickhouse => {
                DatasetKind::Clickhouse
            }
            internal::shared::DatasetKind::ClickhouseKeeper => {
                DatasetKind::ClickhouseKeeper
            }
            internal::shared::DatasetKind::ClickhouseServer => {
                DatasetKind::ClickhouseServer
            }
            internal::shared::DatasetKind::ExternalDns => {
                DatasetKind::ExternalDns
            }
            internal::shared::DatasetKind::InternalDns => {
                DatasetKind::InternalDns
            }
            internal::shared::DatasetKind::TransientZoneRoot => {
                DatasetKind::TransientZoneRoot
            }
            // Enums in the database do not have associated data, so this drops
            // the "name" of the zone and only considers the type.
            //
            // The zone name, if it exists, is stored in a separate column.
            internal::shared::DatasetKind::TransientZone { .. } => {
                DatasetKind::TransientZone
            }
            internal::shared::DatasetKind::Debug => DatasetKind::Debug,
            internal::shared::DatasetKind::LocalStorage => {
                DatasetKind::LocalStorage
            }
        }
    }
}
