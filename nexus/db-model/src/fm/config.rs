// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing runtime configuration for the fault management
//! subsystem.

use crate::SqlU32;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::fm_config;
use nexus_types::fm;
use omicron_common::api::external::Error;
use std::num::NonZeroU32;

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = fm_config)]
pub struct FmConfig {
    pub version: SqlU32,
    pub sitrep_limit: SqlU32,
    pub sitrep_deletion_threshold: SqlU32,
    pub time_modified: DateTime<Utc>,
}

impl TryFrom<fm::FmConfigParam> for FmConfig {
    type Error = Error;

    fn try_from(param: fm::FmConfigParam) -> Result<Self, Self::Error> {
        // Validate the config values by converting the `FmConfigParam` into a
        // `nexus_types::fm::FmConfig`. This `FmConfig` is destructured
        // exhaustively so that if any fields are added to the `nexus_types`
        // version, they must be handled here and included in the `db::model`
        // type.
        let fm::FmConfig { sitrep_limit, sitrep_deletion_threshold } =
            fm::FmConfig::try_from(&param)?;
        Ok(Self {
            version: param.version.get().into(),
            sitrep_limit: sitrep_limit.get().into(),
            sitrep_deletion_threshold: sitrep_deletion_threshold.get().into(),
            time_modified: Utc::now(),
        })
    }
}

impl TryFrom<FmConfig> for fm::FmConfigView {
    type Error = Error;

    fn try_from(value: FmConfig) -> Result<Self, Self::Error> {
        let FmConfig {
            version,
            sitrep_limit,
            sitrep_deletion_threshold,
            time_modified,
        } = value;

        let version = NonZeroU32::new(version.into()).ok_or_else(|| {
            Error::invalid_value(
                "version",
                "the fm_config row has version 0, violating the \
                 `versions_are_positive` CHECK constraint",
            )
        })?;

        // Construct the validated domain type by first constructing an
        // (unvalidated) `FmConfigParam` and then converting it into a
        // `nexus_types::fm::FmConfig`. This validates the values we read from
        // the database.
        let param = fm::FmConfigParam {
            version,
            sitrep_limit: sitrep_limit.into(),
            sitrep_deletion_threshold: sitrep_deletion_threshold.into(),
        };
        let config = fm::FmConfig::try_from(&param)?;
        let source = fm::FmConfigSource::Override { version, time_modified };

        Ok(Self { config, source })
    }
}
