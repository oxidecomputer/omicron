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

/// Database representation of a fault management configuration override.
///
/// Each row in the `fm_config` table is an override of the system-defined
/// default configuration. Values representing the actual config settings are
/// nullable, with a NULL value indicating that the default for that setting
/// should be used.
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = fm_config)]
pub struct FmConfig {
    pub version: SqlU32,
    pub comment: String,
    pub time_modified: DateTime<Utc>,
    pub analysis_enabled: Option<bool>,
    pub sitrep_limit: Option<SqlU32>,
    pub history_pruning_threshold: Option<SqlU32>,
    pub certificate_expiry_warning_days: Option<SqlU32>,
}

impl FmConfig {
    pub fn new(param: fm::FmConfigParam) -> Result<Self, Error> {
        param.validate()?;
        let fm::FmConfigParam {
            version,
            comment,
            config:
                fm::FmConfig {
                    analysis_enabled,
                    sitrep_limit,
                    history_pruning_threshold,
                    certificate_expiry_warning_days,
                },
        } = param;
        Ok(Self {
            version: version.get().into(),
            comment,
            time_modified: Utc::now(),
            analysis_enabled: analysis_enabled.into_override(),
            sitrep_limit: sitrep_limit.map_override(|v| v.get().into()),
            history_pruning_threshold: history_pruning_threshold
                .map_override(|v| v.get().into()),
            certificate_expiry_warning_days: certificate_expiry_warning_days
                .map_override(|v| v.get().into()),
        })
    }
}

/// Note: this conversion does *not* re-run config validation rules, as they may
/// change across software versions, and we want to respect older configs if
/// they have already been written to CRDB, rather than failing to load them and
/// leaving FM blocked indefinitely. Instead, we only perform the validation
/// necessary to construct the domain type (i.e. checking `NonZeroU32`s).
impl TryFrom<FmConfig> for fm::FmConfigView {
    type Error = Error;

    fn try_from(value: FmConfig) -> Result<Self, Self::Error> {
        let FmConfig {
            version,
            comment,
            analysis_enabled,
            sitrep_limit,
            history_pruning_threshold,
            certificate_expiry_warning_days,
            time_modified,
        } = value;

        macro_rules! nz {
            ($field: ident) => {{
                $field.map(|value| {
                    let name = stringify!($field);
                    NonZeroU32::new(value.into()).ok_or_else(|| {
                        Error::invalid_value(
                            name,
                            format!(
                                "the fm_config row has {name} 0, which should \
                                    have violated a CHECK constraint!"
                            ),
                        )
                    })
                }).transpose()
            }};
        }

        // Construct the domain type, and then validate it.
        let config = fm::FmConfig {
            analysis_enabled: analysis_enabled.into(),
            sitrep_limit: nz!(sitrep_limit)?.into(),
            history_pruning_threshold: nz!(history_pruning_threshold)?.into(),
            certificate_expiry_warning_days: nz!(
                certificate_expiry_warning_days
            )?
            .into(),
        };
        let version = NonZeroU32::new(version.into()).ok_or_else(|| {
            Error::invalid_value(
                "version",
                "the fm_config row has version 0, which should have \
                 violated a CHECK constraint!",
            )
        })?;
        let source =
            fm::FmConfigSource::Override { version, time_modified, comment };

        Ok(Self { config, source })
    }
}
