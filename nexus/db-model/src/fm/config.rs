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
    pub comment: String,
    pub analysis_enabled: bool,
    pub sitrep_limit: SqlU32,
    pub history_pruning_threshold: SqlU32,
    pub time_modified: DateTime<Utc>,
}

impl TryFrom<fm::FmConfigParam> for FmConfig {
    type Error = Error;

    fn try_from(param: fm::FmConfigParam) -> Result<Self, Self::Error> {
        param.validate()?;
        // This `FmConfigParam` is destructured exhaustively so that if any
        // fields are added to the `nexus_types` version, they must be handled
        // here and included in the `db::model` type.
        let fm::FmConfigParam {
            comment,
            version,
            config:
                fm::FmConfig {
                    analysis_enabled,
                    history_pruning_threshold,
                    sitrep_limit,
                },
        } = param;
        Ok(Self {
            version: version.get().into(),
            comment,
            analysis_enabled,
            sitrep_limit: sitrep_limit.get().into(),
            history_pruning_threshold: history_pruning_threshold.get().into(),
            time_modified: Utc::now(),
        })
    }
}

impl TryFrom<FmConfig> for fm::FmConfigView {
    type Error = Error;

    fn try_from(value: FmConfig) -> Result<Self, Self::Error> {
        let FmConfig {
            version,
            comment,
            analysis_enabled,
            sitrep_limit,
            history_pruning_threshold,
            time_modified,
        } = value;

        macro_rules! nz {
            ($field: ident) => {{
                let name = stringify!($field);
                NonZeroU32::new($field.into()).ok_or_else(|| {
                    Error::invalid_value(
                        name,
                        format!(
                            "the fm_config row has {name} 0, which should \
                                have violated a CHECK constraint!"
                        ),
                    )
                })
            }};
        }

        // Construct the domain type, and then validate it.
        let config = fm::FmConfig {
            analysis_enabled,
            sitrep_limit: nz!(sitrep_limit)?,
            history_pruning_threshold: nz!(history_pruning_threshold)?,
        };
        config.validate()?;
        let source = fm::FmConfigSource::Override {
            version: nz!(version)?,
            time_modified,
            comment,
        };

        Ok(Self { config, source })
    }
}
