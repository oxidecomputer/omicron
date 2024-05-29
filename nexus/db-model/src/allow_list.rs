// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/5.0/.

// Copyright 2024 Oxide Computer Company

//! Database representation of allowed source IP address, for implementing basic
//! IP allowlisting.

use crate::schema::allow_list;
use chrono::DateTime;
use chrono::Utc;
use ipnetwork::IpNetwork;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Database model for an allowlist of source IP addresses.
#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = allow_list)]
pub struct AllowList {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub allowed_ips: Option<Vec<IpNetwork>>,
}

impl AllowList {
    /// Construct a new allowlist record.
    pub fn new(id: Uuid, allowed_ips: external::AllowedSourceIps) -> Self {
        let now = Utc::now();
        let allowed_ips = match allowed_ips {
            external::AllowedSourceIps::Any => None,
            external::AllowedSourceIps::List(list) => {
                Some(list.into_iter().map(Into::into).collect())
            }
        };
        Self { id, time_created: now, time_modified: now, allowed_ips }
    }

    /// Create an `AllowedSourceIps` type from the contained address.
    pub fn allowed_source_ips(
        &self,
    ) -> Result<external::AllowedSourceIps, Error> {
        match &self.allowed_ips {
            Some(list) => external::AllowedSourceIps::try_from(list.as_slice())
                .map_err(|_| {
                    Error::internal_error(
                        "Allowlist from database is empty, but NULL \
                        should be used to allow any source IP",
                    )
                }),
            None => Ok(external::AllowedSourceIps::Any),
        }
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = allow_list, treat_none_as_null = true)]
pub struct AllowListUpdate {
    /// The new list of allowed IPs.
    pub allowed_ips: Option<Vec<IpNetwork>>,
}

impl From<params::AllowListUpdate> for AllowListUpdate {
    fn from(params: params::AllowListUpdate) -> Self {
        let allowed_ips = match params.allowed_ips {
            external::AllowedSourceIps::Any => None,
            external::AllowedSourceIps::List(list) => {
                Some(list.into_iter().map(Into::into).collect())
            }
        };
        Self { allowed_ips }
    }
}

impl TryFrom<AllowList> for views::AllowList {
    type Error = Error;

    fn try_from(db: AllowList) -> Result<Self, Self::Error> {
        db.allowed_source_ips().map(|allowed_ips| Self {
            time_created: db.time_created,
            time_modified: db.time_modified,
            allowed_ips,
        })
    }
}
