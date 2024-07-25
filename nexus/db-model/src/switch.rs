// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::Generation;
use crate::{schema::switch, SqlU32};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_types::{external_api::shared, external_api::views, identity::Asset};
use uuid::Uuid;

/// Baseboard information about a switch.
///
/// The combination of these columns may be used as a unique identifier for the
/// switch.
pub struct SwitchBaseboard {
    pub serial_number: String,
    pub part_number: String,
    pub revision: u32,
}

/// Database representation of a Switch.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = switch)]
pub struct Switch {
    #[diesel(embed)]
    identity: SwitchIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    pub rack_id: Uuid,

    serial_number: String,
    part_number: String,
    revision: SqlU32,
}

impl Switch {
    // TODO: Unify hardware identifiers
    pub fn new(
        id: Uuid,
        serial_number: String,
        part_number: String,
        revision: u32,
        rack_id: Uuid,
    ) -> Self {
        Self {
            identity: SwitchIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            rack_id,
            serial_number,
            part_number,
            revision: SqlU32(revision),
        }
    }
}

impl From<Switch> for views::Switch {
    fn from(switch: Switch) -> Self {
        Self {
            identity: switch.identity(),
            rack_id: switch.rack_id,
            baseboard: shared::Baseboard {
                serial: switch.serial_number,
                part: switch.part_number,
                revision: *switch.revision,
            },
        }
    }
}
