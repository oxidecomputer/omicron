use super::Generation;
use crate::schema::switch;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_types::{external_api::views, identity::Asset};
use uuid::Uuid;

/// Baseboard information about a switch.
///
/// The combination of these columns may be used as a unique identifier for the
/// switch.
pub struct SwitchBaseboard {
    pub serial_number: String,
    pub part_number: String,
    pub revision: i64,
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
    revision: i64,
}

impl Switch {
    // TODO: Unify hardware identifiers
    pub fn new(
        id: Uuid,
        serial_number: String,
        part_number: String,
        revision: i64,
        rack_id: Uuid,
    ) -> Self {
        Self {
            identity: SwitchIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            rack_id,
            serial_number,
            part_number,
            revision,
        }
    }
}

impl From<Switch> for views::Switch {
    fn from(switch: Switch) -> Self {
        Self {
            identity: switch.identity(),
            rack_id: switch.rack_id,
            baseboard: views::Baseboard {
                serial: switch.serial_number,
                part: switch.part_number,
                revision: switch.revision,
            },
        }
    }
}
