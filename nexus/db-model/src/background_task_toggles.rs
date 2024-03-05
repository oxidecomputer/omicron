use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::schema::background_task_toggles;

/// Values used to create a background task toggle
#[derive(Insertable, Debug, Clone, Eq, PartialEq)]
#[diesel(table_name = background_task_toggles)]
pub struct BackgroundTaskToggleValues {
    pub name: String,
    pub enabled: bool,
}

/// Database representation of a background task toggle
///
/// TODO: discussion
/// This is currently desgined to be "internal use only", and likely needs more discussion.
/// The motivation for this is to allow for the ability to toggle background
/// tasks on and off, since there are phases in first rack startup where the background tasks are
/// active before Nexus "proper" is active, and there is critical information provided to Nexus via
/// `handoff_to_nexus` that isn't yet stored in the DB that some RPWs rely on. I (levon) am not sure
/// if this something we should _require_ for all RPWs / tasks, so I don't think it's a problem if
/// all tasks don't have a toggle present in the table. However, for troubleshooting purposes, I can
/// see it being beneficial to have a toggle for every task.
///
#[derive(Queryable, Debug, Clone, Selectable, Serialize, Deserialize)]
#[diesel(table_name = background_task_toggles)]
pub struct BackgroundTaskToggle {
    pub id: Uuid,
    pub name: String,
    pub enabled: bool,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

// TODO convert to enum?
pub const SYNC_SWITCH_PORT_SETTINGS: &str = "sync_switch_port_settings";
