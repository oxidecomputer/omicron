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
