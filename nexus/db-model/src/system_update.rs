use crate::schema::system_update;
use db_macros::Asset;
use nexus_types::{external_api::views, identity::Asset};
use serde::{Deserialize, Serialize};

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Asset,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = system_update)]
pub struct SystemUpdate {
    #[diesel(embed)]
    identity: SystemUpdateIdentity,
    pub version: String,
}

impl From<SystemUpdate> for views::SystemUpdate {
    fn from(system_update: SystemUpdate) -> Self {
        Self {
            identity: system_update.identity(),
            // TODO: figure out how to ser/de semver versions
            // version: system_update.version,
            version: views::SemverVersion::new(1, 0, 0),
        }
    }
}
