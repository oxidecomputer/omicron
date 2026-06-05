// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_schema::schema::system_networking_settings;
use serde::{Deserialize, Serialize};

/// Singleton row holding fleet-wide networking settings.
#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Serialize,
    Deserialize,
    AsChangeset,
)]
#[diesel(table_name = system_networking_settings)]
pub struct SystemNetworkingSettings {
    pub singleton: bool,
    /// When true, end users may opt in to jumbo frames on the primary
    /// interface of an instance. When false, the per-instance opt-in is
    /// ignored and OPTE ports are created with the default MTU.
    pub external_jumbo_frames_opt_in_enabled: bool,
}

/// Updates to the [`SystemNetworkingSettings`] singleton.
#[derive(AsChangeset)]
#[diesel(table_name = system_networking_settings)]
pub struct SystemNetworkingSettingsUpdate {
    pub external_jumbo_frames_opt_in_enabled: bool,
}
