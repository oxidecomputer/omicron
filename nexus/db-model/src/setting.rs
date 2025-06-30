// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_db_schema::schema::setting;

impl_enum_type!(
    SettingNameEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum SettingName;

    // Enum values
    ControlPlaneStorageBuffer => b"control_plane_storage_buffer"
);

#[derive(Queryable, Insertable, Debug, Selectable, Clone)]
#[diesel(table_name = setting)]
pub struct Setting {
    pub name: SettingName,
    pub int_value: Option<i64>,
}
