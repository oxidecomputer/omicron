// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::db_metadata;

/// Internal database metadata
#[derive(Queryable, Insertable, Debug, Clone, Selectable)]
#[diesel(table_name = db_metadata)]
pub struct DbMetadata {
    name: String,
    value: String,
}

impl DbMetadata {
    pub fn new(name: String, value: String) -> Self {
        Self { name, value }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}
