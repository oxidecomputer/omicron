// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for mapping CockroachDB Omicron zone IDs to internal-to-CRDB node IDs

use crate::schema::cockroachdb_zone_id_to_node_id;
use crate::typed_uuid::DbTypedUuid;
use omicron_uuid_kinds::OmicronZoneKind;

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = cockroachdb_zone_id_to_node_id)]
pub struct CockroachZoneIdToNodeId {
    pub omicron_zone_id: DbTypedUuid<OmicronZoneKind>,
    pub crdb_node_id: String,
}
