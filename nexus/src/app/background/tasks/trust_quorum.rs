// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Read trust quorum related tables from the database and drive configuration
//! by talking to sled-agents.

use nexus_db_queries::db::DataStore;

pub struct TrustQuorumConfigManager {
    datastore: Arc<DataStore>,
}
