// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::expunge_precheck::CockroachdbProblemKind;
use crate::latest::expunge_precheck::ExpungePrecheck;
use crate::latest::expunge_precheck::ExpungePrecheckResult;
use crate::latest::expunge_precheck::InventoryProblemKind;
use crate::latest::expunge_precheck::SledsProblemKind;
use crate::latest::expunge_precheck::UserDataProblemKind;
use chrono::{DateTime, Utc};

impl ExpungePrecheck {
    /// Assembles a `ExpungePrecheck` from the results of each check.
    ///
    /// This computes `ok` from the checks' problems. Callers should use this
    /// rather than constructing a `ExpungePrecheck` directly so that `ok` is
    /// always consistent with the checks.
    pub fn new(
        inventory_collection_time: DateTime<Utc>,
        inventory: ExpungePrecheckResult<InventoryProblemKind>,
        user_data: ExpungePrecheckResult<UserDataProblemKind>,
        cockroachdb: ExpungePrecheckResult<CockroachdbProblemKind>,
        sleds: ExpungePrecheckResult<SledsProblemKind>,
    ) -> Self {
        let ok = inventory.problems.is_empty()
            && user_data.problems.is_empty()
            && cockroachdb.problems.is_empty()
            && sleds.problems.is_empty();
        ExpungePrecheck {
            ok,
            inventory_collection_time,
            inventory,
            user_data,
            cockroachdb,
            sleds,
        }
    }
}
