// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::expunge_precheck::CockroachdbPrecheck;
use crate::latest::expunge_precheck::ExpungePrecheck;
use crate::latest::expunge_precheck::InventoryPrecheck;
use crate::latest::expunge_precheck::SledsPrecheck;
use crate::latest::expunge_precheck::UserDataPrecheck;

impl ExpungePrecheck {
    /// Assembles an `ExpungePrecheck` from the results of each check.
    ///
    /// This computes `ok` from the checks' problems. Callers should use this
    /// rather than constructing an `ExpungePrecheck` directly so that `ok` is
    /// always consistent with the checks.
    pub fn new(
        inventory: InventoryPrecheck,
        user_data: UserDataPrecheck,
        cockroachdb: CockroachdbPrecheck,
        sleds: SledsPrecheck,
    ) -> Self {
        let ok = inventory.problems.is_empty()
            && user_data.problems.is_empty()
            && cockroachdb.problems.is_empty()
            && sleds.problems.is_empty();
        ExpungePrecheck { ok, inventory, user_data, cockroachdb, sleds }
    }
}
