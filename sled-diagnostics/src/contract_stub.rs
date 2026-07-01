// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Stub implementation for platfroms without libcontract(3lib).

use std::collections::BTreeSet;

use slog::{Logger, warn};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ContractError {}

pub fn find_oxide_pids(log: &Logger) -> Result<BTreeSet<i32>, ContractError> {
    warn!(
        log,
        "Unable to find oxide pids on a non illumos platform, \
        returning empty set"
    );
    Ok(BTreeSet::new())
}
