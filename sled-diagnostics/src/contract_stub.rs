//! Stub implementation for platfroms without libcontract(3lib).

use std::collections::BTreeSet;

use slog::{warn, Logger};
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
