// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnostics for an Oxide sled that exposes common support commands.

use std::collections::BTreeSet;

use futures::{stream::FuturesUnordered, StreamExt};
use slog::Logger;

#[cfg(target_os = "illumos")]
mod contract;

mod queries;
pub use crate::queries::{
    SledDiagnosticsCmdError, SledDiagnosticsCmdOutput,
    SledDiagnosticsCommandHttpOutput,
};
use queries::*;

/// List all zones on a sled.
pub async fn zoneadm_info(
) -> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
    execute_command_with_timeout(zoneadm_list(), DEFAULT_TIMEOUT).await
}

/// Retrieve various `ipadm` command output for the system.
pub async fn ipadm_info(
) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    [ipadm_show_interface(), ipadm_show_addr(), ipadm_show_prop()]
        .into_iter()
        .map(|c| async move {
            execute_command_with_timeout(c, DEFAULT_TIMEOUT).await
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<Result<_, _>>>()
        .await
}

/// Retrieve various `dladm` command output for the system.
pub async fn dladm_info(
) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    [
        dladm_show_phys(),
        dladm_show_ether(),
        dladm_show_link(),
        dladm_show_vnic(),
        dladm_show_linkprop(),
    ]
        .into_iter()
        .map(|c| async move {
            execute_command_with_timeout(c, DEFAULT_TIMEOUT).await
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<Result<_, _>>>()
        .await
}

#[allow(unused_variables)]
fn find_oxide_pids(
    log: &Logger,
) -> Result<BTreeSet<i32>, SledDiagnosticsCmdError> {
    #[cfg(target_os = "illumos")]
    return contract::find_oxide_pids(log)
        .map_err(SledDiagnosticsCmdError::Contract);
    #[cfg(not(target_os = "illumos"))]
    return Ok(BTreeSet::new());
}

pub async fn pargs_oxide_processes(
    log: &Logger,
) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    // In a diagnostics context we care about looping over every pid we find,
    // but on failure we should just return a single error in a vec that
    // represents the entire failed operation.
    let pids = match find_oxide_pids(log) {
        Ok(pids) => pids,
        Err(e) => return vec![Err(e)],
    };

    pids.iter()
        .map(|pid| pargs_process(*pid))
        .map(|c| async move {
            execute_command_with_timeout(c, DEFAULT_TIMEOUT).await
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<Result<_, _>>>()
        .await
}

pub async fn pstack_oxide_processes(
    log: &Logger,
) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    // In a diagnostics context we care about looping over every pid we find,
    // but on failure we should just return a single error in a vec that
    // represents the entire failed operation.
    let pids = match find_oxide_pids(log) {
        Ok(pids) => pids,
        Err(e) => return vec![Err(e)],
    };

    pids.iter()
        .map(|pid| pstack_process(*pid))
        .map(|c| async move {
            execute_command_with_timeout(c, DEFAULT_TIMEOUT).await
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<Result<_, _>>>()
        .await
}
