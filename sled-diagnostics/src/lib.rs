// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnostics for an Oxide sled that exposes common support commands.

use futures::{stream::FuturesUnordered, StreamExt};

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
        .collect::<Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>>>()
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
        .collect::<Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>>>()
        .await
}
