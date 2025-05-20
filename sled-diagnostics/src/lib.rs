// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnostics for an Oxide sled that exposes common support commands.

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parallel_task_set::ParallelTaskSet;
use slog::Logger;

#[macro_use]
extern crate slog;

cfg_if::cfg_if! {
    if #[cfg(target_os = "illumos")] {
        mod contract;
    } else {
        mod contract_stub;
        use contract_stub as contract;
    }
}

pub mod logs;
pub use logs::{LogError, LogsHandle};

mod queries;
pub use crate::queries::{
    SledDiagnosticsCmdError, SledDiagnosticsCmdOutput,
    SledDiagnosticsCommandHttpOutput, SledDiagnosticsQueryOutput,
};
use queries::*;

/// Max number of ptool commands to run in parallel
const MAX_PTOOL_PARALLELISM: usize = 50;

/// List all zones on a sled.
pub async fn zoneadm_info()
-> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
    execute_command_with_timeout(zoneadm_list(), DEFAULT_TIMEOUT).await
}

/// Retrieve various `ipadm` command output for the system.
pub async fn ipadm_info()
-> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    let mut results = Vec::new();
    let mut commands = ParallelTaskSet::new();
    for command in
        [ipadm_show_interface(), ipadm_show_addr(), ipadm_show_prop()]
    {
        if let Some(res) = commands
            .spawn(execute_command_with_timeout(command, DEFAULT_TIMEOUT))
            .await
        {
            results.push(res);
        }
    }
    results.extend(commands.join_all().await);
    results
}

/// Retrieve various `dladm` command output for the system.
pub async fn dladm_info()
-> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    let mut results = Vec::new();
    let mut commands = ParallelTaskSet::new();
    for command in [
        dladm_show_phys(),
        dladm_show_ether(),
        dladm_show_link(),
        dladm_show_vnic(),
        dladm_show_linkprop(),
    ] {
        if let Some(res) = commands
            .spawn(execute_command_with_timeout(command, DEFAULT_TIMEOUT))
            .await
        {
            results.push(res);
        }
    }
    results.extend(commands.join_all().await);
    results
}

pub async fn nvmeadm_info()
-> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
    execute_command_with_timeout(nvmeadm_list(), DEFAULT_TIMEOUT).await
}

pub async fn pargs_oxide_processes(
    log: &Logger,
) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    // In a diagnostics context we care about looping over every pid we find,
    // but on failure we should just return a single error in a vec that
    // represents the entire failed operation.
    let pids = match contract::find_oxide_pids(log) {
        Ok(pids) => pids,
        Err(e) => return vec![Err(e.into())],
    };

    let mut results = Vec::new();
    let mut commands =
        ParallelTaskSet::new_with_parallelism(MAX_PTOOL_PARALLELISM);
    for pid in pids {
        if let Some(res) = commands
            .spawn(execute_command_with_timeout(
                pargs_process(pid),
                DEFAULT_TIMEOUT,
            ))
            .await
        {
            results.push(res);
        }
    }

    results.extend(commands.join_all().await);
    results
}

pub async fn pstack_oxide_processes(
    log: &Logger,
) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    // In a diagnostics context we care about looping over every pid we find,
    // but on failure we should just return a single error in a vec that
    // represents the entire failed operation.
    let pids = match contract::find_oxide_pids(log) {
        Ok(pids) => pids,
        Err(e) => return vec![Err(e.into())],
    };

    let mut results = Vec::new();
    let mut commands =
        ParallelTaskSet::new_with_parallelism(MAX_PTOOL_PARALLELISM);
    for pid in pids {
        if let Some(res) = commands
            .spawn(execute_command_with_timeout(
                pstack_process(pid),
                DEFAULT_TIMEOUT,
            ))
            .await
        {
            results.push(res);
        }
    }
    results.extend(commands.join_all().await);
    results
}

pub async fn pfiles_oxide_processes(
    log: &Logger,
) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    // In a diagnostics context we care about looping over every pid we find,
    // but on failure we should just return a single error in a vec that
    // represents the entire failed operation.
    let pids = match contract::find_oxide_pids(log) {
        Ok(pids) => pids,
        Err(e) => return vec![Err(e.into())],
    };

    let mut results = Vec::new();
    let mut commands =
        ParallelTaskSet::new_with_parallelism(MAX_PTOOL_PARALLELISM);
    for pid in pids {
        if let Some(res) = commands
            .spawn(execute_command_with_timeout(
                pfiles_process(pid),
                DEFAULT_TIMEOUT,
            ))
            .await
        {
            results.push(res);
        }
    }
    results.extend(commands.join_all().await);
    results
}

/// Retrieve various `zfs` command output for the system.
pub async fn zfs_info()
-> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
    execute_command_with_timeout(zfs_list(), DEFAULT_TIMEOUT).await
}

/// Retrieve various `zpool` command output for the system.
pub async fn zpool_info()
-> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
    execute_command_with_timeout(zpool_status(), DEFAULT_TIMEOUT).await
}

pub async fn health_check()
-> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    [
        uptime(),
        kstat_low_page(),
        svcs_enabled_but_not_running(),
        count_disks(),
        zfs_list_unmounted(),
        count_crucibles(),
        identify_datasets_close_to_quota(),
        identify_datasets_with_less_than_300_gib_avail(),
        dimm_check(),
    ]
        .into_iter()
        .map(|c| async move {
            execute_command_with_timeout(c, DEFAULT_TIMEOUT).await
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<Result<_, _>>>()
        .await
}
