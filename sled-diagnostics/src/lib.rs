// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnostics for an Oxide sled that exposes common support commands.

use std::sync::Arc;

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
use tokio::{sync::Semaphore, task::JoinSet};

mod queries;
pub use crate::queries::{
    SledDiagnosticsCmdError, SledDiagnosticsCmdOutput,
    SledDiagnosticsCommandHttpOutput, SledDiagnosticsQueryOutput,
};
use queries::*;

/// Max number of commands to run in parallel
const MAX_PARALLELISM: usize = 50;

trait ParallelCommandExecution {
    type Output;

    /// Add a command to the set of commands to be executed.
    fn add_command<F>(&mut self, command: F)
    where
        F: std::future::Future<Output = Self::Output> + Send + 'static;
}

struct MultipleCommands<T> {
    semaphore: Arc<Semaphore>,
    set: JoinSet<T>,
}

impl<T: 'static> MultipleCommands<T> {
    fn new() -> MultipleCommands<T> {
        let semaphore = Arc::new(Semaphore::new(MAX_PARALLELISM));
        let set = JoinSet::new();

        Self { semaphore, set }
    }

    /// Wait for all commands to execute and return their output.
    async fn join_all(self) -> Vec<T> {
        self.set.join_all().await
    }
}

impl<T> ParallelCommandExecution for MultipleCommands<T>
where
    T: Send + 'static,
{
    type Output = T;

    fn add_command<F>(&mut self, command: F)
    where
        F: std::future::Future<Output = Self::Output> + Send + 'static,
    {
        let semaphore = Arc::clone(&self.semaphore);
        let _abort_handle = self.set.spawn(async move {
            let permit =
                semaphore.acquire_owned().await.expect("semaphore acquire");
            let res = command.await;
            drop(permit);
            res
        });
    }
}

/// List all zones on a sled.
pub async fn zoneadm_info()
-> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
    execute_command_with_timeout(zoneadm_list(), DEFAULT_TIMEOUT).await
}

/// Retrieve various `ipadm` command output for the system.
pub async fn ipadm_info()
-> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    let mut commands = MultipleCommands::new();
    for command in
        [ipadm_show_interface(), ipadm_show_addr(), ipadm_show_prop()]
    {
        commands
            .add_command(execute_command_with_timeout(command, DEFAULT_TIMEOUT))
    }
    commands.join_all().await
}

/// Retrieve various `dladm` command output for the system.
pub async fn dladm_info()
-> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
    let mut commands = MultipleCommands::new();
    for command in [
        dladm_show_phys(),
        dladm_show_ether(),
        dladm_show_link(),
        dladm_show_vnic(),
        dladm_show_linkprop(),
    ] {
        commands
            .add_command(execute_command_with_timeout(command, DEFAULT_TIMEOUT))
    }
    commands.join_all().await
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

    let mut commands = MultipleCommands::new();
    for pid in pids {
        commands.add_command(execute_command_with_timeout(
            pargs_process(pid),
            DEFAULT_TIMEOUT,
        ));
    }
    commands.join_all().await
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

    let mut commands = MultipleCommands::new();
    for pid in pids {
        commands.add_command(execute_command_with_timeout(
            pstack_process(pid),
            DEFAULT_TIMEOUT,
        ));
    }
    commands.join_all().await
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

    let mut commands = MultipleCommands::new();
    for pid in pids {
        commands.add_command(execute_command_with_timeout(
            pfiles_process(pid),
            DEFAULT_TIMEOUT,
        ));
    }
    commands.join_all().await
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
