use contract::find_oxide_pids;
use futures::{stream::FuturesUnordered, StreamExt};

pub mod command;
mod contract;
use command::{
    execute_command_with_timeout, ipadm_show_addr, ipadm_show_interface,
    ipadm_show_prop, pargs_process, pstack_process, zoneadm_list,
    DEFAULT_TIMEOUT,
};
pub use command::{SupportBundleCmdError, SupportBundleCmdOutput};

// mod contract;

/// List all zones on a sled.
pub async fn zoneadm_info(
) -> Result<SupportBundleCmdOutput, SupportBundleCmdError> {
    execute_command_with_timeout(zoneadm_list(), DEFAULT_TIMEOUT).await
}

/// Retrieve various `ipadm` command output for the system.
pub async fn ipadm_info(
) -> Vec<Result<SupportBundleCmdOutput, SupportBundleCmdError>> {
    [ipadm_show_interface(), ipadm_show_addr(), ipadm_show_prop()]
        .into_iter()
        .map(|c| async move {
            execute_command_with_timeout(c, DEFAULT_TIMEOUT).await
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<Result<SupportBundleCmdOutput, SupportBundleCmdError>>>()
        .await
}

pub async fn pargs_oxide_processes(
) -> Vec<Result<SupportBundleCmdOutput, SupportBundleCmdError>> {
    find_oxide_pids()
        .unwrap()
        .iter()
        .map(|pid| pargs_process(*pid))
        .map(|c| async move {
            execute_command_with_timeout(c, DEFAULT_TIMEOUT).await
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<Result<SupportBundleCmdOutput, SupportBundleCmdError>>>()
        .await
}

pub async fn pstack_oxide_processes(
) -> Vec<Result<SupportBundleCmdOutput, SupportBundleCmdError>> {
    find_oxide_pids()
        .unwrap()
        .iter()
        .map(|pid| pstack_process(*pid))
        .map(|c| async move {
            execute_command_with_timeout(c, DEFAULT_TIMEOUT).await
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<Result<SupportBundleCmdOutput, SupportBundleCmdError>>>()
        .await
}
