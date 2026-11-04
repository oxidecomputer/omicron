// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! APIs to cleanup networking utilities

use anyhow::Error;
use futures::stream::{self, StreamExt, TryStreamExt};
use illumos_utils::ExecutionError;
use illumos_utils::dladm::Api;
use illumos_utils::dladm::BOOTSTRAP_ETHERSTUB_NAME;
use illumos_utils::dladm::BOOTSTRAP_ETHERSTUB_VNIC_NAME;
use illumos_utils::dladm::Dladm;
use illumos_utils::dladm::UNDERLAY_ETHERSTUB_NAME;
use illumos_utils::dladm::UNDERLAY_ETHERSTUB_VNIC_NAME;
use illumos_utils::link::LinkKind;
use illumos_utils::opte;
use illumos_utils::zone::IPADM;
use illumos_utils::{PFEXEC, execute};
use slog::Logger;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::process::Command;

pub fn delete_underlay_addresses(log: &Logger) -> Result<(), Error> {
    let underlay_prefix = format!("{}/", UNDERLAY_ETHERSTUB_VNIC_NAME);
    delete_addresses_matching_prefixes(log, &[underlay_prefix])
}

pub fn delete_bootstrap_addresses(log: &Logger) -> Result<(), Error> {
    let bootstrap_prefix = format!("{}/", BOOTSTRAP_ETHERSTUB_VNIC_NAME);
    delete_addresses_matching_prefixes(log, &[bootstrap_prefix])
}

fn delete_addresses_matching_prefixes(
    log: &Logger,
    prefixes: &[String],
) -> Result<(), Error> {
    use std::io::BufRead;
    let mut cmd = Command::new(PFEXEC);
    let cmd = cmd.args(&[IPADM, "show-addr", "-p", "-o", "ADDROBJ"]);
    let output = execute(cmd)?;

    // `ipadm show-addr` can return multiple addresses with the same name, but
    // multiple values. Collecting to a set ensures that only a single name is
    // used.
    let addrobjs = output
        .stdout
        .lines()
        .filter_map(|line| match line {
            Ok(line) => Some(line),
            Err(err) => {
                warn!(
                    log,
                    "ipadm show-addr returned line that wasn't valid UTF-8";
                    InlineErrorChain::new(&err),
                );
                None
            }
        })
        .collect::<std::collections::HashSet<_>>();

    for addrobj in addrobjs {
        if prefixes.iter().any(|prefix| addrobj.starts_with(prefix)) {
            warn!(
                log,
                "Deleting existing Omicron IP address";
                "addrobj" => addrobj.as_str(),
            );
            let mut cmd = Command::new(PFEXEC);
            let cmd = cmd.args(&[IPADM, "delete-addr", addrobj.as_str()]);
            execute(cmd)?;
        }
    }
    Ok(())
}

/// Delete the etherstub and underlay VNIC used for interzone communication
pub async fn delete_etherstub(log: &Logger) -> Result<(), ExecutionError> {
    warn!(log, "Deleting Omicron underlay VNIC"; "vnic_name" => UNDERLAY_ETHERSTUB_VNIC_NAME);
    Dladm::delete_etherstub_vnic(UNDERLAY_ETHERSTUB_VNIC_NAME).await?;
    warn!(log, "Deleting Omicron underlay etherstub"; "stub_name" => UNDERLAY_ETHERSTUB_NAME);
    Dladm::delete_etherstub(UNDERLAY_ETHERSTUB_NAME).await?;
    warn!(log, "Deleting Omicron bootstrap VNIC"; "vnic_name" => BOOTSTRAP_ETHERSTUB_VNIC_NAME);
    Dladm::delete_etherstub_vnic(BOOTSTRAP_ETHERSTUB_VNIC_NAME).await?;
    warn!(log, "Deleting Omicron bootstrap etherstub"; "stub_name" => BOOTSTRAP_ETHERSTUB_NAME);
    Dladm::delete_etherstub(BOOTSTRAP_ETHERSTUB_NAME).await?;
    Ok(())
}

/// Delete all VNICs that can be managed by the control plane.
///
/// These are currently those that match the prefix `ox` or `vopte`.
pub async fn delete_omicron_vnics(log: &Logger) -> Result<(), Error> {
    let vnics = Dladm::get_vnics().await?;
    stream::iter(vnics)
        .zip(stream::iter(std::iter::repeat(log.clone())))
        .map(Ok::<_, illumos_utils::dladm::DeleteVnicError>)
        .try_for_each_concurrent(None, |(vnic, log)| async move {
            warn!(
              log,
              "Deleting existing VNIC";
                "vnic_name" => &vnic,
                "vnic_kind" => ?LinkKind::from_name(&vnic).unwrap(),
            );
            Dladm::real_api().delete_vnic(&vnic).await
        })
        .await?;
    Ok(())
}

pub async fn cleanup_networking_resources(log: &Logger) -> Result<(), Error> {
    delete_underlay_addresses(log)?;
    delete_bootstrap_addresses(log)?;
    delete_omicron_vnics(log).await?;
    delete_etherstub(log).await?;
    opte::delete_all_xde_devices(log)?;

    Ok(())
}
