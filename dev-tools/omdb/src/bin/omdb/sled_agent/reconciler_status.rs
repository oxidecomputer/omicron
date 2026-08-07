// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb sled-agent network-config reconciler-status` subcommand

use crate::helpers::datetime_rfc3339_concise;
use anyhow::Context;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::DetermineSwitchSlotStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ReconcilerActivationReason;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ReconcilerCurrentStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ReconcilerInertReason;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ReconcilerRunningStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ReconciliationCompletedStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ScrimletReconcilersStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdNatReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdNatReconcilerStatusNatEntry;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdNatReconcilerStatusNatEntryFailure;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdPortOperationFailure;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdPortReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::lldpd::LldpdReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::MgdBfdOperationFailure;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::MgdBfdReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::MgdBgpReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::MgdBgpReconcilerStatusOpCount;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::MgdReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::MgdStaticRouteReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::uplinkd::UplinkdReconcilerStatus;
use indent_write::fmt::IndentWriter;
use std::fmt;
use std::fmt::Write;

const INDENT: &str = "    ";

/// Runs `omdb sled-agent network-config reconciler-status`
pub(super) async fn cmd_network_config_reconciler_status(
    client: &bootstrap_agent_lockstep_client::Client,
) -> anyhow::Result<()> {
    let status = client
        .scrimlet_reconcilers_status_for_debug()
        .await
        .context("failed to fetch reconciler status")?
        .into_inner();
    println!("{}", ScrimletReconcilersStatusDisplay(&status));
    Ok(())
}

// The remainder of this module is `Display` adapters for formatting all the
// details in the scrimlet reconcilers status structure returned by the
// bootstrap agent lockstep API.

struct ScrimletReconcilersStatusDisplay<'a>(&'a ScrimletReconcilersStatus);

impl fmt::Display for ScrimletReconcilersStatusDisplay<'_> {
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo => {
                write!(
                    f,
                    "not running: sled-agent has not yet initialized \
                     the reconciler subsystem with networking information"
                )
            }
            ScrimletReconcilersStatus::DeterminingSwitchSlot(status) => {
                match status {
                    DetermineSwitchSlotStatus::NotScrimlet => {
                        write!(f, "not running: no switch detected")
                    }
                    DetermineSwitchSlotStatus::ContactingMgs {
                        prev_attempt_err,
                    } => {
                        write!(
                            f,
                            "not yet running: attempting to contact MGS \
                             to determine our location"
                        )?;
                        if let Some(err) = prev_attempt_err {
                            write!(
                                f,
                                " (previous MGS contact attempt failed: {err})"
                            )?;
                        }
                        Ok(())
                    }
                    DetermineSwitchSlotStatus::WaitingToRetry {
                        prev_attempt_err,
                    } => {
                        write!(
                            f,
                            "not yet running: sleeping before retrying \
                             contacting MGS to determine our location \
                             (previous MGS contact attempt failed: \
                              {prev_attempt_err})"
                        )
                    }
                }
            }
            ScrimletReconcilersStatus::Running {
                dpd_reconciler,
                lldpd_reconciler,
                mgd_reconciler,
                uplinkd_reconciler,
            } => {
                let reconcilers = [
                    (
                        "dpd",
                        &ReconcilerStatusDisplay(&dpd_reconciler)
                            as &dyn fmt::Display,
                    ),
                    ("mgd", &ReconcilerStatusDisplay(&mgd_reconciler)),
                    ("lldpd", &ReconcilerStatusDisplay(&lldpd_reconciler)),
                    ("uplinkd", &ReconcilerStatusDisplay(&uplinkd_reconciler)),
                ];
                let mut reconcilers = reconcilers.iter().peekable();
                while let Some((name, displayable)) = reconcilers.next() {
                    writeln!(f, "{name} reconciler:")?;
                    let mut f = IndentWriter::new(INDENT, &mut f);
                    if reconcilers.peek().is_some() {
                        writeln!(f, "{displayable}")?;
                    } else {
                        write!(f, "{displayable}")?;
                    }
                }
                Ok(())
            }
        }
    }
}

trait DisplayableStatus {
    type DisplayAdapter<'a>: fmt::Display
    where
        Self: 'a;

    fn display<'a>(&'a self) -> Self::DisplayAdapter<'a>;
}

struct ReconcilerStatusDisplay<'a, T>(&'a ReconcilerStatus<T>);

impl<T> fmt::Display for ReconcilerStatusDisplay<'_, T>
where
    T: DisplayableStatus,
{
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ReconcilerStatus { current_status, last_completion } = self.0;
        writeln!(f, "current status:")?;
        writeln!(
            IndentWriter::new(INDENT, &mut f),
            "{}",
            ReconcilerCurrentStatusDisplay(&current_status),
        )?;

        if let Some(last_completion) = last_completion {
            writeln!(f, "last completion:")?;
            writeln!(
                IndentWriter::new(INDENT, f),
                "{}",
                ReconciliationCompletedStatusDisplay(&last_completion)
            )?;
        } else {
            writeln!(f, "last completion: none")?;
        }
        Ok(())
    }
}

fn reason_description(r: &ReconcilerActivationReason) -> &'static str {
    match r {
        ReconcilerActivationReason::Startup => "first execution on start",
        ReconcilerActivationReason::PeriodicTimer => "periodic timer fired",
        ReconcilerActivationReason::SystemNetworkingConfigChanged => {
            "networking config changed"
        }
        ReconcilerActivationReason::ScrimletStatusChanged => {
            "switch presence changed"
        }
    }
}

struct ReconcilerCurrentStatusDisplay<'a>(&'a ReconcilerCurrentStatus);

impl fmt::Display for ReconcilerCurrentStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ReconcilerCurrentStatus::Inert(reason) => match reason {
                ReconcilerInertReason::NoLongerAScrimlet => {
                    write!(f, "inert: switch no longer present")
                }
                ReconcilerInertReason::TaskExitedUnexpectedly => write!(
                    f,
                    "inert: task exited unexpectedly \
                     (this should be impossible!)"
                ),
            },
            ReconcilerCurrentStatus::Running(ReconcilerRunningStatus {
                activation_reason,
                started_at_time,
                running_for,
            }) => {
                writeln!(f, "currently running:")?;
                let mut f = IndentWriter::new(INDENT, f);
                writeln!(
                    f,
                    "activation reason: {}",
                    reason_description(&activation_reason),
                )?;
                writeln!(
                    f,
                    "started at {}",
                    datetime_rfc3339_concise(started_at_time)
                )?;
                write!(f, "running for {running_for:?}")
            }
            ReconcilerCurrentStatus::Idle => write!(f, "idle"),
        }
    }
}

struct ReconciliationCompletedStatusDisplay<'a, T>(
    &'a ReconciliationCompletedStatus<T>,
);

impl<T> fmt::Display for ReconciliationCompletedStatusDisplay<'_, T>
where
    T: DisplayableStatus,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ReconciliationCompletedStatus {
            activation_reason,
            completed_at_time,
            ran_for,
            activation_count,
            status,
        } = self.0;
        writeln!(
            f,
            "activation reason: {}",
            reason_description(&activation_reason)
        )?;
        writeln!(f, "activation count: {activation_count}")?;
        writeln!(
            f,
            "completed at {}",
            datetime_rfc3339_concise(completed_at_time)
        )?;
        writeln!(f, "ran for {ran_for:?}")?;
        writeln!(f, "detailed status:")?;
        write!(IndentWriter::new(INDENT, f), "{}", status.display())
    }
}

impl DisplayableStatus for LldpdReconcilerStatus {
    type DisplayAdapter<'a>
        = LldpdReconcilerStatusDisplay<'a>
    where
        Self: 'a;

    /// Get a `fmt::Display`-able version of this status (e.g., for `omdb`).
    fn display(&self) -> LldpdReconcilerStatusDisplay<'_> {
        LldpdReconcilerStatusDisplay(self)
    }
}

struct LldpdReconcilerStatusDisplay<'a>(&'a LldpdReconcilerStatus);

impl fmt::Display for LldpdReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            LldpdReconcilerStatus::Failed(reason) => {
                write!(f, "reconciliation failed: {reason}")
            }
            LldpdReconcilerStatus::SkippedConfigUpToDate => {
                write!(f, "reconciliation skipped: config is up to date")
            }
            LldpdReconcilerStatus::Reconciled { ports } => {
                let plural = if ports.len() == 1 { "" } else { "s" };
                write!(
                    f,
                    "successfully reconciled {} port{plural}",
                    ports.len()
                )?;

                if !ports.is_empty() {
                    writeln!(f, ":")?;
                    let mut w = IndentWriter::new(INDENT, f);
                    let mut ports = ports.iter().peekable();
                    while let Some((port, status)) = ports.next() {
                        let s = format_args!("* {port}: {status:?}");
                        if ports.peek().is_some() {
                            writeln!(w, "{s}")?;
                        } else {
                            write!(w, "{s}")?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

impl DisplayableStatus for UplinkdReconcilerStatus {
    type DisplayAdapter<'a>
        = UplinkdReconcilerStatusDisplay<'a>
    where
        Self: 'a;

    /// Get a `fmt::Display`-able version of this status (e.g., for `omdb`).
    fn display(&self) -> UplinkdReconcilerStatusDisplay<'_> {
        UplinkdReconcilerStatusDisplay(self)
    }
}

struct UplinkdReconcilerStatusDisplay<'a>(&'a UplinkdReconcilerStatus);

impl fmt::Display for UplinkdReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            UplinkdReconcilerStatus::Failed(reason) => {
                write!(f, "reconciliation failed: {reason}")
            }
            UplinkdReconcilerStatus::SkippedConfigUpToDate => {
                write!(f, "reconciliation skipped: config is up to date")
            }
            UplinkdReconcilerStatus::Reconciled { ports } => {
                let plural = if ports.len() == 1 { "" } else { "s" };
                write!(
                    f,
                    "successfully reconciled {} port{plural}",
                    ports.len()
                )?;

                if !ports.is_empty() {
                    writeln!(f, ":")?;
                    let mut w = IndentWriter::new(INDENT, f);
                    let mut ports = ports.iter().peekable();
                    while let Some((port, values)) = ports.next() {
                        let s = format_args!("* {port}: {}", values.join(", "));
                        if ports.peek().is_some() {
                            writeln!(w, "{s}")?;
                        } else {
                            write!(w, "{s}")?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

impl DisplayableStatus for DpdReconcilerStatus {
    type DisplayAdapter<'a>
        = DpdReconcilerStatusDisplay<'a>
    where
        Self: 'a;

    fn display<'a>(&'a self) -> Self::DisplayAdapter<'a> {
        DpdReconcilerStatusDisplay(self)
    }
}

struct DpdReconcilerStatusDisplay<'a>(&'a DpdReconcilerStatus);

impl fmt::Display for DpdReconcilerStatusDisplay<'_> {
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let DpdReconcilerStatus { port_settings_status, nat_status } = self.0;
        writeln!(f, "port settings:")?;
        writeln!(
            IndentWriter::new(INDENT, &mut f),
            "{}",
            DpdPortReconcilerStatusDisplay(&port_settings_status),
        )?;
        writeln!(f, "NAT:")?;
        write!(
            IndentWriter::new(INDENT, &mut f),
            "{}",
            DpdNatReconcilerStatusDisplay(&nat_status)
        )
    }
}

struct DpdPortReconcilerStatusDisplay<'a>(&'a DpdPortReconcilerStatus);

impl fmt::Display for DpdPortReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            DpdPortReconcilerStatus::FailedReadingCurrentSettings(err) => {
                write!(f, "failed to read current dpd settings: {err}")
            }
            DpdPortReconcilerStatus::FailedGeneratingPlan(err) => {
                write!(f, "failed to generate reconciliation plan: {err}")
            }
            DpdPortReconcilerStatus::Complete {
                unchanged,
                cleared,
                clear_failures,
                applied,
                apply_failures,
            } => {
                writeln!(f, "reconciliation completed")?;
                let mut f = IndentWriter::new(INDENT, f);
                if !unchanged.is_empty() {
                    writeln!(
                        f,
                        "ports unchanged: {}",
                        unchanged
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ")
                    )?;
                }
                if !cleared.is_empty() {
                    writeln!(
                        f,
                        "ports cleared: {}",
                        cleared.iter().cloned().collect::<Vec<_>>().join(", ")
                    )?;
                }
                if !applied.is_empty() {
                    writeln!(
                        f,
                        "ports applied: {}",
                        applied.iter().cloned().collect::<Vec<_>>().join(", ")
                    )?;
                }
                if clear_failures.is_empty() {
                    writeln!(f, "clear failures: none")?;
                } else {
                    writeln!(f, "clear failures:")?;
                    let mut f = IndentWriter::new(INDENT, &mut f);
                    for DpdPortOperationFailure { port_id, error } in
                        clear_failures
                    {
                        writeln!(f, "* {port_id}: {error}")?;
                    }
                }
                if apply_failures.is_empty() {
                    write!(f, "apply failures: none")?;
                } else {
                    writeln!(f, "apply failures:")?;
                    let mut f = IndentWriter::new(INDENT, &mut f);
                    let mut apply_failures = apply_failures.iter().peekable();
                    while let Some(DpdPortOperationFailure { port_id, error }) =
                        apply_failures.next()
                    {
                        let s = format_args!("* {port_id}: {error}");
                        if apply_failures.peek().is_some() {
                            writeln!(f, "{s}")?;
                        } else {
                            write!(f, "{s}")?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

struct DpdNatReconcilerStatusNatEntryDisplay<'a>(
    &'a DpdNatReconcilerStatusNatEntry,
);

impl fmt::Display for DpdNatReconcilerStatusNatEntryDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let DpdNatReconcilerStatusNatEntry {
            external_ip,
            first_port,
            last_port,
        } = self.0;
        write!(f, "{external_ip} (ports {first_port}-{last_port})")
    }
}

struct DpdNatReconcilerStatusDisplay<'a>(&'a DpdNatReconcilerStatus);

impl fmt::Display for DpdNatReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            DpdNatReconcilerStatus::NoNatEntriesConfig => {
                write!(f, "skipped: no NAT entries in bootstore")
            }
            DpdNatReconcilerStatus::FailedReadingCurrentDpdNatEntries(err) => {
                write!(f, "failed to read current dpd settings: {err}")
            }
            DpdNatReconcilerStatus::InvalidSystemNetworkingConfig(err) => {
                write!(f, "bootstore config is INVALID: {err}")
            }
            DpdNatReconcilerStatus::Complete {
                unchanged,
                removed,
                remove_failures,
                created,
                create_failures,
            } => {
                writeln!(f, "reconciliation completed")?;
                let mut f = IndentWriter::new(INDENT, f);
                if !unchanged.is_empty() {
                    writeln!(
                        f,
                        "zones unchanged: {}",
                        unchanged
                            .iter()
                            .map(|id| id.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )?;
                }
                if !removed.is_empty() {
                    writeln!(
                        f,
                        "NAT entries removed: {}",
                        removed
                            .iter()
                            .map(|entry| DpdNatReconcilerStatusNatEntryDisplay(
                                &entry
                            )
                            .to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )?;
                }
                if !created.is_empty() {
                    writeln!(
                        f,
                        "NAT entries created: {}",
                        created
                            .iter()
                            .map(|(id, entry)| format!(
                                "zone {id}: {}",
                                DpdNatReconcilerStatusNatEntryDisplay(&entry)
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )?;
                }
                if remove_failures.is_empty() {
                    writeln!(f, "remove failures: none")?;
                } else {
                    writeln!(f, "remove failures:")?;
                    let mut f = IndentWriter::new(INDENT, &mut f);
                    for DpdNatReconcilerStatusNatEntryFailure {
                        entry,
                        error,
                    } in remove_failures
                    {
                        writeln!(
                            f,
                            "* {}: {error}",
                            DpdNatReconcilerStatusNatEntryDisplay(&entry)
                        )?;
                    }
                }
                if create_failures.is_empty() {
                    write!(f, "create failures: none")?;
                } else {
                    writeln!(f, "create failures:")?;
                    let mut f = IndentWriter::new(INDENT, &mut f);
                    let mut create_failures = create_failures.iter().peekable();
                    while let Some((id, failure)) = create_failures.next() {
                        let DpdNatReconcilerStatusNatEntryFailure {
                            entry,
                            error,
                        } = failure;
                        let s = format_args!(
                            "* zone {id}, {}: {error}",
                            DpdNatReconcilerStatusNatEntryDisplay(&entry),
                        );
                        if create_failures.peek().is_some() {
                            writeln!(f, "{s}")?;
                        } else {
                            write!(f, "{s}")?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

impl DisplayableStatus for MgdReconcilerStatus {
    type DisplayAdapter<'a>
        = MgdReconcilerStatusDisplay<'a>
    where
        Self: 'a;

    fn display<'a>(&'a self) -> Self::DisplayAdapter<'a> {
        MgdReconcilerStatusDisplay(self)
    }
}

struct MgdReconcilerStatusDisplay<'a>(&'a MgdReconcilerStatus);

impl fmt::Display for MgdReconcilerStatusDisplay<'_> {
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let MgdReconcilerStatus {
            bfd_status,
            bgp_status,
            static_routes_status,
        } = self.0;
        writeln!(f, "static routes:")?;
        writeln!(
            IndentWriter::new(INDENT, &mut f),
            "{}",
            MgdStaticRouteReconcilerStatusDisplay(&static_routes_status)
        )?;
        writeln!(f, "BGP:")?;
        writeln!(
            IndentWriter::new(INDENT, &mut f),
            "{}",
            MgdBgpReconcilerStatusDisplay(&bgp_status)
        )?;
        writeln!(f, "BFD:")?;
        write!(
            IndentWriter::new(INDENT, &mut f),
            "{}",
            MgdBfdReconcilerStatusDisplay(&bfd_status)
        )
    }
}

struct MgdBfdReconcilerStatusDisplay<'a>(&'a MgdBfdReconcilerStatus);

impl fmt::Display for MgdBfdReconcilerStatusDisplay<'_> {
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            MgdBfdReconcilerStatus::FailedReadingBfdPeers(err) => {
                write!(f, "failed to read current bfd peers: {err}")
            }
            MgdBfdReconcilerStatus::Complete {
                unchanged,
                remove_success,
                remove_failure,
                add_success,
                add_failure,
            } => {
                writeln!(f, "reconciliation completed")?;
                let mut f = IndentWriter::new(INDENT, f);
                if !unchanged.is_empty() {
                    writeln!(
                        f,
                        "peers unchanged: {}",
                        unchanged
                            .iter()
                            .map(|ip| ip.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )?;
                }
                if !add_success.is_empty() {
                    writeln!(
                        f,
                        "peers added: {}",
                        add_success
                            .iter()
                            .map(|ip| ip.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )?;
                }
                if !remove_success.is_empty() {
                    writeln!(
                        f,
                        "peers removed: {}",
                        remove_success
                            .iter()
                            .map(|ip| ip.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )?;
                }
                if remove_failure.is_empty() {
                    writeln!(f, "remove failures: none")?;
                } else {
                    writeln!(f, "remove failures:")?;
                    let mut f = IndentWriter::new(INDENT, &mut f);
                    for MgdBfdOperationFailure { peer, error } in remove_failure
                    {
                        writeln!(f, "* {peer}: {error}")?;
                    }
                }
                if add_failure.is_empty() {
                    write!(f, "add failures: none")?;
                } else {
                    writeln!(f, "add failures:")?;
                    let mut f = IndentWriter::new(INDENT, &mut f);
                    let mut add_failure = add_failure.iter().peekable();
                    while let Some(MgdBfdOperationFailure { peer, error }) =
                        add_failure.next()
                    {
                        let s = format_args!("* {peer}: {error}");
                        if add_failure.peek().is_some() {
                            writeln!(f, "{s}")?;
                        } else {
                            write!(f, "{s}")?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

struct MgdBgpReconcilerStatusOpCountDisplay<'a>(
    &'a MgdBgpReconcilerStatusOpCount,
);

impl fmt::Display for MgdBgpReconcilerStatusOpCountDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let MgdBgpReconcilerStatusOpCount {
            routers_deleted,
            routers_updated,
            routers_created,
            origin4_deleted,
            origin4_updated,
            origin4_created,
            origin6_deleted,
            origin6_updated,
            origin6_created,
            shapers_deleted,
            shapers_updated,
            shapers_created,
            checkers_deleted,
            checkers_updated,
            checkers_created,
            numbered_peers_deleted,
            numbered_peers_updated,
            numbered_peers_created,
            unnumbered_peers_deleted,
            unnumbered_peers_updated,
            unnumbered_peers_created,
        } = self.0;
        let lines = [
            ("routers", routers_created, routers_updated, routers_deleted),
            ("origin4", origin4_created, origin4_updated, origin4_deleted),
            ("origin6", origin6_created, origin6_updated, origin6_deleted),
            ("shapers", shapers_created, shapers_updated, shapers_deleted),
            ("checkers", checkers_created, checkers_updated, checkers_deleted),
            (
                "numbered peers",
                numbered_peers_created,
                numbered_peers_updated,
                numbered_peers_deleted,
            ),
            (
                "unnumbered peers",
                unnumbered_peers_created,
                unnumbered_peers_updated,
                unnumbered_peers_deleted,
            ),
        ];
        let mut lines = lines.iter().peekable();
        while let Some((name, created, updated, deleted)) = lines.next() {
            let s = format_args!(
                "{name} created / updated / deleted: \
                 {created} / {updated} / {deleted}"
            );
            if lines.peek().is_some() {
                writeln!(f, "{s}")?;
            } else {
                write!(f, "{s}")?;
            }
        }
        Ok(())
    }
}

struct MgdBgpReconcilerStatusDisplay<'a>(&'a MgdBgpReconcilerStatus);

impl fmt::Display for MgdBgpReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            MgdBgpReconcilerStatus::FailedReadingBgpConfig(err) => {
                write!(f, "failed to read current bgp config: {err}")
            }
            MgdBgpReconcilerStatus::FailedGeneratingDesiredConfig(err) => {
                write!(f, "failed to generate desired bgp config: {err}")
            }
            MgdBgpReconcilerStatus::Complete {
                counts,
                did_change_max_paths,
                errors,
            } => {
                writeln!(f, "reconciliation completed")?;
                let mut f = IndentWriter::new(INDENT, f);
                writeln!(f, "did change max paths: {did_change_max_paths}")?;
                writeln!(
                    f,
                    "{}",
                    MgdBgpReconcilerStatusOpCountDisplay(&counts)
                )?;
                if errors.is_empty() {
                    write!(f, "errors: none")?;
                } else {
                    writeln!(f, "errors:")?;
                    let mut f = IndentWriter::new(INDENT, f);
                    let mut errors = errors.iter().peekable();
                    while let Some(err) = errors.next() {
                        let s = format_args!("* {err}");
                        if errors.peek().is_some() {
                            writeln!(f, "{s}")?;
                        } else {
                            write!(f, "{s}")?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

struct MgdStaticRouteReconcilerStatusDisplay<'a>(
    &'a MgdStaticRouteReconcilerStatus,
);

impl fmt::Display for MgdStaticRouteReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            MgdStaticRouteReconcilerStatus::FailedReadingStaticRoutes(err) => {
                write!(f, "failed to read current static routes: {err}")
            }
            MgdStaticRouteReconcilerStatus::FailedGeneratingPlan(err) => {
                write!(f, "failed to generate reconciliation plan: {err}")
            }
            MgdStaticRouteReconcilerStatus::Complete {
                unchanged,
                delete_v4_result,
                delete_v6_result,
                add_v4_result,
                add_v6_result,
            } => {
                writeln!(f, "reconciliation completed")?;
                let mut f = IndentWriter::new(INDENT, f);

                writeln!(f, "routes unchanged: {unchanged}")?;

                if let (Ok(deleted), Ok(added)) =
                    (delete_v4_result, add_v4_result)
                {
                    writeln!(
                        f,
                        "v4 routes deleted / added: {deleted} / {added}"
                    )?;
                } else {
                    match delete_v4_result {
                        Ok(n) => {
                            writeln!(f, "v4 routes deleted: {n}")?;
                        }
                        Err(err) => {
                            writeln!(f, "failed to delete v4 routes: {err}")?;
                        }
                    }
                    match add_v4_result {
                        Ok(n) => {
                            writeln!(f, "v4 routes added: {n}")?;
                        }
                        Err(err) => {
                            writeln!(f, "failed to add v4 routes: {err}")?;
                        }
                    }
                }

                if let (Ok(deleted), Ok(added)) =
                    (delete_v6_result, add_v6_result)
                {
                    write!(
                        f,
                        "v6 routes deleted / added: {deleted} / {added}"
                    )?;
                } else {
                    match delete_v6_result {
                        Ok(n) => {
                            writeln!(f, "v6 routes deleted: {n}")?;
                        }
                        Err(err) => {
                            writeln!(f, "failed to delete v6 routes: {err}")?;
                        }
                    }
                    match add_v6_result {
                        Ok(n) => {
                            write!(f, "v6 routes added: {n}")?;
                        }
                        Err(err) => {
                            write!(f, "failed to add v6 routes: {err}")?;
                        }
                    }
                }

                Ok(())
            }
        }
    }
}
