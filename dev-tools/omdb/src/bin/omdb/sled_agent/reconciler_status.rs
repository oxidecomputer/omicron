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
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ddmd::DdmdReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdNatReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdNatReconcilerStatusNatEntry;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdNatReconcilerStatusNatEntryFailure;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdPortOperationFailure;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdPortReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::DpdReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::lldpd::LldpdReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::MgdReconcilerStatus;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::uplinkd::UplinkdReconcilerStatus;
use indent_write::fmt::IndentWriter;
use std::fmt;
use std::fmt::Write;

/// Runs `omdb sled-agent network-config reconciler-status`
pub(super) async fn cmd_network_config_reconciler_status(
    client: &bootstrap_agent_lockstep_client::Client,
) -> anyhow::Result<()> {
    let status = client
        .scrimlet_reconcilers_status_for_debug()
        .await
        .context("failed to fetch reconciler status")?
        .into_inner();
    let mut out = String::new();
    write_block(&mut out, ScrimletReconcilersStatusDisplay(&status))
        .expect("writing to a String always succeeds");
    print!("{out}");
    Ok(())
}

// Indentation level for each sub-section.
const INDENT: &str = "    ";

/// A `Write` wrapper which remembers whether the writer is at the start of a
/// line.
struct NewlineTerminated<W> {
    inner: W,
    at_line_start: bool,
}

impl<W: fmt::Write> NewlineTerminated<W> {
    /// Create a new wrapper assuming the writer is already at a line start.
    fn new(inner: W) -> Self {
        Self { inner, at_line_start: true }
    }

    fn finish_line(&mut self) -> fmt::Result {
        if self.at_line_start { Ok(()) } else { self.write_char('\n') }
    }
}

impl<W: fmt::Write> fmt::Write for NewlineTerminated<W> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.inner.write_str(s)?;
        if !s.is_empty() {
            self.at_line_start = s.ends_with('\n');
        }
        Ok(())
    }
}

/// Write a nested `Display` adapter and leave exactly one newline after it,
/// whether or not the adapter ended a line itself.
fn write_block<W: fmt::Write>(f: W, block: impl fmt::Display) -> fmt::Result {
    let mut f = NewlineTerminated::new(f);
    write!(f, "{block}")?;
    f.finish_line()
}

/// Write a sequence of items, each ending in exactly one newline.
fn write_lines<W: fmt::Write, I, T, F>(
    f: W,
    items: I,
    mut write_one: F,
) -> fmt::Result
where
    I: IntoIterator<Item = T>,
    F: FnMut(&mut NewlineTerminated<W>, T) -> fmt::Result,
{
    let mut f = NewlineTerminated::new(f);
    for item in items {
        write_one(&mut f, item)?;
        f.finish_line()?;
    }
    Ok(())
}

// The remainder of this module is `Display` adapters for formatting all the
// details in the scrimlet reconcilers status structure returned by the
// bootstrap agent lockstep API.

struct ScrimletReconcilersStatusDisplay<'a>(&'a ScrimletReconcilersStatus);

impl fmt::Display for ScrimletReconcilersStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo => {
                write!(
                    f,
                    "not running: sled-agent has not initialized \
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
                ddmd_reconciler,
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
                    ("ddmd", &ReconcilerStatusDisplay(&ddmd_reconciler)),
                ];
                write_lines(f, reconcilers, |f, (name, displayable)| {
                    writeln!(f, "{name} reconciler:")?;
                    write_block(
                        IndentWriter::new(INDENT, &mut *f),
                        displayable,
                    )?;
                    // Add a blank line between the top-level sections,
                    // including after the last one.
                    writeln!(f)
                })
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
        write_block(
            IndentWriter::new(INDENT, &mut f),
            ReconcilerCurrentStatusDisplay(&current_status),
        )?;

        if let Some(last_completion) = last_completion {
            writeln!(f, "last completion:")?;
            write_block(
                IndentWriter::new(INDENT, f),
                ReconciliationCompletedStatusDisplay(&last_completion),
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
                    "started at: {}",
                    datetime_rfc3339_concise(started_at_time)
                )?;
                write!(f, "running for: {running_for:?}")
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
            "completed at: {}",
            datetime_rfc3339_concise(completed_at_time)
        )?;
        writeln!(f, "ran for: {ran_for:?}")?;
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
                    write_lines(
                        &mut IndentWriter::new(INDENT, f),
                        ports,
                        |f, (port, status)| write!(f, "* {port}: {status:?}"),
                    )?;
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
                    write_lines(
                        &mut IndentWriter::new(INDENT, f),
                        ports,
                        |f, (port, values)| {
                            write!(f, "* {port}: {}", values.join(", "))
                        },
                    )?;
                }
                Ok(())
            }
        }
    }
}

impl DisplayableStatus for DdmdReconcilerStatus {
    type DisplayAdapter<'a>
        = DdmdReconcilerStatusDisplay<'a>
    where
        Self: 'a;

    /// Get a `fmt::Display`-able version of this status (e.g., for `omdb`).
    fn display(&self) -> DdmdReconcilerStatusDisplay<'_> {
        DdmdReconcilerStatusDisplay(self)
    }
}

struct DdmdReconcilerStatusDisplay<'a>(&'a DdmdReconcilerStatus);

impl fmt::Display for DdmdReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            DdmdReconcilerStatus::Failed(reason) => {
                write!(f, "reconciliation failed: {reason}")
            }
            DdmdReconcilerStatus::Reconciled {
                external_peers_address_objects,
            } => {
                let plural = if external_peers_address_objects.len() == 1 {
                    ""
                } else {
                    "s"
                };
                write!(
                    f,
                    "successfully reconciled {} external peer address object{plural}",
                    external_peers_address_objects.len()
                )?;

                if !external_peers_address_objects.is_empty() {
                    writeln!(f, ":")?;
                    write_lines(
                        &mut IndentWriter::new(INDENT, f),
                        external_peers_address_objects,
                        |f, addrobj| write!(f, "* {addrobj}"),
                    )?;
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
        write_block(
            IndentWriter::new(INDENT, &mut f),
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
                    write_lines(
                        &mut IndentWriter::new(INDENT, &mut f),
                        clear_failures,
                        |f, DpdPortOperationFailure { port_id, error }| {
                            write!(f, "* {port_id}: {error}")
                        },
                    )?;
                }
                if apply_failures.is_empty() {
                    write!(f, "apply failures: none")?;
                } else {
                    writeln!(f, "apply failures:")?;
                    write_lines(
                        &mut IndentWriter::new(INDENT, &mut f),
                        apply_failures,
                        |f, DpdPortOperationFailure { port_id, error }| {
                            write!(f, "* {port_id}: {error}")
                        },
                    )?;
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
                if unchanged.is_empty() {
                    writeln!(f, "zones unchanged: none")?;
                } else {
                    writeln!(f, "zones unchanged:")?;
                    write_lines(
                        &mut IndentWriter::new(INDENT, &mut f),
                        unchanged,
                        |f, id| write!(f, "* zone {id}"),
                    )?;
                }
                if removed.is_empty() {
                    writeln!(f, "NAT entries removed: none")?;
                } else {
                    writeln!(f, "NAT entries removed:")?;
                    write_lines(
                        &mut IndentWriter::new(INDENT, &mut f),
                        removed,
                        |f, entry| {
                            let entry =
                                DpdNatReconcilerStatusNatEntryDisplay(&entry);
                            write!(f, "* {entry}")
                        },
                    )?;
                }
                if created.is_empty() {
                    writeln!(f, "NAT entries created: none")?;
                } else {
                    writeln!(f, "NAT entries created:")?;
                    write_lines(
                        &mut IndentWriter::new(INDENT, &mut f),
                        created,
                        |f, (id, entry)| {
                            let entry =
                                DpdNatReconcilerStatusNatEntryDisplay(&entry);
                            write!(f, "* zone {id}: {entry}")
                        },
                    )?;
                }
                if remove_failures.is_empty() {
                    writeln!(f, "remove failures: none")?;
                } else {
                    writeln!(f, "remove failures:")?;
                    write_lines(
                        &mut IndentWriter::new(INDENT, &mut f),
                        remove_failures,
                        |f,
                         DpdNatReconcilerStatusNatEntryFailure {
                             entry,
                             error,
                         }| {
                            let entry =
                                DpdNatReconcilerStatusNatEntryDisplay(&entry);
                            write!(f, "* {entry}: {error}")
                        },
                    )?;
                }
                if create_failures.is_empty() {
                    write!(f, "create failures: none")?;
                } else {
                    writeln!(f, "create failures:")?;
                    write_lines(
                        &mut IndentWriter::new(INDENT, &mut f),
                        create_failures,
                        |f, (id, failure)| {
                            let DpdNatReconcilerStatusNatEntryFailure {
                                entry,
                                error,
                            } = failure;
                            write!(
                                f,
                                "* zone {id}, {}: {error}",
                                DpdNatReconcilerStatusNatEntryDisplay(&entry),
                            )
                        },
                    )?;
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            MgdReconcilerStatus::FailedGeneratingDesiredConfig(err) => {
                write!(f, "failed to generate desired router set: {err}")
            }
            MgdReconcilerStatus::FailedApplying { routers, error } => {
                write!(
                    f,
                    "failed to apply desired router set \
                     ({routers} routers): {error}"
                )
            }
            MgdReconcilerStatus::Success { routers } => {
                write!(f, "applied desired router set ({routers} routers)")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use omicron_test_utils::dev::test_cmds::OutputSnapshot;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use sled_agent_types::early_networking::LldpAdminStatus;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::time::Duration;

    fn nat_entry(last_octet: u8) -> DpdNatReconcilerStatusNatEntry {
        DpdNatReconcilerStatusNatEntry {
            external_ip: IpAddr::V4(Ipv4Addr::new(192, 0, 2, last_octet)),
            first_port: 0,
            last_port: 16383,
        }
    }

    #[test]
    fn test_reconciliation_completed_status_display() {
        let status = ReconciliationCompletedStatus {
            activation_reason: ReconcilerActivationReason::Startup,
            completed_at_time: chrono::DateTime::from_timestamp(0, 0)
                .expect("the epoch is a valid timestamp"),
            ran_for: Duration::from_millis(1500),
            activation_count: 3,
            status: LldpdReconcilerStatus::SkippedConfigUpToDate,
        };
        expectorate::assert_contents(
            "tests/output/reconciliation-completed-status.txt",
            &ReconciliationCompletedStatusDisplay(&status).to_string(),
        );
    }

    #[test]
    fn test_dpd_nat_reconciler_status_display() {
        let status = DpdNatReconcilerStatus::Complete {
            unchanged: BTreeSet::from([OmicronZoneUuid::nil()]),
            removed: vec![nat_entry(1), nat_entry(3)],
            remove_failures: Vec::new(),
            created: BTreeMap::from([(OmicronZoneUuid::nil(), nat_entry(2))]),
            create_failures: BTreeMap::new(),
        };
        expectorate::assert_contents(
            "tests/output/dpd-nat-reconciler-status.txt",
            &DpdNatReconcilerStatusDisplay(&status).to_string(),
        );
    }

    fn port_failure(port_id: &str, error: &str) -> DpdPortOperationFailure {
        DpdPortOperationFailure {
            port_id: port_id.to_string(),
            error: error.to_string(),
        }
    }

    fn nat_failure(
        last_octet: u8,
        error: &str,
    ) -> DpdNatReconcilerStatusNatEntryFailure {
        DpdNatReconcilerStatusNatEntryFailure {
            entry: nat_entry(last_octet),
            error: error.to_string(),
        }
    }

    fn completed<T>(
        status: T,
    ) -> Option<Box<ReconciliationCompletedStatus<T>>> {
        Some(Box::new(ReconciliationCompletedStatus {
            activation_reason: ReconcilerActivationReason::Startup,
            completed_at_time: DateTime::from_timestamp(0, 0)
                .expect("the epoch is a valid timestamp"),
            ran_for: Duration::from_millis(1500),
            activation_count: 1,
            status,
        }))
    }

    fn populated_status() -> ScrimletReconcilersStatus {
        ScrimletReconcilersStatus::Running {
            dpd_reconciler: ReconcilerStatus {
                current_status: ReconcilerCurrentStatus::Running(
                    ReconcilerRunningStatus {
                        activation_reason:
                            ReconcilerActivationReason::PeriodicTimer,
                        started_at_time: DateTime::from_timestamp(60, 0)
                            .expect("one minute after the epoch is valid"),
                        running_for: Duration::from_millis(250),
                    },
                ),
                last_completion: completed(DpdReconcilerStatus {
                    port_settings_status: DpdPortReconcilerStatus::Complete {
                        unchanged: BTreeSet::from([
                            "qsfp0".to_string(),
                            "qsfp1".to_string(),
                        ]),
                        cleared: BTreeSet::from(["qsfp2".to_string()]),
                        clear_failures: vec![
                            port_failure("qsfp3", "clear failed"),
                            port_failure("qsfp4", "clear failed again"),
                        ],
                        applied: BTreeSet::from(["qsfp5".to_string()]),
                        apply_failures: vec![
                            port_failure("qsfp6", "no"),
                            port_failure("qsfp7", "nope"),
                        ],
                    },
                    nat_status: DpdNatReconcilerStatus::Complete {
                        unchanged: BTreeSet::from([
                            OmicronZoneUuid::from_u128(1),
                            OmicronZoneUuid::from_u128(2),
                        ]),
                        removed: vec![nat_entry(1), nat_entry(3)],
                        remove_failures: vec![
                            nat_failure(4, "remove failed"),
                            nat_failure(5, "remove failed again"),
                        ],
                        created: BTreeMap::from([(
                            OmicronZoneUuid::from_u128(3),
                            nat_entry(2),
                        )]),
                        create_failures: BTreeMap::from([
                            (
                                OmicronZoneUuid::from_u128(4),
                                nat_failure(6, "create failed"),
                            ),
                            (
                                OmicronZoneUuid::from_u128(5),
                                nat_failure(7, "create failed again"),
                            ),
                        ]),
                    },
                }),
            },
            mgd_reconciler: ReconcilerStatus {
                current_status: ReconcilerCurrentStatus::Idle,
                last_completion: completed(
                    MgdReconcilerStatus::FailedApplying {
                        routers: 2,
                        error: "apply failed".to_string(),
                    },
                ),
            },
            uplinkd_reconciler: ReconcilerStatus {
                current_status: ReconcilerCurrentStatus::Idle,
                last_completion: completed(
                    UplinkdReconcilerStatus::Reconciled {
                        ports: BTreeMap::from([
                            (
                                "qsfp0".to_string(),
                                vec![
                                    "192.0.2.10/24".to_string(),
                                    "2001:db8::10/64".to_string(),
                                ],
                            ),
                            (
                                "qsfp1".to_string(),
                                vec!["192.0.2.11/24".to_string()],
                            ),
                        ]),
                    },
                ),
            },
            lldpd_reconciler: ReconcilerStatus {
                current_status: ReconcilerCurrentStatus::Idle,
                last_completion: completed(LldpdReconcilerStatus::Reconciled {
                    ports: BTreeMap::from([
                        ("qsfp0".to_string(), LldpAdminStatus::Enabled),
                        ("qsfp1".to_string(), LldpAdminStatus::Disabled),
                    ]),
                }),
            },
            ddmd_reconciler: ReconcilerStatus {
                current_status: ReconcilerCurrentStatus::Idle,
                last_completion: completed(DdmdReconcilerStatus::Reconciled {
                    external_peers_address_objects: BTreeSet::from([
                        "tfportqsfp0_0/ll".to_string(),
                        "tfportqsfp1_0/ll".to_string(),
                    ]),
                }),
            },
        }
    }

    fn empty_status() -> ScrimletReconcilersStatus {
        ScrimletReconcilersStatus::Running {
            dpd_reconciler: ReconcilerStatus {
                current_status: ReconcilerCurrentStatus::Idle,
                last_completion: completed(DpdReconcilerStatus {
                    port_settings_status: DpdPortReconcilerStatus::Complete {
                        unchanged: BTreeSet::new(),
                        cleared: BTreeSet::new(),
                        clear_failures: Vec::new(),
                        applied: BTreeSet::new(),
                        apply_failures: Vec::new(),
                    },
                    nat_status: DpdNatReconcilerStatus::Complete {
                        unchanged: BTreeSet::new(),
                        removed: Vec::new(),
                        remove_failures: Vec::new(),
                        created: BTreeMap::new(),
                        create_failures: BTreeMap::new(),
                    },
                }),
            },
            mgd_reconciler: ReconcilerStatus {
                current_status: ReconcilerCurrentStatus::Idle,
                last_completion: completed(MgdReconcilerStatus::Success {
                    routers: 0,
                }),
            },
            uplinkd_reconciler: ReconcilerStatus {
                current_status: ReconcilerCurrentStatus::Idle,
                last_completion: None,
            },
            lldpd_reconciler: ReconcilerStatus {
                current_status: ReconcilerCurrentStatus::Idle,
                last_completion: completed(LldpdReconcilerStatus::Reconciled {
                    ports: BTreeMap::new(),
                }),
            },
            ddmd_reconciler: ReconcilerStatus {
                current_status: ReconcilerCurrentStatus::Idle,
                last_completion: completed(DdmdReconcilerStatus::Reconciled {
                    external_peers_address_objects: BTreeSet::new(),
                }),
            },
        }
    }

    #[test]
    fn test_scrimlet_reconcilers_status_display() {
        let cases = [
            ("all fields populated", populated_status()),
            ("all fields empty", empty_status()),
        ];
        let mut snapshot = OutputSnapshot::new();
        for (name, status) in cases {
            snapshot.push(
                format_args!("CASE: {name}"),
                ScrimletReconcilersStatusDisplay(&status),
                "",
            );
        }
        snapshot
            .assert_contents("tests/output/scrimlet-reconcilers-status.txt");
    }
}
