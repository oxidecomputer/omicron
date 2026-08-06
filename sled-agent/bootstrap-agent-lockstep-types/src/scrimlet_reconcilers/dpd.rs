// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconcilers responsible for
//! syncing configuration from the bootstore to dpd in the switch zone.

use indent_write::fmt::IndentWriter;
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Write;
use std::net::IpAddr;

/// Description of a failure to perform some dpd operation on a specific port.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct DpdPortOperationFailure {
    pub port_id: String,
    pub error: String,
}

/// Status of reconciling QSFP port settings with `dpd`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum DpdPortReconcilerStatus {
    /// Reconciliation failed while attempting to read the current settings from
    /// `dpd`.
    FailedReadingCurrentSettings(String),

    /// Reconciliation failed because the data prevented us from constructing a
    /// plan - this should be impossible absent bugs.
    FailedGeneratingPlan(String),

    /// Reconciliation completed.
    Complete {
        unchanged: BTreeSet<String>,
        cleared: BTreeSet<String>,
        clear_failures: Vec<DpdPortOperationFailure>,
        applied: BTreeSet<String>,
        apply_failures: Vec<DpdPortOperationFailure>,
    },
}

impl DpdPortReconcilerStatus {
    pub fn display(&self) -> DpdPortReconcilerStatusDisplay<'_> {
        DpdPortReconcilerStatusDisplay(self)
    }
}

pub struct DpdPortReconcilerStatusDisplay<'a>(&'a DpdPortReconcilerStatus);

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
                writeln!(f, "reconciliation complete")?;
                let mut f = IndentWriter::new("    ", f);
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
                    let mut f = IndentWriter::new("    ", &mut f);
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
                    let mut f = IndentWriter::new("    ", &mut f);
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

impl slog::KV for DpdPortReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let skipped_key = "port-reconciler-skipped";
        match self {
            Self::FailedReadingCurrentSettings(reason) => {
                serializer.emit_str(skipped_key.into(), reason)
            }
            Self::FailedGeneratingPlan(reason) => {
                serializer.emit_str(skipped_key.into(), reason)
            }
            Self::Complete {
                unchanged,
                cleared,
                clear_failures,
                applied,
                apply_failures,
            } => {
                // Only show a summary count; we have individual log statements
                // for each clear/apply.
                for (key, val) in [
                    ("port-settings-unchanged", unchanged.len()),
                    ("port-settings-successfully-cleared", cleared.len()),
                    ("port-settings-failed-to-clear", clear_failures.len()),
                    ("port-settings-successfully-applied", applied.len()),
                    ("port-settings-failed-to-apply", apply_failures.len()),
                ] {
                    serializer.emit_usize(key.into(), val)?;
                }
                Ok(())
            }
        }
    }
}

/// Status of the `dpd` scrimlet reconciler.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DpdReconcilerStatus {
    /// Result of reconciling port settings
    pub port_settings_status: DpdPortReconcilerStatus,
    /// Result of reconciling service zone NAT entries
    pub nat_status: DpdNatReconcilerStatus,
}

impl slog::KV for DpdReconcilerStatus {
    fn serialize(
        &self,
        record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self { port_settings_status, nat_status } = self;
        slog::KV::serialize(&port_settings_status, record, serializer)?;
        slog::KV::serialize(&nat_status, record, serializer)?;
        Ok(())
    }
}

impl super::DisplayableStatus for DpdReconcilerStatus {
    type DisplayAdapter<'a>
        = DpdReconcilerStatusDisplay<'a>
    where
        Self: 'a;

    fn display<'a>(&'a self) -> Self::DisplayAdapter<'a> {
        DpdReconcilerStatusDisplay(self)
    }
}

pub struct DpdReconcilerStatusDisplay<'a>(&'a DpdReconcilerStatus);

impl fmt::Display for DpdReconcilerStatusDisplay<'_> {
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let DpdReconcilerStatus { port_settings_status, nat_status } = self.0;
        writeln!(f, "port settings:")?;
        writeln!(
            IndentWriter::new("    ", &mut f),
            "{}",
            port_settings_status.display()
        )?;
        writeln!(f, "NAT:")?;
        write!(
            IndentWriter::new("    ", &mut f),
            "{}",
            nat_status.display()
        )
    }
}

/// Identifying information of a single NAT entry.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct DpdNatReconcilerStatusNatEntry {
    pub external_ip: IpAddr,
    pub first_port: u16,
    pub last_port: u16,
}

impl DpdNatReconcilerStatusNatEntry {
    pub fn display(&self) -> DpdNatReconcilerStatusNatEntryDisplay<'_> {
        DpdNatReconcilerStatusNatEntryDisplay(self)
    }
}

pub struct DpdNatReconcilerStatusNatEntryDisplay<'a>(
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

/// Description of a failure to perform some dpd operation on a specific NAT
/// entry.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct DpdNatReconcilerStatusNatEntryFailure {
    pub entry: DpdNatReconcilerStatusNatEntry,
    pub error: String,
}

/// Status of reconciling service zone NAT entries with `dpd`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum DpdNatReconcilerStatus {
    /// Reconciliation was skipped because the bootstore contains no NAT entry
    /// config information.
    NoNatEntriesConfig,

    /// Reconciliation failed while attempting to read the current set of
    /// entries from `dpd`.
    FailedReadingCurrentDpdNatEntries(String),

    /// Reconciliation failed because the bootstore config contained an illegal
    /// combination of entries (e.g., two zones with identical NAT entries).
    InvalidSystemNetworkingConfig(String),

    /// Reconciliation completed.
    Complete {
        /// Set of zone IDs whose NAT entries were already correct in `dpd` and
        /// left unchanged.
        unchanged: BTreeSet<OmicronZoneUuid>,

        /// List of NAT entries successfully removed.
        removed: Vec<DpdNatReconcilerStatusNatEntry>,

        /// List of NAT entries we tried but failed to remove.
        remove_failures: Vec<DpdNatReconcilerStatusNatEntryFailure>,

        /// Map of zone NAT entries successfully created.
        created: BTreeMap<OmicronZoneUuid, DpdNatReconcilerStatusNatEntry>,

        /// Map of zone NAT entries we tried but failed to create.
        create_failures:
            BTreeMap<OmicronZoneUuid, DpdNatReconcilerStatusNatEntryFailure>,
    },
}

impl slog::KV for DpdNatReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match self {
            DpdNatReconcilerStatus::NoNatEntriesConfig => serializer.emit_str(
                "nat-reconciler-skipped".into(),
                "no NAT entries present in config",
            ),
            DpdNatReconcilerStatus::FailedReadingCurrentDpdNatEntries(
                reason,
            ) => serializer.emit_str("nat-reconciler-failed".into(), reason),
            DpdNatReconcilerStatus::InvalidSystemNetworkingConfig(reason) => {
                serializer.emit_arguments(
                    "nat-reconciler-failed".into(),
                    &format_args!("invalid system networking config: {reason}"),
                )
            }
            DpdNatReconcilerStatus::Complete {
                unchanged,
                removed,
                remove_failures,
                created,
                create_failures,
            } => {
                // Only show a summary count; we have individual log statements
                // for each create/remove.
                for (key, val) in [
                    ("nat-entries-unchanged", unchanged.len()),
                    ("nat-entries-successfully-removed", removed.len()),
                    ("nat-entries-failed-to-remove", remove_failures.len()),
                    ("nat-entries-successfully-created", created.len()),
                    ("nat-entries-failed-to-create", create_failures.len()),
                ] {
                    serializer.emit_usize(key.into(), val)?;
                }
                Ok(())
            }
        }
    }
}

impl DpdNatReconcilerStatus {
    pub fn display(&self) -> DpdNatReconcilerStatusDisplay<'_> {
        DpdNatReconcilerStatusDisplay(self)
    }
}

pub struct DpdNatReconcilerStatusDisplay<'a>(&'a DpdNatReconcilerStatus);

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
                let mut f = IndentWriter::new("    ", f);
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
                            .map(|entry| entry.display().to_string())
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
                                entry.display()
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )?;
                }
                if remove_failures.is_empty() {
                    writeln!(f, "remove failures: none")?;
                } else {
                    writeln!(f, "remove failures:")?;
                    let mut f = IndentWriter::new("    ", &mut f);
                    for DpdNatReconcilerStatusNatEntryFailure {
                        entry,
                        error,
                    } in remove_failures
                    {
                        writeln!(f, "* {}: {error}", entry.display())?;
                    }
                }
                if create_failures.is_empty() {
                    write!(f, "create failures: none")?;
                } else {
                    writeln!(f, "create failures:")?;
                    let mut f = IndentWriter::new("    ", &mut f);
                    let mut create_failures = create_failures.iter().peekable();
                    while let Some((id, failure)) = create_failures.next() {
                        let DpdNatReconcilerStatusNatEntryFailure {
                            entry,
                            error,
                        } = failure;
                        let s = format_args!(
                            "* zone {id}, {}: {error}",
                            entry.display()
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
