// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconcilers responsible for
//! syncing configuration from the bootstore to dpd in the switch zone.

use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
};

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
pub enum DpdPortReconcilerStatus {
    /// Reconciliation failed while attempting to read the current settings from
    /// `dpd`.
    FailedReadingCurrentSettings(String),

    /// Reconciliation failed because the data prevented us from constructing a
    /// plan - this should be impossible absent bugs.
    FailedGeneratingPlan(String),

    /// Reconciliation completed successfully.
    Success {
        unchanged: BTreeSet<String>,
        cleared: BTreeSet<String>,
        applied: BTreeSet<String>,
    },

    /// Reconciliation completed but had at least one failure.
    PartialSuccess {
        unchanged: BTreeSet<String>,
        cleared: BTreeSet<String>,
        clear_failures: Vec<DpdPortOperationFailure>,
        applied: BTreeSet<String>,
        apply_failures: Vec<DpdPortOperationFailure>,
    },
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
            Self::Success { unchanged, cleared, applied } => {
                // Only show a summary count; we have individual log statements
                // for each clear/apply.
                for (key, val) in [
                    ("port-settings-unchanged", unchanged.len()),
                    ("port-settings-successfully-cleared", cleared.len()),
                    ("port-settings-failed-to-clear", 0),
                    ("port-settings-successfully-applied", applied.len()),
                    ("port-settings-failed-to-apply", 0),
                ] {
                    serializer.emit_usize(key.into(), val)?;
                }
                Ok(())
            }
            Self::PartialSuccess {
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

    /// Reconciliation completed successfully.
    Success {
        /// Set of zone IDs whose NAT entries were already correct in `dpd` and
        /// left unchanged.
        unchanged: BTreeSet<OmicronZoneUuid>,

        /// List of NAT entries removed.
        removed: Vec<DpdNatReconcilerStatusNatEntry>,

        /// Map of zone NAT entries created.
        created: BTreeMap<OmicronZoneUuid, DpdNatReconcilerStatusNatEntry>,
    },

    /// Reconciliation completed but had at least one failure.
    PartialSuccess {
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
            DpdNatReconcilerStatus::Success { unchanged, removed, created } => {
                // Only show a summary count; we have individual log statements
                // for each create/remove.
                for (key, val) in [
                    ("nat-entries-unchanged", unchanged.len()),
                    ("nat-entries-successfully-removed", removed.len()),
                    ("nat-entries-failed-to-remove", 0),
                    ("nat-entries-successfully-created", created.len()),
                    ("nat-entries-failed-to-create", 0),
                ] {
                    serializer.emit_usize(key.into(), val)?;
                }
                Ok(())
            }
            DpdNatReconcilerStatus::PartialSuccess {
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
