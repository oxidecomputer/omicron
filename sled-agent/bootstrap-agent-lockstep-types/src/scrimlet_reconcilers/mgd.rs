// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconcilers responsible for
//! syncing configuration from the bootstore to mgd in the switch zone.

use indent_write::fmt::IndentWriter;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Write;
use std::net::IpAddr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MgdBfdOperationFailure {
    pub peer: IpAddr,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum MgdBfdReconcilerStatus {
    /// Reconciliation failed because we couldn't fetch the current set of
    /// BFD peers from mgd.
    FailedReadingBfdPeers(String),

    /// Reconciliation completed.
    Complete {
        unchanged: BTreeSet<IpAddr>,
        remove_success: Vec<IpAddr>,
        remove_failure: Vec<MgdBfdOperationFailure>,
        add_success: Vec<IpAddr>,
        add_failure: Vec<MgdBfdOperationFailure>,
    },
}

impl slog::KV for MgdBfdReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let skipped_key = "bfd-reconciler-skipped";
        match self {
            Self::FailedReadingBfdPeers(reason) => {
                serializer.emit_str(skipped_key.into(), reason)
            }
            Self::Complete {
                unchanged,
                remove_success,
                remove_failure,
                add_success,
                add_failure,
            } => {
                for (key, val) in [
                    ("bfd-unchanged", unchanged.len()),
                    ("bfd-successfully-removed", remove_success.len()),
                    ("bfd-failed-to-remove", remove_failure.len()),
                    ("bfd-successfully-added", add_success.len()),
                    ("bfd-failed-to-add", add_failure.len()),
                ] {
                    serializer.emit_usize(key.into(), val)?;
                }
                Ok(())
            }
        }
    }
}

impl MgdBfdReconcilerStatus {
    pub fn display(&self) -> MgdBfdReconcilerStatusDisplay<'_> {
        MgdBfdReconcilerStatusDisplay(self)
    }
}

pub struct MgdBfdReconcilerStatusDisplay<'a>(&'a MgdBfdReconcilerStatus);

impl fmt::Display for MgdBfdReconcilerStatusDisplay<'_> {
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            MgdBfdReconcilerStatus::FailedReadingBfdPeers(err) => {
                writeln!(f, "failed to read current bfd peers: {err}")
            }
            MgdBfdReconcilerStatus::Success {
                unchanged,
                remove_success,
                add_success,
            } => {
                writeln!(f, "reconciliation succeeded")?;
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
                Ok(())
            }
            MgdBfdReconcilerStatus::PartialSuccess {
                unchanged,
                remove_success,
                remove_failure,
                add_success,
                add_failure,
            } => {
                writeln!(f, "reconciliation FAILED")?;
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
                    let mut f = IndentWriter::new("    ", &mut f);
                    for MgdBfdOperationFailure { peer, error } in remove_failure
                    {
                        writeln!(f, "* {peer}: {error}")?;
                    }
                }
                if add_failure.is_empty() {
                    writeln!(f, "add failures: none")?;
                } else {
                    writeln!(f, "add failures:")?;
                    let mut f = IndentWriter::new("    ", &mut f);
                    for MgdBfdOperationFailure { peer, error } in add_failure {
                        writeln!(f, "* {peer}: {error}")?;
                    }
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct MgdBgpReconcilerStatusOpCount {
    pub routers_deleted: usize,
    pub routers_updated: usize,
    pub routers_created: usize,

    pub origin4_deleted: usize,
    pub origin4_updated: usize,
    pub origin4_created: usize,

    pub origin6_deleted: usize,
    pub origin6_updated: usize,
    pub origin6_created: usize,

    pub shapers_deleted: usize,
    pub shapers_updated: usize,
    pub shapers_created: usize,

    pub checkers_deleted: usize,
    pub checkers_updated: usize,
    pub checkers_created: usize,

    pub numbered_peers_deleted: usize,
    pub numbered_peers_updated: usize,
    pub numbered_peers_created: usize,

    pub unnumbered_peers_deleted: usize,
    pub unnumbered_peers_updated: usize,
    pub unnumbered_peers_created: usize,
}

impl slog::KV for MgdBgpReconcilerStatusOpCount {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self {
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
        } = self;
        for (key, val) in [
            ("bgp-routers-deleted", routers_deleted),
            ("bgp-routers-updated", routers_updated),
            ("bgp-routers-created", routers_created),
            ("bgp-origin4-deleted", origin4_deleted),
            ("bgp-origin4-updated", origin4_updated),
            ("bgp-origin4-created", origin4_created),
            ("bgp-origin6-deleted", origin6_deleted),
            ("bgp-origin6-updated", origin6_updated),
            ("bgp-origin6-created", origin6_created),
            ("bgp-shapers-deleted", shapers_deleted),
            ("bgp-shapers-updated", shapers_updated),
            ("bgp-shapers-created", shapers_created),
            ("bgp-checkers-deleted", checkers_deleted),
            ("bgp-checkers-updated", checkers_updated),
            ("bgp-checkers-created", checkers_created),
            ("bgp-numbered-peers-deleted", numbered_peers_deleted),
            ("bgp-numbered-peers-updated", numbered_peers_updated),
            ("bgp-numbered-peers-created", numbered_peers_created),
            ("bgp-unnumbered-peers-deleted", unnumbered_peers_deleted),
            ("bgp-unnumbered-peers-updated", unnumbered_peers_updated),
            ("bgp-unnumbered-peers-created", unnumbered_peers_created),
        ] {
            serializer.emit_usize(key.into(), *val)?;
        }
        Ok(())
    }
}

impl MgdBgpReconcilerStatusOpCount {
    pub fn display(&self) -> MgdBgpReconcilerStatusOpCountDisplay<'_> {
        MgdBgpReconcilerStatusOpCountDisplay(self)
    }
}

pub struct MgdBgpReconcilerStatusOpCountDisplay<'a>(
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
        for (name, created, updated, deleted) in [
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
        ] {
            writeln!(
                f,
                "{name} created / updated / deleted: \
                 {created} / {updated} / {deleted}"
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum MgdBgpReconcilerStatus {
    /// Reconciliation failed because we couldn't fetch the current BGP
    /// configuration from MGD.
    FailedReadingBgpConfig(String),

    /// Reconciliation failed because we couldn't generate a desired config
    /// from the bootstore.
    ///
    /// This should never happen - it indicates there's faulty data in the
    /// persisted config.
    FailedGeneratingDesiredConfig(String),

    /// Reconciliation completed.
    ///
    /// mgd operations are performed in bulk, so `counts` contains the number of
    /// items involved.
    Complete {
        counts: MgdBgpReconcilerStatusOpCount,
        did_change_max_paths: bool,
        errors: Vec<String>,
    },
}

impl slog::KV for MgdBgpReconcilerStatus {
    fn serialize(
        &self,
        record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let skipped_key = "bgp-skipped".into();
        match self {
            Self::FailedReadingBgpConfig(reason)
            | Self::FailedGeneratingDesiredConfig(reason) => {
                serializer.emit_str(skipped_key, reason)
            }
            MgdBgpReconcilerStatus::Complete {
                counts,
                did_change_max_paths,
                errors,
            } => {
                serializer.emit_bool(
                    "bgp-did-change-max-paths".into(),
                    *did_change_max_paths,
                )?;
                // Each individual error is already logged; only log the count.
                serializer.emit_usize("bgp-errors".into(), errors.len())?;
                slog::KV::serialize(&counts, record, serializer)
            }
        }
    }
}

impl MgdBgpReconcilerStatus {
    pub fn display(&self) -> MgdBgpReconcilerStatusDisplay<'_> {
        MgdBgpReconcilerStatusDisplay(self)
    }
}

pub struct MgdBgpReconcilerStatusDisplay<'a>(&'a MgdBgpReconcilerStatus);

impl fmt::Display for MgdBgpReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            MgdBgpReconcilerStatus::FailedReadingBgpConfig(err) => {
                writeln!(f, "failed to read current bgp config: {err}")
            }
            MgdBgpReconcilerStatus::FailedGeneratingDesiredConfig(err) => {
                writeln!(f, "failed to generate desired bgp config: {err}")
            }
            MgdBgpReconcilerStatus::Success {
                counts,
                did_change_max_paths,
            } => {
                writeln!(f, "reconciliation succeeded")?;
                writeln!(f, "did change max paths: {did_change_max_paths}")?;
                write!(f, "{}", counts.display())?;
                Ok(())
            }
            MgdBgpReconcilerStatus::PartialSuccess {
                counts,
                did_change_max_paths,
                errors,
            } => {
                writeln!(f, "reconciliation FAILED")?;
                writeln!(f, "did change max paths: {did_change_max_paths}")?;
                write!(f, "{}", counts.display())?;
                writeln!(f, "errors:")?;
                let mut f = IndentWriter::new("    ", f);
                for err in errors {
                    writeln!(f, "* {err}")?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum MgdStaticRouteReconcilerStatus {
    /// Reconciliation failed because we couldn't fetch the current set of
    /// static routes from MGD.
    FailedReadingStaticRoutes(String),

    /// Reconciliation failed because we couldn't determine a plan for
    /// changes to make.
    ///
    /// This should never happen - it indicates there's some faulty data
    /// somewhere (either coming from mgd or in the rack network config).
    FailedGeneratingPlan(String),

    /// Reconciliation completed.
    ///
    /// mgd operations are performed in bulk, so each item here contains the
    /// count of items involved (on success) or the error associated with the
    /// bulk operation (on failure).
    Complete {
        unchanged: usize,
        #[serde(with = "snake_case_result")]
        #[schemars(
            schema_with = "SnakeCaseResult::<usize, String>::json_schema"
        )]
        delete_v4_result: Result<usize, String>,
        #[serde(with = "snake_case_result")]
        #[schemars(
            schema_with = "SnakeCaseResult::<usize, String>::json_schema"
        )]
        delete_v6_result: Result<usize, String>,
        #[serde(with = "snake_case_result")]
        #[schemars(
            schema_with = "SnakeCaseResult::<usize, String>::json_schema"
        )]
        add_v4_result: Result<usize, String>,
        #[serde(with = "snake_case_result")]
        #[schemars(
            schema_with = "SnakeCaseResult::<usize, String>::json_schema"
        )]
        add_v6_result: Result<usize, String>,
    },
}

impl slog::KV for MgdStaticRouteReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let skipped_key = "static-routes-reconciler-skipped";
        match self {
            Self::FailedReadingStaticRoutes(reason) => {
                serializer.emit_str(skipped_key.into(), reason)
            }
            Self::FailedGeneratingPlan(reason) => {
                serializer.emit_str(skipped_key.into(), reason)
            }
            Self::Complete {
                unchanged,
                delete_v4_result,
                delete_v6_result,
                add_v4_result,
                add_v6_result,
            } => {
                serializer
                    .emit_usize("static-routes-unchanged".into(), *unchanged)?;
                for (key, result) in [
                    ("static-routes-delete-v4", delete_v4_result),
                    ("static-routes-delete-v6", delete_v6_result),
                    ("static-routes-add-v4", add_v4_result),
                    ("static-routes-add-v6", add_v6_result),
                ] {
                    match result {
                        Ok(items) => serializer.emit_arguments(
                            key.into(),
                            &format_args!("success ({items} routes affected)"),
                        )?,
                        Err(err) => {
                            serializer.emit_str(key.into(), &err)?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

impl MgdStaticRouteReconcilerStatus {
    pub fn display(&self) -> MgdStaticRouteReconcilerStatusDisplay<'_> {
        MgdStaticRouteReconcilerStatusDisplay(self)
    }
}

pub struct MgdStaticRouteReconcilerStatusDisplay<'a>(
    &'a MgdStaticRouteReconcilerStatus,
);

impl fmt::Display for MgdStaticRouteReconcilerStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            MgdStaticRouteReconcilerStatus::FailedReadingStaticRoutes(err) => {
                writeln!(f, "failed to read current static routes: {err}")
            }
            MgdStaticRouteReconcilerStatus::FailedGeneratingPlan(err) => {
                writeln!(f, "failed to generate reconciliation plan: {err}")
            }
            MgdStaticRouteReconcilerStatus::Success {
                unchanged,
                deleted_v4,
                deleted_v6,
                added_v4,
                added_v6,
            } => {
                writeln!(f, "reconciliation succeeded")?;
                writeln!(f, "routes unchanged: {unchanged}")?;
                writeln!(
                    f,
                    "v4 routes deleted / added: {deleted_v4} / {added_v4}"
                )?;
                writeln!(
                    f,
                    "v6 routes deleted / added: {deleted_v6} / {added_v6}"
                )?;
                Ok(())
            }
            MgdStaticRouteReconcilerStatus::PartialSuccess {
                unchanged,
                delete_v4_result,
                delete_v6_result,
                add_v4_result,
                add_v6_result,
            } => {
                writeln!(f, "reconciliation FAILED")?;
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
                    writeln!(
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
                            writeln!(f, "v6 routes added: {n}")?;
                        }
                        Err(err) => {
                            writeln!(f, "failed to add v6 routes: {err}")?;
                        }
                    }
                }

                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MgdReconcilerStatus {
    pub bfd_status: MgdBfdReconcilerStatus,
    pub bgp_status: MgdBgpReconcilerStatus,
    pub static_routes_status: MgdStaticRouteReconcilerStatus,
}

impl slog::KV for MgdReconcilerStatus {
    fn serialize(
        &self,
        record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self { bfd_status, bgp_status, static_routes_status } = self;
        slog::KV::serialize(bfd_status, record, serializer)?;
        slog::KV::serialize(bgp_status, record, serializer)?;
        slog::KV::serialize(static_routes_status, record, serializer)?;
        Ok(())
    }
}

impl super::DisplayableStatus for MgdReconcilerStatus {
    type DisplayAdapter<'a>
        = MgdReconcilerStatusDisplay<'a>
    where
        Self: 'a;

    fn display<'a>(&'a self) -> Self::DisplayAdapter<'a> {
        MgdReconcilerStatusDisplay(self)
    }
}

pub struct MgdReconcilerStatusDisplay<'a>(&'a MgdReconcilerStatus);

impl fmt::Display for MgdReconcilerStatusDisplay<'_> {
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let MgdReconcilerStatus {
            bfd_status,
            bgp_status,
            static_routes_status,
        } = self.0;
        writeln!(f, "static routes:")?;
        writeln!(
            IndentWriter::new("    ", &mut f),
            "{}",
            static_routes_status.display()
        )?;
        writeln!(f, "BGP:")?;
        writeln!(
            IndentWriter::new("    ", &mut f),
            "{}",
            bgp_status.display()
        )?;
        writeln!(f, "BFD:")?;
        writeln!(
            IndentWriter::new("    ", &mut f),
            "{}",
            bfd_status.display()
        )?;
        Ok(())
    }
}
