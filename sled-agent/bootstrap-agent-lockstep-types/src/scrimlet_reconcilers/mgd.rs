// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconcilers responsible for
//! syncing configuration from the bootstore to mgd in the switch zone.

use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::net::IpAddr;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MgdBfdOperationFailure {
    pub peer: IpAddr,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum MgdBfdReconcilerStatus {
    /// Reconciliation was skipped because we couldn't fetch the current set of
    /// BFD peers from mgd.
    FailedReadingBfdPeers(String),

    /// Reconciliation completed successfully.
    Success {
        unchanged: BTreeSet<IpAddr>,
        remove_success: Vec<IpAddr>,
        add_success: Vec<IpAddr>,
    },

    /// Reconciliation completed.
    PartialSuccess {
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
            Self::Success { unchanged, remove_success, add_success } => {
                for (key, val) in [
                    ("bfd-unchanged", unchanged.len()),
                    ("bfd-successfully-removed", remove_success.len()),
                    ("bfd-failed-to-remove", 0),
                    ("bfd-successfully-added", add_success.len()),
                    ("bfd-failed-to-add", 0),
                ] {
                    serializer.emit_usize(key.into(), val)?;
                }
                Ok(())
            }
            Self::PartialSuccess {
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum MgdBgpReconcilerStatus {
    /// Reconciliation was skipped because we couldn't fetch the current BGP
    /// configuration from MGD.
    FailedReadingBgpConfig(String),

    /// Reconciliation was skipped because we couldn't generate a desired config
    /// from the bootstore.
    ///
    /// This should never happen - it indicates there's faulty data in the
    /// persisted config.
    FailedGeneratingDesiredConfig(String),

    /// Reconciliation completed successfully.
    ///
    /// mgd operations are performed in bulk, so each item here contains the
    /// count of items involved.
    Success {
        counts: MgdBgpReconcilerStatusOpCount,
        did_change_max_paths: bool,
    },

    /// Reconciliation completed with at least one failure.
    ///
    /// mgd operations are performed in bulk, so each item here contains the
    /// count of items applied on success.
    PartialSuccess {
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
            Self::Success { counts, did_change_max_paths } => {
                serializer.emit_bool(
                    "bgp-did-change-max-paths".into(),
                    *did_change_max_paths,
                )?;
                slog::KV::serialize(&counts, record, serializer)
            }
            MgdBgpReconcilerStatus::PartialSuccess {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum MgdStaticRouteReconcilerStatus {
    /// Reconciliation was skipped because we couldn't fetch the current set of
    /// static routes from MGD.
    FailedReadingStaticRoutes(String),

    /// Reconciliation was skipped because we couldn't determine a plan for
    /// changes to make.
    ///
    /// This should never happen - it indicates there's some faulty data
    /// somewhere (either coming from mgd or in the rack network config).
    FailedGeneratingPlan(String),

    /// Reconciliation completed successfully.
    ///
    /// mgd operations are performed in bulk, so each item here contains the
    /// count of items involved.
    Success {
        unchanged: usize,
        deleted_v4: usize,
        deleted_v6: usize,
        added_v4: usize,
        added_v6: usize,
    },

    /// Reconciliation completed with at least one failure.
    ///
    /// mgd operations are performed in bulk, so each item here contains the
    /// count of items applied on success.
    PartialSuccess {
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
            Self::Success {
                unchanged,
                deleted_v4,
                deleted_v6,
                added_v4,
                added_v6,
            } => {
                serializer
                    .emit_usize("static-routes-unchanged".into(), *unchanged)?;
                for (key, items) in [
                    ("static-routes-delete-v4", deleted_v4),
                    ("static-routes-delete-v6", deleted_v6),
                    ("static-routes-add-v4", added_v4),
                    ("static-routes-add-v6", added_v6),
                ] {
                    serializer.emit_arguments(
                        key.into(),
                        &format_args!("success ({items} routes affected)"),
                    )?;
                }
                Ok(())
            }
            Self::PartialSuccess {
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
