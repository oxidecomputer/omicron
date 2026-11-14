// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Status report types for trust quorum nodes.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1::types::Epoch;

/// Status reported by a node that is not currently coordinating a
/// reconfiguration or LRTQ upgrade.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct NotCoordinatingStatus {
    /// The latest configuration epoch this node has persisted.
    ///
    /// Below, N is the epoch of a configuration this node was asked to
    /// coordinate.
    ///
    /// * If lower than N, then this node has not persisted that
    ///   configuration: it never received or accepted it, or it stopped
    ///   coordinating it (for example, by restarting) before collecting shares
    ///   and persisting it.
    ///
    /// * If equal to N, then this node started coordinating that epoch but is
    ///   no longer doing so: either it committed the epoch (see
    ///   `latest_committed_epoch`), or it lost its coordination state, for
    ///   example by restarting.
    ///
    /// * If greater than N, then this node has moved on to a newer
    ///   configuration, and N has been superseded.
    ///
    /// This is `None` if the node has not persisted a trust quorum
    /// configuration, including an LRTQ node that has not yet persisted an
    /// upgrade configuration.
    pub latest_config_epoch: Option<Epoch>,
    /// The latest configuration epoch this node has committed.
    ///
    /// This is never greater than `latest_config_epoch`.
    ///
    /// If this value is lower than N (see `latest_config_epoch`), it means
    /// that _this_ node has not committed N, not that _no_ node has. (For
    /// example, the coordinator may have crashed after Nexus started
    /// committing, but before its own commit arrived.)
    ///
    /// This is `None` if the node has not committed a trust quorum
    /// configuration, including a node that has not yet upgraded from LRTQ.
    // TODO: Resends will use this data as described above, but are not
    // implemented yet -- implement them within Nexus.
    pub latest_committed_epoch: Option<Epoch>,
}
