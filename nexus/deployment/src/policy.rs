// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generic deployment policy types and implementation
//!
//! See crate-level docs for details.

use std::num::NonZeroUsize;

/// Fleet-wide deployment policy
// XXX-dap this is more of a placeholder right now.
pub struct Policy {
    /// number of CockroachDB nodes
    pub ncockroach: NonZeroUsize,
    /// number of Nexus nodes
    pub nnexus: NonZeroUsize,
    /// number of Internal DNS nodes
    pub ninternal_dns: NonZeroUsize,
}
