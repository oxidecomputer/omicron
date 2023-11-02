// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generic deployment planner types and implementation
//!
//! See crate-level docs for details.

use crate::policy::Policy;
use nexus_types::inventory::Collection;

/// Pluggable planner implementation
///
/// This is a trait so that we can provide various impls that cover various
/// parts of the plan.  For example, one impl might only know about MGS-managed
/// software (the SP, RoT, and host OS software).  Another impl might only know
/// about Omicron zones.
pub trait Planner {
    /// Returns whether the given plan is consistent with the given policy
    fn plan_acceptable(
        &self,
        policy: &Policy,
        latest_collection: Option<&Collection>,
        plan: &Plan,
    ) -> PlanAcceptability;

    /// Uses `builder` to generate a new plan consistent with the latest policy
    /// and collection
    fn plan_generate(
        &self,
        policy: &Policy,
        latest_collection: Option<&Collection>,
        builder: &mut PlanBuilder,
    ) -> anyhow::Result<()>;
}

pub enum PlanAcceptability {
    /// the plan is consistent with the policy
    Acceptable,
    /// the plan is not consistent with the policy
    Unacceptable {
        /// reasons why the plan is not consistent with the policy
        reasons: Vec<String>,
    },
}

// XXX-dap
pub struct Plan {}

// XXX-dap
pub struct PlanBuilder {}
