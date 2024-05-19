// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Symbolic operations against a symbolic [`super::Fleet`] that are "executed"
//! by the [`super::TestHarness`].

/// All symbolic operations against a `Fleet`.
///
/// Each operation results in one set of behaviors during symbolic execution
/// and another during dynamic (concrete) execution.
pub enum Op {
    /// Symbolic: Add a new sled to `DbState`
    ///
    /// Dynamic: Add the new sled to the concrete `PlanningInput`
    ///
    /// Preconditions:
    ///   1. This sled must not already have been addded to a rack
    ///   2. There must be room in a rack for a new sled
    AddSledToDbState,

    /// Symbolic: Add a new sled to a `Rack`, along with any resources such
    /// as zones and disks.
    ///
    /// Dynamic: Add the sled resources that would be initially deployed, such
    /// as the NTP zone, to a `Collection`.
    ///
    /// Precondition: The sled must already have been added with
    /// `AddSledToDbState`.
    DeployInitialZones,

    /// Symbolic: Do nothing
    ///
    /// Dynamic: Take the current concrete `PlanningInput`, `Collection`, and
    /// `Blueprint` created as a result of prior operations and utilize the
    /// `Planner` to generate a new concrete blueprint.
    GenerateBlueprint,
}
