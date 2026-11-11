// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2026_01_16_01;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Utilization of addresses in a subnet pool.
///
/// Note that both the count of remaining addresses and the total capacity are
/// integers, reported as floating point numbers. This accommodates allocations
/// larger than a 64-bit integer, which is common with IPv6 address spaces. With
/// very large subnet pools (> 2**53 addresses), integer precision will be lost,
/// in exchange for representing the entire range. In such a case the pool still
/// has many available addresses.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolUtilization {
    /// The number of remaining addresses in the pool.
    pub remaining: f64,
    /// The total number of addresses in the pool.
    pub capacity: f64,
}

// Response-only type: convert from new to old.
impl From<SubnetPoolUtilization>
    for v2026_01_16_01::subnet_pool::SubnetPoolUtilization
{
    fn from(new: SubnetPoolUtilization) -> Self {
        Self { allocated: new.capacity - new.remaining, capacity: new.capacity }
    }
}
