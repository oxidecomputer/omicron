// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1::instance::InstanceMigrationTargetParams;
use crate::v1::instance::VmmStateRequested as VmmStateRequestedV1;

/// Instance-stopping method choices
#[derive(
    Clone,
    Copy,
    Default,
    Debug,
    Eq,
    PartialEq,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum InstanceShutdownPolicy {
    /// Instance's VM is forcefully terminated immediately.
    // default to hard-off, preserving the existing behavior of our system prior
    // to ACPI shutdown support
    #[default]
    HardOff,
    /// Instance is sent PWRBTN_STS as described in ACPI chapter 4. If the VM
    /// guest has not stopped itself with a power event within the given
    /// timeout, it is *then* forcefully terminated.
    PowerButton { timeout_secs: AcpiTimeoutSecs },
}

/// ACPI power button timeout in seconds.
// RFD 622: "A user-configurable timeout for the instance to finish stopping
// itself should be present, bounded between 1 and 3600 seconds, inclusive."
// This is enforced at the API level (by this type and its `Deserialize` impl)
// and as a crdb constraint.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(try_from = "u64", into = "u64")]
pub struct AcpiTimeoutSecs(#[schemars(range(min = 1, max = 3600))] u64);

// schemars(range(max)) above is required to be a *literal*. if the constant
// changes, and you're here because of the assertion, change it accordingly.
const _: () = const { assert!(AcpiTimeoutSecs::MIN == 1) };
const _: () = const { assert!(AcpiTimeoutSecs::MAX == 3600) };

// All the impls on `AcpiTimeoutSecs` live here instead of `crate::impls`
// because they're required as a part of implementing `Deserialize`.

impl AcpiTimeoutSecs {
    const MIN: u64 = 1;
    const MAX: u64 = 3600;
}

impl From<AcpiTimeoutSecs> for u64 {
    fn from(value: AcpiTimeoutSecs) -> Self {
        value.0
    }
}

// Some consumers of this need it to be an `i64` (e.g., to convert it to a
// `TimeDelta` for storage in the db). We can provide an infallible conversion
// for it since we know the range we enforce is valid for both u64 and i64.
impl From<AcpiTimeoutSecs> for i64 {
    fn from(value: AcpiTimeoutSecs) -> Self {
        value.0.try_into().expect("AcpiTimeoutSecs range is always a valid i64")
    }
}

impl TryFrom<u64> for AcpiTimeoutSecs {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value < Self::MIN || value > Self::MAX {
            anyhow::bail!(
                "timeout_secs must be between {} and {} (got {value})",
                Self::MIN,
                Self::MAX
            );
        }
        Ok(Self(value))
    }
}

/// Requestable running state of an Instance.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum VmmStateRequested {
    /// Run this instance by migrating in from a previous running incarnation of
    /// the instance.
    MigrationTarget(InstanceMigrationTargetParams),
    /// Start the instance if it is not already running.
    Running,
    /// Stop the instance.
    Stopped {
        /// The method by which the instance should be stopped *this* time.
        /// May be `HardOff` for an instance whose policy is `PowerButton`
        /// if the operator wants to use force.
        method: InstanceShutdownPolicy,
    },
    /// Immediately reset the instance, as though it had stopped and immediately
    /// began to run again.
    Reboot {
        /// The method by which the instance should be rebooted *this* time.
        /// May be `HardOff` for an instance whose policy is `PowerButton`
        /// if the operator wants to use force.
        method: InstanceShutdownPolicy,
    },
}

impl Into<VmmStateRequested> for VmmStateRequestedV1 {
    fn into(self) -> VmmStateRequested {
        match self {
            VmmStateRequestedV1::MigrationTarget(
                instance_migration_target_params,
            ) => VmmStateRequested::MigrationTarget(
                instance_migration_target_params,
            ),
            VmmStateRequestedV1::Running => VmmStateRequested::Running,
            VmmStateRequestedV1::Stopped => VmmStateRequested::Stopped {
                method: InstanceShutdownPolicy::HardOff,
            },
            VmmStateRequestedV1::Reboot => VmmStateRequested::Reboot {
                method: InstanceShutdownPolicy::HardOff,
            },
        }
    }
}

/// The body of a request to move a previously-ensured instance into a specific
/// runtime state.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct VmmPutStateBody {
    /// The state into which the instance should be driven.
    pub state: VmmStateRequested,
}

impl Into<VmmPutStateBody> for crate::v1::instance::VmmPutStateBody {
    fn into(self) -> VmmPutStateBody {
        VmmPutStateBody { state: self.state.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acpi_timeout_secs_range() {
        // Test some numbers outside `AcpiTimeoutSecs`'s range in both
        // directions: deserialization and the `TryFrom<u64>` should only accept
        // values in the valid range.
        for i in -100..(AcpiTimeoutSecs::MAX as i64) + 10 {
            let deserialize_result =
                serde_json::from_str::<AcpiTimeoutSecs>(&i.to_string());

            if matches!(
                u64::try_from(i),
                Ok(AcpiTimeoutSecs::MIN..=AcpiTimeoutSecs::MAX)
            ) {
                let expected = AcpiTimeoutSecs(i as u64);
                assert_eq!(deserialize_result.unwrap(), expected);
                assert_eq!(
                    AcpiTimeoutSecs::try_from(i as u64).unwrap(),
                    expected
                );
                assert_eq!(u64::from(expected), i as u64);
                assert_eq!(i64::from(expected), i);
            } else {
                assert!(deserialize_result.is_err());
                if let Ok(i) = u64::try_from(i) {
                    assert!(AcpiTimeoutSecs::try_from(i).is_err());
                }
            }
        }
    }
}
