// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod instance {
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use crate::v1::instance::InstanceMigrationTargetParams;
    use crate::v1::instance::VmmStateRequested as VmmStateRequestedV1;

    /// Requestable running state of an Instance.
    ///
    /// A subset of [`omicron_common::api::external::InstanceState`].
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
            /// The number of seconds to wait between sending the VM guest a power
            /// button signal and forcefully terminating it, if it does not power
            /// itself down by then after receiving the signal.
            /// If none, it is stopped with no prior power button signal.
            acpi_timeout_secs: Option<u64>,
        },
        /// Immediately reset the instance, as though it had stopped and immediately
        /// began to run again.
        Reboot {
            /// The number of seconds to wait between sending the VM guest a power
            /// button signal and forcefully rebooting it, if it does not power
            /// itself down by then after receiving the signal.
            /// If none, it is rebooted with no prior power button signal.
            acpi_timeout_secs: Option<u64>,
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
                VmmStateRequestedV1::Stopped => {
                    VmmStateRequested::Stopped { acpi_timeout_secs: None }
                }
                VmmStateRequestedV1::Reboot => {
                    VmmStateRequested::Reboot { acpi_timeout_secs: None }
                }
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
}
