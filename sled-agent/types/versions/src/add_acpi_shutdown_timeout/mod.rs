// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod instance {
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use crate::v1::instance::InstanceMigrationTargetParams;
    use crate::v1::instance::VmmStateRequested as VmmStateRequestedV1;
    use omicron_common::api::external::InstanceShutdownPolicy;

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
}
