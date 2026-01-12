// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Versioned types for the Sled Agent API.
//!
//! # Adding a new API version
//!
//! When adding a new API version N with added or changed types:
//!
//! 1. Create `<version_name>/mod.rs`, where `<version_name>` is the lowercase
//!    form of the new version's identifier, as defined in the API trait's
//!    `api_versions!` macro.
//!
//! 2. Add to the end of this list:
//!
//!    ```rust,ignore
//!    #[path = "<version_name>/mod.rs"]
//!    pub mod vN;
//!    ```
//!
//! 3. Add your types to the new module, mirroring the module structure from
//!    earlier versions.
//!
//! 4. Update `latest.rs` with new and updated types from the new version.
//!
//! For more information, see the [detailed guide] and [RFD 619].
//!
//! [detailed guide]: https://github.com/oxidecomputer/dropshot-api-manager/blob/main/guides/new-version.md
//! [RFD 619]: https://rfd.shared.oxide.computer/rfd/619

#[path = "bootstrap_initial/mod.rs"]
pub mod bootstrap_v1;
mod impls;
pub mod latest;
#[path = "initial/mod.rs"]
pub mod v1;
#[path = "add_dual_stack_shared_network_interfaces/mod.rs"]
pub mod v10;
#[path = "add_dual_stack_external_ip_config/mod.rs"]
pub mod v11;
#[path = "add_health_monitor/mod.rs"]
pub mod v12;
#[path = "add_trust_quorum/mod.rs"]
pub mod v13;
#[path = "measurements/mod.rs"]
pub mod v14;
#[path = "add_health_monitor_zpools/mod.rs"]
pub mod v15;
#[path = "add_switch_zone_operator_policy/mod.rs"]
pub mod v3;
#[path = "add_nexus_lockstep_port_to_inventory/mod.rs"]
pub mod v4;
#[path = "add_probe_put_endpoint/mod.rs"]
pub mod v6;
#[path = "multicast_support/mod.rs"]
pub mod v7;
#[path = "delegate_zvol_to_propolis/mod.rs"]
pub mod v9;
