// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Versioned types for the Nexus external API.
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

mod impls;
pub mod latest;
#[path = "initial/mod.rs"]
pub mod v2025112000;
#[path = "local_storage/mod.rs"]
pub mod v2025120300;
#[path = "bgp_peer_collision_state/mod.rs"]
pub mod v2025121200;
#[path = "ip_version_and_multiple_default_pools/mod.rs"]
pub mod v2025122300;
#[path = "silo_project_ip_version_and_pool_type/mod.rs"]
pub mod v2026010100;
#[path = "dual_stack_nics/mod.rs"]
pub mod v2026010300;
#[path = "pool_selection_enums/mod.rs"]
pub mod v2026010500;
