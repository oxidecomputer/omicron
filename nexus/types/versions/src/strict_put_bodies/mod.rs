// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `STRICT_PUT_BODIES` of the Nexus external API.
//!
//! Converts several update (PUT) request bodies from the `Option`-everywhere
//! "PATCH-via-PUT" pattern to strict PUT bodies: non-nullable resource fields
//! become required, and clearable fields become `Nullable<T>` (present on the
//! wire, possibly explicit `null`). The affected bodies are `ProjectUpdate`,
//! `VpcSubnetUpdate`, `SiloQuotasUpdate`, and `SupportBundleUpdate`.

pub mod identity;
pub mod project;
pub mod silo;
pub mod support_bundle;
pub mod vpc;
