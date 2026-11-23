// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `INSTANCE_ATTACHED_SUBNETS_MAP` of the Sled Agent API.
//!
//! This version changes `InstanceSledLocalConfig::attached_subnets` from a
//! `Vec` to an `IdOrdMap` keyed by subnet, rejecting duplicates.

pub mod instance;
