// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management

use nexus_types::fm;
use nexus_types::inventory;

pub struct DiagnosisInput<'a> {
    inventory: &'a inventory::Collection,
    parent_sitrep: Option<&'a fm::Sitrep>,
}
