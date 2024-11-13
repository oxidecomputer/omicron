// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Silo-related utilities and fixed data.

use std::sync::LazyLock;

use omicron_common::api::external::Name;

/// The ID of the "default" silo.
pub static DEFAULT_SILO_ID: uuid::Uuid =
    uuid::Uuid::from_u128(0x001de000_5110_4000_8000_000000000000);

/// Return the name of the default silo.
pub fn default_silo_name() -> &'static Name {
    static DEFAULT_SILO_NAME: LazyLock<Name> =
        LazyLock::new(|| "default-silo".parse().unwrap());

    &DEFAULT_SILO_NAME
}

/// The ID of the built-in internal silo.
pub static INTERNAL_SILO_ID: uuid::Uuid =
    uuid::Uuid::from_u128(0x001de000_5110_4000_8000_000000000001);

/// Return the name of the internal silo.
pub fn internal_silo_name() -> &'static Name {
    static INTERNAL_SILO_NAME: LazyLock<Name> =
        LazyLock::new(|| "oxide-internal".parse().unwrap());

    &INTERNAL_SILO_NAME
}

/// Returns the (relative) DNS name for this Silo's API and console endpoints
/// _within_ the external DNS zone (i.e., without that zone's suffix)
///
/// This specific naming scheme is determined under RFD 357.
pub fn silo_dns_name(name: &omicron_common::api::external::Name) -> String {
    // RFD 4 constrains resource names (including Silo names) to DNS-safe
    // strings, which is why it's safe to directly put the name of the
    // resource into the DNS name rather than doing any kind of escaping.
    format!("{}.sys", name)
}
