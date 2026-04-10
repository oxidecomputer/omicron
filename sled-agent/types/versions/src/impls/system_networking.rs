// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for system networking types.

use crate::latest::system_networking::ServiceZoneNatEntries;
use crate::latest::system_networking::ServiceZoneNatEntry;
use crate::latest::system_networking::ServiceZoneNatKind;
use iddqd::IdOrdMap;
use omicron_common::address::MAX_PORT;
use std::net::IpAddr;
use std::ops::Deref;

/// [`ServiceZoneNatEntries`] is a newtype wrapper around an
/// [`IdOrdMap<ServiceZoneNatEntry>`] with additional requirements on the
/// contents that are enforced on construction. We implement [`Deref`] to allow
/// access to all the underlying map's immutable methods, but explicitly do not
/// implement `DerefMut`: mutable access would allow consumers to change the
/// contents, potentially invalidating the checks performed on construction.
impl Deref for ServiceZoneNatEntries {
    type Target = IdOrdMap<ServiceZoneNatEntry>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ServiceZoneNatKind {
    /// Return the external IP address described by this NAT kind.
    pub fn external_ip(&self) -> IpAddr {
        match self {
            ServiceZoneNatKind::BoundaryNtp { snat_cfg } => snat_cfg.ip,
            ServiceZoneNatKind::ExternalDns { external_ip }
            | ServiceZoneNatKind::Nexus { external_ip } => *external_ip,
        }
    }

    /// Return the raw `[first_port, last_port]` range described by this NAT
    /// kind.
    pub fn nat_port_range(&self) -> (u16, u16) {
        match self {
            ServiceZoneNatKind::BoundaryNtp { snat_cfg } => {
                snat_cfg.port_range_raw()
            }
            ServiceZoneNatKind::ExternalDns { external_ip: _ }
            | ServiceZoneNatKind::Nexus { external_ip: _ } => (0, MAX_PORT),
        }
    }
}

// Implement `Arbitrary` for `ServiceZoneNatEntries` so we can use it in
// property-based tests.
#[cfg(any(test, feature = "testing"))]
impl proptest::arbitrary::Arbitrary for ServiceZoneNatEntries {
    type Parameters = ();
    type Strategy = proptest::prelude::BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use omicron_common::api::internal::shared::SourceNatConfigGeneric;
        use proptest::prelude::*;

        // We know `try_from` will fail if we don't have at least one boundary
        // NTP, external DNS, and Nexus, so start with that. We still have to
        // filter through try_from() since we might produce overlapping NAT
        // entries.
        let ntp = SourceNatConfigGeneric::arbitrary()
            .prop_map(|snat_cfg| ServiceZoneNatKind::BoundaryNtp { snat_cfg });
        let dns = IpAddr::arbitrary().prop_map(|external_ip| {
            ServiceZoneNatKind::ExternalDns { external_ip }
        });
        let nexus = IpAddr::arbitrary()
            .prop_map(|external_ip| ServiceZoneNatKind::Nexus { external_ip });

        let ntp = (ServiceZoneNatEntry::arbitrary(), ntp).prop_map(
            |(mut entry, ntp)| {
                entry.kind = ntp;
                entry
            },
        );
        let dns = (ServiceZoneNatEntry::arbitrary(), dns).prop_map(
            |(mut entry, dns)| {
                entry.kind = dns;
                entry
            },
        );
        let nexus = (ServiceZoneNatEntry::arbitrary(), nexus).prop_map(
            |(mut entry, nexus)| {
                entry.kind = nexus;
                entry
            },
        );

        (ntp, dns, nexus, Vec::<ServiceZoneNatEntry>::arbitrary())
            .prop_filter_map(
                "ServiceZoneNatEntries::try_from() failed",
                |(ntp, dns, nexus, extra)| {
                    let mut entries = IdOrdMap::new();
                    entries.insert_overwrite(ntp);
                    entries.insert_overwrite(dns);
                    entries.insert_overwrite(nexus);
                    for e in extra {
                        entries.insert_overwrite(e);
                    }
                    ServiceZoneNatEntries::try_from(entries).ok()
                },
            )
            .boxed()
    }
}
