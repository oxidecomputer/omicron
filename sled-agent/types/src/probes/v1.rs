// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version 1 of types for manipulating networking probe zones.

use iddqd::IdHashItem;
use iddqd::IdHashMap;
use iddqd::id_upcast;
use omicron_common::api::external;
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface;
use omicron_uuid_kinds::ProbeUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

// Re-export types that haven't changed.
pub use super::ExternalIp;
pub use super::IpKind;

/// Parameters used to create a probe.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProbeCreate {
    /// The ID for the probe.
    pub id: ProbeUuid,
    /// The external IP addresses assigned to the probe.
    pub external_ips: Vec<ExternalIp>,
    /// The probe's networking interface.
    pub interface: NetworkInterface,
}

impl IdHashItem for ProbeCreate {
    type Key<'a> = ProbeUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

impl TryFrom<ProbeCreate> for super::ProbeCreate {
    type Error = external::Error;

    fn try_from(value: ProbeCreate) -> Result<Self, Self::Error> {
        value.interface.try_into().map(|interface| Self {
            id: value.id,
            external_ips: value.external_ips,
            interface,
        })
    }
}

/// A set of probes that the target sled should run.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProbeSet {
    /// The exact set of probes to run.
    pub probes: IdHashMap<ProbeCreate>,
}

impl TryFrom<ProbeSet> for super::ProbeSet {
    type Error = external::Error;

    fn try_from(value: ProbeSet) -> Result<Self, Self::Error> {
        value
            .probes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
            .map(|probes| Self { probes })
    }
}
