// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Probe types for version `ADD_ROUTER_LISTS`.

use crate::v10;
use crate::v10::inventory::NetworkInterface;
use crate::v10::probes::ExternalIp;
use iddqd::IdHashItem;
use iddqd::IdHashMap;
use iddqd::id_upcast;
use omicron_common::api::internal::shared::RouterListEntry;
use omicron_uuid_kinds::ProbeUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Parameters used to create a probe.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProbeCreate {
    /// The ID for the probe.
    pub id: ProbeUuid,
    /// The external IP addresses assigned to the probe.
    pub external_ips: Vec<ExternalIp>,
    /// The probe's networking interface.
    pub interface: NetworkInterface,
    /// The tunnel-router list the probe's OPTE port is born with. Empty
    /// means no tunnel routers (no external egress).
    pub router_list: Vec<RouterListEntry>,
}

impl IdHashItem for ProbeCreate {
    type Key<'a> = ProbeUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

/// A set of probes that the target sled should run.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProbeSet {
    /// The exact set of probes to run.
    pub probes: IdHashMap<ProbeCreate>,
}

impl From<v10::probes::ProbeCreate> for ProbeCreate {
    fn from(old: v10::probes::ProbeCreate) -> Self {
        Self {
            id: old.id,
            external_ips: old.external_ips,
            interface: old.interface,
            // Fail closed: the router-lists background task corrects drift.
            router_list: Vec::new(),
        }
    }
}

impl From<v10::probes::ProbeSet> for ProbeSet {
    fn from(old: v10::probes::ProbeSet) -> Self {
        Self { probes: old.probes.into_iter().map(Into::into).collect() }
    }
}
