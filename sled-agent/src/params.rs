// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_sled_agent_shared::inventory::{OmicronZoneConfig, OmicronZoneType};
use omicron_common::disk::{DatasetKind, DatasetName};
pub use sled_hardware::DendriteAsic;
use std::net::SocketAddrV6;

/// Extension trait for `OmicronZoneType` and `OmicronZoneConfig`.
///
/// This lives here because it requires extra dependencies that
/// nexus-sled-agent-shared doesn't have.
pub(crate) trait OmicronZoneTypeExt {
    fn as_omicron_zone_type(&self) -> &OmicronZoneType;

    /// If this kind of zone has an associated dataset, return the dataset's name.
    /// Otherwise, return `None`.
    fn dataset_name(&self) -> Option<DatasetName> {
        self.dataset_name_and_address().map(|(name, _)| name)
    }

    /// If this kind of zone has an associated dataset, return the dataset's name
    /// and the associated "service address". Otherwise, return `None`.
    fn dataset_name_and_address(&self) -> Option<(DatasetName, SocketAddrV6)> {
        let (dataset, dataset_kind, address) = match self.as_omicron_zone_type()
        {
            OmicronZoneType::BoundaryNtp { .. }
            | OmicronZoneType::InternalNtp { .. }
            | OmicronZoneType::Nexus { .. }
            | OmicronZoneType::Oximeter { .. }
            | OmicronZoneType::CruciblePantry { .. } => None,
            OmicronZoneType::Clickhouse { dataset, address, .. } => {
                Some((dataset, DatasetKind::Clickhouse, address))
            }
            OmicronZoneType::ClickhouseKeeper { dataset, address, .. } => {
                Some((dataset, DatasetKind::ClickhouseKeeper, address))
            }
            OmicronZoneType::ClickhouseServer { dataset, address, .. } => {
                Some((dataset, DatasetKind::ClickhouseServer, address))
            }
            OmicronZoneType::CockroachDb { dataset, address, .. } => {
                Some((dataset, DatasetKind::Cockroach, address))
            }
            OmicronZoneType::Crucible { dataset, address, .. } => {
                Some((dataset, DatasetKind::Crucible, address))
            }
            OmicronZoneType::ExternalDns { dataset, http_address, .. } => {
                Some((dataset, DatasetKind::ExternalDns, http_address))
            }
            OmicronZoneType::InternalDns { dataset, http_address, .. } => {
                Some((dataset, DatasetKind::InternalDns, http_address))
            }
        }?;

        Some((DatasetName::new(dataset.pool_name, dataset_kind), *address))
    }
}

impl OmicronZoneTypeExt for OmicronZoneType {
    fn as_omicron_zone_type(&self) -> &OmicronZoneType {
        self
    }
}

impl OmicronZoneTypeExt for OmicronZoneConfig {
    fn as_omicron_zone_type(&self) -> &OmicronZoneType {
        &self.zone_type
    }
}
