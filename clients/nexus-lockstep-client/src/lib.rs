// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to the Oxide control plane at large from
//! callers that update in lockstep with Nexus itself (e.g. rack initialization,
//! tests and debugging)

use std::collections::HashMap;

use iddqd::IdOrdItem;
use iddqd::id_upcast;
use uuid::Uuid;

progenitor::generate_api!(
    spec = "../../openapi/nexus-lockstep.json",
    interface = Positional,
    derives = [schemars::JsonSchema, PartialEq],
    inner_type = slog::Logger,
    pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
        slog::debug!(log, "client request";
            "method" => %request.method(),
            "uri" => %request.url(),
            "body" => ?&request.body(),
        );
    }),
    post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
        slog::debug!(log, "client response"; "result" => ?result);
    }),
    crates = {
        "iddqd" = "*",
        "omicron-uuid-kinds" = "*",
        "oxnet" = "0.1.0",
    },
    replace = {
        // It's kind of unfortunate to pull in such a complex and unstable type
        // as "blueprint" this way, but we have really useful functionality
        // (e.g., diff'ing) that's implemented on our local type.
        Blueprint = nexus_types::deployment::Blueprint,
        BlueprintDatasetConfig = nexus_types::deployment::BlueprintDatasetConfig,
        BlueprintPhysicalDiskConfig = nexus_types::deployment::BlueprintPhysicalDiskConfig,
        BlueprintPhysicalDiskDisposition = nexus_types::deployment::BlueprintPhysicalDiskDisposition,
        BlueprintZoneConfig = nexus_types::deployment::BlueprintZoneConfig,
        BlueprintZoneImageSource = nexus_types::deployment::BlueprintZoneImageSource,
        Certificate = omicron_common::api::internal::nexus::Certificate,
        ClickhouseMode = nexus_types::deployment::ClickhouseMode,
        ClickhousePolicy = nexus_types::deployment::ClickhousePolicy,
        DatasetKind = omicron_common::api::internal::shared::DatasetKind,
        DnsConfigParams = nexus_types::internal_api::params::DnsConfigParams,
        DnsConfigZone = nexus_types::internal_api::params::DnsConfigZone,
        DnsRecord = nexus_types::internal_api::params::DnsRecord,
        Generation = omicron_common::api::external::Generation,
        ImportExportPolicy = omicron_common::api::external::ImportExportPolicy,
        MacAddr = omicron_common::api::external::MacAddr,
        MgsUpdateDriverStatus = nexus_types::internal_api::views::MgsUpdateDriverStatus,
        Name = omicron_common::api::external::Name,
        NetworkInterface = omicron_common::api::internal::shared::NetworkInterface,
        NetworkInterfaceKind = omicron_common::api::internal::shared::NetworkInterfaceKind,
        NewPasswordHash = omicron_passwords::NewPasswordHash,
        OximeterReadMode = nexus_types::deployment::OximeterReadMode,
        OximeterReadPolicy = nexus_types::deployment::OximeterReadPolicy,
        PendingMgsUpdate = nexus_types::deployment::PendingMgsUpdate,
        PlannerConfig = nexus_types::deployment::PlannerConfig,
        ReconfiguratorConfig = nexus_types::deployment::ReconfiguratorConfig,
        ReconfiguratorConfigParam = nexus_types::deployment::ReconfiguratorConfigParam,
        ReconfiguratorConfigView = nexus_types::deployment::ReconfiguratorConfigView,
        RecoverySiloConfig = nexus_sled_agent_shared::recovery_silo::RecoverySiloConfig,
        SledAgentUpdateStatus = nexus_types::internal_api::views::SledAgentUpdateStatus,
        UpdateStatus = nexus_types::internal_api::views::UpdateStatus,
        ZoneStatus = nexus_types::internal_api::views::ZoneStatus,
        ZpoolName = omicron_common::zpool_name::ZpoolName,
    },
    patch = {
        ByteCount = { derives = [PartialEq, Eq] },
        Baseboard = { derives = [PartialEq, Eq] }
    }
);

impl IdOrdItem for types::PendingSagaInfo {
    type Key<'a> = Uuid;

    fn key(&self) -> Self::Key<'_> {
        self.saga_id
    }

    id_upcast!();
}

impl IdOrdItem for types::HeldDbClaimInfo {
    type Key<'a> = u64;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

impl From<std::time::Duration> for types::Duration {
    fn from(s: std::time::Duration) -> Self {
        Self { secs: s.as_secs(), nanos: s.subsec_nanos() }
    }
}

impl From<types::Duration> for std::time::Duration {
    fn from(s: types::Duration) -> Self {
        std::time::Duration::from_nanos(
            s.secs * 1000000000 + u64::from(s.nanos),
        )
    }
}

impl From<omicron_common::address::IpRange> for types::IpRange {
    fn from(r: omicron_common::address::IpRange) -> Self {
        use omicron_common::address::IpRange;
        match r {
            IpRange::V4(r) => types::IpRange::V4(r.into()),
            IpRange::V6(r) => types::IpRange::V6(r.into()),
        }
    }
}

impl From<omicron_common::address::Ipv4Range> for types::Ipv4Range {
    fn from(r: omicron_common::address::Ipv4Range) -> Self {
        Self { first: r.first, last: r.last }
    }
}

impl From<omicron_common::address::Ipv6Range> for types::Ipv6Range {
    fn from(r: omicron_common::address::Ipv6Range) -> Self {
        Self { first: r.first, last: r.last }
    }
}

impl From<&omicron_common::api::internal::shared::SourceNatConfigGeneric>
    for types::SourceNatConfigGeneric
{
    fn from(
        r: &omicron_common::api::internal::shared::SourceNatConfigGeneric,
    ) -> Self {
        let (first_port, last_port) = r.port_range_raw();
        Self { ip: r.ip, first_port, last_port }
    }
}

impl From<omicron_common::api::internal::shared::PortSpeed>
    for types::PortSpeed
{
    fn from(value: omicron_common::api::internal::shared::PortSpeed) -> Self {
        match value {
            omicron_common::api::internal::shared::PortSpeed::Speed0G => {
                types::PortSpeed::Speed0G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed1G => {
                types::PortSpeed::Speed1G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed10G => {
                types::PortSpeed::Speed10G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed25G => {
                types::PortSpeed::Speed25G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed40G => {
                types::PortSpeed::Speed40G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed50G => {
                types::PortSpeed::Speed50G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed100G => {
                types::PortSpeed::Speed100G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed200G => {
                types::PortSpeed::Speed200G
            }
            omicron_common::api::internal::shared::PortSpeed::Speed400G => {
                types::PortSpeed::Speed400G
            }
        }
    }
}

impl From<omicron_common::api::internal::shared::PortFec> for types::PortFec {
    fn from(value: omicron_common::api::internal::shared::PortFec) -> Self {
        match value {
            omicron_common::api::internal::shared::PortFec::Firecode => {
                types::PortFec::Firecode
            }
            omicron_common::api::internal::shared::PortFec::None => {
                types::PortFec::None
            }
            omicron_common::api::internal::shared::PortFec::Rs => {
                types::PortFec::Rs
            }
        }
    }
}

impl From<omicron_common::api::internal::shared::SwitchLocation>
    for types::SwitchLocation
{
    fn from(
        value: omicron_common::api::internal::shared::SwitchLocation,
    ) -> Self {
        match value {
            omicron_common::api::internal::shared::SwitchLocation::Switch0 => {
                types::SwitchLocation::Switch0
            }
            omicron_common::api::internal::shared::SwitchLocation::Switch1 => {
                types::SwitchLocation::Switch1
            }
        }
    }
}

impl From<omicron_common::api::internal::shared::ExternalPortDiscovery>
    for types::ExternalPortDiscovery
{
    fn from(
        value: omicron_common::api::internal::shared::ExternalPortDiscovery,
    ) -> Self {
        match value {
            omicron_common::api::internal::shared::ExternalPortDiscovery::Auto(val) => {
                let new: HashMap<_, _> = val.iter().map(|(slot, addr)| {
                    (slot.to_string(), *addr)
                }).collect();
                types::ExternalPortDiscovery::Auto(new)
            },
            omicron_common::api::internal::shared::ExternalPortDiscovery::Static(val) => {
                let new: HashMap<_, _> = val.iter().map(|(slot, ports)| {
                    (slot.to_string(), ports.clone())
                }).collect();
                types::ExternalPortDiscovery::Static(new)
            },
        }
    }
}

impl From<&omicron_common::api::external::AllowedSourceIps>
    for types::AllowedSourceIps
{
    fn from(ips: &omicron_common::api::external::AllowedSourceIps) -> Self {
        use omicron_common::api::external::AllowedSourceIps;
        match ips {
            AllowedSourceIps::Any => types::AllowedSourceIps::Any,
            AllowedSourceIps::List(list) => {
                types::AllowedSourceIps::List(list.iter().cloned().collect())
            }
        }
    }
}
