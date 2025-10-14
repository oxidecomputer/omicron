// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::{IpAddr, Ipv6Addr};

use illumos_utils::opte::{
    Error, Port, PortCreateParams, PortManager, PortTicket,
};
use nexus_types::inventory::{NetworkInterfaceKind, SourceNatConfig};
use omicron_common::api::{
    external,
    internal::shared::{
        ExternalIpGatewayMap, ResolvedVpcFirewallRule, ResolvedVpcRouteSet,
        ResolvedVpcRouteState, VirtualNetworkInterfaceHost,
    },
};
use oximeter_instruments::kstat::KstatSemaphore;
use uuid::Uuid;

/// A wrapper around illumos-utils's PortManager to allow for accesses to be
/// protected by a semaphore.
///
/// All operations that result in an ioctl to XDE (i.e. that result in an upcall
/// to the XDE driver) are guarded by the semaphore.
///
/// This is a temporary workaround for oxidecomputer/opte#758.
#[derive(Clone, Debug)]
pub(crate) struct SledAgentPortManager {
    semaphore: KstatSemaphore,
    port_manager: PortManager,
}

impl SledAgentPortManager {
    pub(crate) fn new(
        log: slog::Logger,
        semaphore: KstatSemaphore,
        underlay_ip: Ipv6Addr,
    ) -> Self {
        let port_manager = PortManager::new(log, underlay_ip);
        Self { semaphore, port_manager }
    }

    pub(crate) fn underlay_ip(&self) -> &Ipv6Addr {
        // no ioctl (just reads in-memory state)
        self.port_manager.underlay_ip()
    }

    pub(crate) fn create_port(
        &self,
        params: PortCreateParams,
    ) -> Result<(Port, PortTicket), Error> {
        // ioctl => must be guarded by the semaphore
        self.semaphore.run(|| self.port_manager.create_port(params))
    }

    pub(crate) fn vpc_routes_list(&self) -> Vec<ResolvedVpcRouteState> {
        // no ioctl (just reads in-memory state)
        self.port_manager.vpc_routes_list()
    }

    pub(crate) fn vpc_routes_ensure(
        &self,
        new_routes: Vec<ResolvedVpcRouteSet>,
    ) -> Result<(), Error> {
        // ioctl => must be guarded by the semaphore
        self.semaphore.run(|| self.port_manager.vpc_routes_ensure(new_routes))
    }

    pub(crate) fn set_eip_gateways(
        &self,
        mappings: ExternalIpGatewayMap,
    ) -> bool {
        // no ioctl (just an in-memory set), no need to be guarded
        self.port_manager.set_eip_gateways(mappings)
    }

    pub(crate) fn external_ips_ensure(
        &self,
        nic_id: Uuid,
        nic_kind: NetworkInterfaceKind,
        source_nat: Option<SourceNatConfig>,
        ephemeral_ip: Option<IpAddr>,
        floating_ips: &[IpAddr],
    ) -> Result<(), Error> {
        // ioctl => must be guarded by the semaphore
        self.semaphore.run(|| {
            self.port_manager.external_ips_ensure(
                nic_id,
                nic_kind,
                source_nat,
                ephemeral_ip,
                floating_ips,
            )
        })
    }

    pub(crate) fn firewall_rules_ensure(
        &self,
        vni: external::Vni,
        rules: &[ResolvedVpcFirewallRule],
    ) -> Result<(), Error> {
        // ioctl => must be guarded by the semaphore
        self.semaphore
            .run(|| self.port_manager.firewall_rules_ensure(vni, rules))
    }

    pub(crate) fn list_virtual_nics(
        &self,
    ) -> Result<Vec<VirtualNetworkInterfaceHost>, Error> {
        // ioctl => must be guarded by the semaphore (unfortunately, because
        // this is a read operation)
        self.semaphore.run(|| self.port_manager.list_virtual_nics())
    }

    pub(crate) fn set_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        // ioctl => must be guarded by the semaphore
        self.semaphore.run(|| self.port_manager.set_virtual_nic_host(mapping))
    }

    pub(crate) fn unset_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        // ioctl => must be guarded by the semaphore
        self.semaphore.run(|| self.port_manager.unset_virtual_nic_host(mapping))
    }
}
