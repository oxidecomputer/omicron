// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manager for all OPTE ports on a Helios system

use crate::dladm::OPTE_LINK_PREFIX;
use crate::opte::Error;
use crate::opte::Gateway;
use crate::opte::Handle;
use crate::opte::Port;
use crate::opte::Vni;
use crate::opte::opte_firewall_rules;
use crate::opte::port::PortData;
use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use omicron_common::address::IPV4_MULTICAST_RANGE;
use omicron_common::address::IPV6_MULTICAST_RANGE;
use omicron_common::api::external;
use omicron_common::api::internal::shared::ExternalIpGatewayMap;
use omicron_common::api::internal::shared::InternetGatewayRouterTarget;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_common::api::internal::shared::ResolvedVpcFirewallRule;
use omicron_common::api::internal::shared::ResolvedVpcRoute;
use omicron_common::api::internal::shared::ResolvedVpcRouteSet;
use omicron_common::api::internal::shared::ResolvedVpcRouteState;
use omicron_common::api::internal::shared::RouterId;
use omicron_common::api::internal::shared::RouterTarget as ApiRouterTarget;
use omicron_common::api::internal::shared::RouterVersion;
use omicron_common::api::internal::shared::SourceNatConfig;
use omicron_common::api::internal::shared::VirtualNetworkInterfaceHost;
use oxide_vpc::api::AddRouterEntryReq;
use oxide_vpc::api::DelRouterEntryReq;
use oxide_vpc::api::DhcpCfg;
use oxide_vpc::api::Direction;
use oxide_vpc::api::ExternalIpCfg;
use oxide_vpc::api::IpCfg;
use oxide_vpc::api::IpCidr;
use oxide_vpc::api::Ipv4Cfg;
use oxide_vpc::api::Ipv6Cfg;
use oxide_vpc::api::MacAddr;
use oxide_vpc::api::RouterClass;
use oxide_vpc::api::SNat4Cfg;
use oxide_vpc::api::SNat6Cfg;
use oxide_vpc::api::SetExternalIpsReq;
use oxide_vpc::api::VpcCfg;
use oxnet::IpNet;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use uuid::Uuid;

/// Stored routes (and usage count) for a given VPC/subnet.
#[derive(Debug, Default, Clone)]
struct RouteSet {
    version: Option<RouterVersion>,
    routes: HashSet<ResolvedVpcRoute>,
    active_ports: usize,
}

/// Configuration for multicast groups on an OPTE port.
///
/// TODO: This type should be moved to [oxide_vpc::api] when OPTE dependencies
/// are updated, following the same pattern as other VPC configuration types
/// like [ExternalIpCfg], [IpCfg], etc.
///
/// TODO: Eventually remove.
#[derive(Debug, Clone, PartialEq)]
pub struct MulticastGroupCfg {
    /// The multicast group IP address (IPv4 or IPv6).
    pub group_ip: IpAddr,
    /// For Source-Specific Multicast (SSM), list of source addresses.
    pub sources: Vec<IpAddr>,
}

#[derive(Debug)]
struct PortManagerInner {
    log: Logger,

    /// Sequential identifier for each port on the system.
    next_port_id: AtomicU64,

    /// IP address of the hosting sled on the underlay.
    underlay_ip: Ipv6Addr,

    /// Map of all ports, keyed on the interface Uuid and its kind
    /// (which includes the Uuid of the parent instance or service)
    ports: Mutex<BTreeMap<(Uuid, NetworkInterfaceKind), Port>>,

    /// Map of all current resolved routes.
    routes: Mutex<HashMap<RouterId, RouteSet>>,

    /// Mappings of associated Internet Gateways for all External IPs
    /// attached to each NIC.
    ///
    /// IGW IDs are specific to the VPC of each NIC.
    eip_gateways: Mutex<HashMap<Uuid, HashMap<IpAddr, HashSet<Uuid>>>>,
}

impl PortManagerInner {
    fn next_port_name(&self) -> String {
        format!(
            "{}{}",
            OPTE_LINK_PREFIX,
            self.next_port_id.fetch_add(1, Ordering::SeqCst)
        )
    }
}

#[derive(Debug)]
/// Parameters needed to create and configure an OPTE port.
pub struct PortCreateParams<'a> {
    pub nic: &'a NetworkInterface,
    pub source_nat: Option<SourceNatConfig>,
    pub ephemeral_ip: Option<IpAddr>,
    pub floating_ips: &'a [IpAddr],
    pub firewall_rules: &'a [ResolvedVpcFirewallRule],
    pub dhcp_config: DhcpCfg,
}

/// The port manager controls all OPTE ports on a single host.
#[derive(Debug, Clone)]
pub struct PortManager {
    inner: Arc<PortManagerInner>,
}

impl PortManager {
    /// Create a new manager, for creating OPTE ports
    pub fn new(log: Logger, underlay_ip: Ipv6Addr) -> Self {
        let inner = Arc::new(PortManagerInner {
            log,
            next_port_id: AtomicU64::new(0),
            underlay_ip,
            ports: Mutex::new(BTreeMap::new()),
            routes: Mutex::new(Default::default()),
            eip_gateways: Mutex::new(Default::default()),
        });

        Self { inner }
    }

    pub fn underlay_ip(&self) -> &Ipv6Addr {
        &self.inner.underlay_ip
    }

    /// Create an OPTE port
    pub fn create_port(
        &self,
        params: PortCreateParams,
    ) -> Result<(Port, PortTicket), Error> {
        let PortCreateParams {
            nic,
            source_nat,
            ephemeral_ip,
            floating_ips,
            firewall_rules,
            dhcp_config,
        } = params;

        let is_service =
            matches!(nic.kind, NetworkInterfaceKind::Service { .. });
        let is_instance =
            matches!(nic.kind, NetworkInterfaceKind::Instance { .. });

        let mac = *nic.mac;
        let vni = Vni::new(nic.vni).unwrap();
        let subnet = IpNetwork::from(nic.subnet);
        let vpc_subnet = IpCidr::from(subnet);
        let gateway = Gateway::from_subnet(&subnet);

        // Describe the external IP addresses for this port.
        macro_rules! ip_cfg {
            ($ip:expr, $log_prefix:literal, $ip_t:path, $cidr_t:path,
             $ipcfg_e:path, $ipcfg_t:ident, $snat_t:ident) => {{
                let $cidr_t(vpc_subnet) = vpc_subnet else {
                    error!(
                        self.inner.log,
                        concat!($log_prefix, " subnet");
                        "subnet" => ?vpc_subnet,
                    );
                    return Err(Error::InvalidPortIpConfig);
                };
                let $ip_t(gateway_ip) = gateway.ip else {
                    error!(
                        self.inner.log,
                        concat!($log_prefix, " gateway");
                        "gateway_ip" => ?gateway.ip,
                    );
                    return Err(Error::InvalidPortIpConfig);
                };
                let snat = match source_nat {
                    Some(snat) => {
                        let $ip_t(snat_ip) = snat.ip else {
                            error!(
                                self.inner.log,
                                concat!($log_prefix, " SNAT config");
                                "snat_ip" => ?snat.ip,
                            );
                            return Err(Error::InvalidPortIpConfig);
                        };
                        let ports = snat.port_range();
                        Some($snat_t { external_ip: snat_ip.into(), ports })
                    }
                    None => None,
                };
                let ephemeral_ip = match ephemeral_ip {
                    Some($ip_t(ip)) => Some(ip.into()),
                    Some(_) => {
                        error!(
                            self.inner.log,
                            concat!($log_prefix, " ephemeral IP");
                            "ephemeral_ip" => ?ephemeral_ip,
                        );
                        return Err(Error::InvalidPortIpConfig);
                    }
                    None => None,
                };
                let floating_ips: Vec<_> = floating_ips
                    .iter()
                    .copied()
                    .map(|ip| match ip {
                        $ip_t(ip) => Ok(ip.into()),
                        _ => {
                            error!(
                                self.inner.log,
                                concat!($log_prefix, " ephemeral IP");
                                "ephemeral_ip" => ?ephemeral_ip,
                            );
                            Err(Error::InvalidPortIpConfig)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                $ipcfg_e($ipcfg_t {
                    vpc_subnet,
                    private_ip: $ip.into(),
                    gateway_ip: gateway_ip.into(),
                    external_ips: ExternalIpCfg {
                        ephemeral_ip,
                        snat,
                        floating_ips,
                    },
                })
            }}
        }

        // Build the port's IP configuration as either IPv4 or IPv6
        // depending on the IP that was assigned to the NetworkInterface.
        // We use a macro here to be DRY
        // TODO-completeness: Support both dual stack
        let ip_cfg = match nic.ip {
            IpAddr::V4(ip) => ip_cfg!(
                ip,
                "Expected IPv4",
                IpAddr::V4,
                IpCidr::Ip4,
                IpCfg::Ipv4,
                Ipv4Cfg,
                SNat4Cfg
            ),
            IpAddr::V6(ip) => ip_cfg!(
                ip,
                "Expected IPv6",
                IpAddr::V6,
                IpCidr::Ip6,
                IpCfg::Ipv6,
                Ipv6Cfg,
                SNat6Cfg
            ),
        };

        let vpc_cfg = VpcCfg {
            ip_cfg: ip_cfg.clone(),
            guest_mac: MacAddr::from(nic.mac.into_array()),
            gateway_mac: MacAddr::from(gateway.mac.into_array()),
            vni,
            phys_ip: self.inner.underlay_ip.into(),
        };

        // Create the xde device.
        //
        // The sequencing here is important. We'd like to make sure things are
        // cleaned up properly, while having a sequence of fallible operations.
        // So we:
        //
        // - create the xde device
        // - create the port ticket
        // - create the port
        // - add both to the PortManager's map
        //
        // The Port object's drop implementation will clean up both of those, if
        // any of the remaining fallible operations fail.
        let port_name = self.inner.next_port_name();
        debug!(
            self.inner.log,
            "Creating xde device";
            "port_name" => &port_name,
            "vpc_cfg" => ?&vpc_cfg,
            "dhcp_config" => ?&dhcp_config,
        );
        let hdl = {
            let hdl = Handle::new()?;
            hdl.create_xde(
                &port_name,
                vpc_cfg,
                dhcp_config,
                /* passthru = */ false,
            )?;
            hdl
        };
        let (port, ticket) = {
            let mut ports = self.inner.ports.lock().unwrap();
            let ticket = PortTicket::new(nic.id, nic.kind, self.inner.clone());
            let port = Port::new(PortData {
                name: port_name.clone(),
                ip: nic.ip,
                mac,
                slot: nic.slot,
                vni,
                subnet: nic.subnet,
                gateway,
            });
            let old = ports.insert((nic.id, nic.kind), port.clone());
            assert!(
                old.is_none(),
                "Duplicate OPTE port detected: interface_id = {}, kind = {:?}",
                nic.id,
                nic.kind,
            );

            // Ports for Probes/Services cannot have EIP<->IGW mappings filled
            // in dynamically today, so to keep use of their EIPs working we
            // leave them untagged at both the `nat` and `router` layer.
            if is_instance {
                // This is effectively re-asserting the external IP config in order to
                // set the EIP<->IGW mapping. While this should be part of `vpc_cfg`,
                // this currently needs to happen here to prevent a case where an old
                // mapping is not yet removed (and so no 'change' happens to trigger
                // `Instance::refresh_external_ips_inner`), and to prevent updates
                // racing with nexus before an instance/port are reachable from their
                // respective managers.
                self.external_ips_ensure_port(
                    &port,
                    nic.id,
                    source_nat,
                    ephemeral_ip,
                    floating_ips,
                )?;
            }
            (port, ticket)
        };

        // Initialize firewall rules for the new port.
        let rules = opte_firewall_rules(firewall_rules, &vni, &mac);
        debug!(
            self.inner.log,
            "Setting firewall rules";
            "port_name" => &port_name,
            "rules" => ?&rules,
        );
        hdl.set_firewall_rules(&oxide_vpc::api::SetFwRulesReq {
            port_name: port_name.clone(),
            rules,
        })?;

        // Check locally to see whether we have any routes from the
        // control plane for this port already installed. If not,
        // create a record to show that we're interested in receiving
        // those routes.
        let mut route_map = self.inner.routes.lock().unwrap();
        let system_routes =
            route_map.entry(port.system_router_key()).or_insert_with(|| {
                let mut routes = HashSet::new();
                if is_service {
                    // Always insert a rule targeting the _system VPC Internet Gateway_.
                    // This may be sent later from Nexus, but we need it during
                    // bootstrapping NTP or other very early services, before the
                    // control plane database has been started.
                    let target = ApiRouterTarget::InternetGateway(
                        InternetGatewayRouterTarget::System,
                    );
                    routes.insert(ResolvedVpcRoute {
                        dest: IpNet::V4(
                            Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).unwrap(),
                        ),
                        target,
                    });
                    routes.insert(ResolvedVpcRoute {
                        dest: IpNet::V6(
                            Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 0).unwrap(),
                        ),
                        target,
                    });
                }
                RouteSet { version: None, routes, active_ports: 0 }
            });
        system_routes.active_ports += 1;

        // Clone is needed to get borrowck on our side, sadly.
        let system_routes = system_routes.clone();

        let custom_routes = route_map
            .entry(port.custom_router_key())
            .or_insert_with(|| RouteSet {
                version: None,
                routes: HashSet::default(),
                active_ports: 0,
            });
        custom_routes.active_ports += 1;

        for (class, routes) in [
            (RouterClass::System, &system_routes),
            (RouterClass::Custom, custom_routes),
        ] {
            for route in &routes.routes {
                let route = AddRouterEntryReq {
                    class,
                    port_name: port_name.clone(),
                    dest: super::net_to_cidr(route.dest),
                    target: super::router_target_opte(&route.target),
                };

                hdl.add_router_entry(&route)?;

                debug!(
                    self.inner.log,
                    "Added router entry";
                    "port_name" => &port_name,
                    "route" => ?route,
                );
            }
        }

        // If there are any transit IPs set, allow them through.
        // TODO: Currently set only in initial state.
        //       This, external IPs, and cfg'able state
        //       (DHCP?) are probably worth being managed by an RPW.
        for block in &nic.transit_ips {
            // In principle if this were an operation on an existing
            // port, we would explicitly undo the In addition if the
            // Out addition fails.
            // However, failure here will just destroy the port
            // outright -- this should only happen if an excessive
            // number of rules are specified.
            hdl.allow_cidr(
                &port_name,
                super::net_to_cidr(*block),
                Direction::In,
            )?;
            hdl.allow_cidr(
                &port_name,
                super::net_to_cidr(*block),
                Direction::Out,
            )?;

            debug!(
                self.inner.log,
                "Added CIDR to in/out allowlist";
                "port_name" => &port_name,
                "cidr" => ?block,
            );
        }

        info!(
            self.inner.log,
            "Created OPTE port";
            "port" => ?&port,
        );
        Ok((port, ticket))
    }

    pub fn vpc_routes_list(&self) -> Vec<ResolvedVpcRouteState> {
        let routes = self.inner.routes.lock().unwrap();
        routes
            .iter()
            .map(|(k, v)| ResolvedVpcRouteState { id: *k, version: v.version })
            .collect()
    }

    pub fn vpc_routes_ensure(
        &self,
        new_routes: Vec<ResolvedVpcRouteSet>,
    ) -> Result<(), Error> {
        let mut routes = self.inner.routes.lock().unwrap();
        let mut deltas = HashMap::new();
        slog::debug!(self.inner.log, "new routes: {new_routes:#?}");
        for new in new_routes {
            // Disregard any route information for a subnet we don't have.
            let Some(old) = routes.get(&new.id) else {
                slog::warn!(self.inner.log, "ignoring route {new:#?}");
                continue;
            };

            // We have to handle subnet router changes, as well as
            // spurious updates from multiple Nexus instances.
            // If there's a UUID match, only update if vers increased,
            // otherwise take the update verbatim (including loss of version).
            let (to_add, to_delete): (HashSet<_>, HashSet<_>) =
                match (old.version, new.version) {
                    (Some(old_vers), Some(new_vers))
                        if !old_vers.is_replaced_by(&new_vers) =>
                    {
                        slog::info!(
                            self.inner.log,
                            "skipping delta compute for subnet";
                            "subnet" => ?new.id,
                            "old_vers" => ?old_vers,
                            "new_vers" => ?new_vers,
                        );
                        continue;
                    }
                    _ => (
                        new.routes.difference(&old.routes).cloned().collect(),
                        old.routes.difference(&new.routes).cloned().collect(),
                    ),
                };
            deltas.insert(new.id, (to_add, to_delete));

            let active_ports = old.active_ports;
            routes.insert(
                new.id,
                RouteSet {
                    version: new.version,
                    routes: new.routes,
                    active_ports,
                },
            );
        }

        // Note: We're deliberately holding both locks here
        // to prevent several nexuses computng and applying deltas
        // out of order.
        let ports = self.inner.ports.lock().unwrap();
        let hdl = Handle::new()?;

        // Propagate deltas out to all ports.
        for port in ports.values() {
            let system_id = port.system_router_key();
            let system_delta = deltas.get(&system_id);

            let custom_id = port.custom_router_key();
            let custom_delta = deltas.get(&custom_id);

            for (class, delta) in [
                (RouterClass::System, system_delta),
                (RouterClass::Custom, custom_delta),
            ] {
                let Some((to_add, to_delete)) = delta else {
                    debug!(self.inner.log, "vpc route ensure: no delta");
                    continue;
                };

                debug!(self.inner.log, "vpc route ensure to_add: {to_add:#?}");
                debug!(
                    self.inner.log,
                    "vpc router ensure to_delete: {to_delete:#?}"
                );

                for route in to_delete {
                    let route = DelRouterEntryReq {
                        class,
                        port_name: port.name().into(),
                        dest: super::net_to_cidr(route.dest),
                        target: super::router_target_opte(&route.target),
                    };

                    hdl.del_router_entry(&route)?;

                    debug!(
                        self.inner.log,
                        "Removed router entry";
                        "port_name" => &port.name(),
                        "route" => ?route,
                    );
                }

                for route in to_add {
                    let route = AddRouterEntryReq {
                        class,
                        port_name: port.name().into(),
                        dest: super::net_to_cidr(route.dest),
                        target: super::router_target_opte(&route.target),
                    };

                    hdl.add_router_entry(&route)?;

                    debug!(
                        self.inner.log,
                        "Added router entry";
                        "port_name" => &port.name(),
                        "route" => ?route,
                    );
                }
            }
        }

        Ok(())
    }

    /// Set Internet Gateway mappings for all external IPs in use
    /// by attached [NetworkInterface]s.
    ///
    /// Returns whether the internal mappings were changed.
    pub fn set_eip_gateways(&self, mappings: ExternalIpGatewayMap) -> bool {
        let mut gateways = self.inner.eip_gateways.lock().unwrap();

        let changed = &*gateways != &mappings.mappings;

        *gateways = mappings.mappings;

        changed
    }

    /// Lookup an OPTE port, and ensure its external IP config is up to date.
    pub fn external_ips_ensure(
        &self,
        nic_id: Uuid,
        nic_kind: NetworkInterfaceKind,
        source_nat: Option<SourceNatConfig>,
        ephemeral_ip: Option<IpAddr>,
        floating_ips: &[IpAddr],
    ) -> Result<(), Error> {
        let ports = self.inner.ports.lock().unwrap();
        let port = ports.get(&(nic_id, nic_kind)).ok_or_else(|| {
            Error::ExternalIpUpdateMissingPort(nic_id, nic_kind)
        })?;

        self.external_ips_ensure_port(
            port,
            nic_id,
            source_nat,
            ephemeral_ip,
            floating_ips,
        )
    }

    /// Ensure external IPs for an OPTE port are up to date.
    pub fn external_ips_ensure_port(
        &self,
        port: &Port,
        nic_id: Uuid,
        source_nat: Option<SourceNatConfig>,
        ephemeral_ip: Option<IpAddr>,
        floating_ips: &[IpAddr],
    ) -> Result<(), Error> {
        let egw_lock = self.inner.eip_gateways.lock().unwrap();
        let inet_gw_map = egw_lock.get(&nic_id).cloned();
        drop(egw_lock);

        // XXX: duplicates parts of macro logic in `create_port`.
        macro_rules! ext_ip_cfg {
            ($ip:expr, $log_prefix:literal, $ip_t:path, $cidr_t:path,
             $ipcfg_e:path, $ipcfg_t:ident, $snat_t:ident) => {{
                let snat = match source_nat {
                    Some(snat) => {
                        let $ip_t(snat_ip) = snat.ip else {
                            error!(
                                self.inner.log,
                                concat!($log_prefix, " SNAT config");
                                "snat_ip" => ?snat.ip,
                            );
                            return Err(Error::InvalidPortIpConfig);
                        };
                        let ports = snat.port_range();
                        Some($snat_t { external_ip: snat_ip.into(), ports })
                    }
                    None => None,
                };
                let ephemeral_ip = match ephemeral_ip {
                    Some($ip_t(ip)) => Some(ip.into()),
                    Some(_) => {
                        error!(
                            self.inner.log,
                            concat!($log_prefix, " ephemeral IP");
                            "ephemeral_ip" => ?ephemeral_ip,
                        );
                        return Err(Error::InvalidPortIpConfig);
                    }
                    None => None,
                };
                let floating_ips: Vec<_> = floating_ips
                    .iter()
                    .copied()
                    .map(|ip| match ip {
                        $ip_t(ip) => Ok(ip.into()),
                        _ => {
                            error!(
                                self.inner.log,
                                concat!($log_prefix, " ephemeral IP");
                                "ephemeral_ip" => ?ephemeral_ip,
                            );
                            Err(Error::InvalidPortIpConfig)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                ExternalIpCfg {
                    ephemeral_ip,
                    snat,
                    floating_ips,
                }
            }}
        }

        // TODO-completeness: support dual-stack. We'll need to explicitly store
        // a v4 and a v6 ephemeral IP + SNat + gateway + ... in `InstanceInner`
        // to have enough info to build both.
        let mut v4_cfg = None;
        let mut v6_cfg = None;
        match port.gateway().ip {
            IpAddr::V4(_) => {
                v4_cfg = Some(ext_ip_cfg!(
                    ip,
                    "Expected IPv4",
                    IpAddr::V4,
                    IpCidr::Ip4,
                    IpCfg::Ipv4,
                    Ipv4Cfg,
                    SNat4Cfg
                ))
            }
            IpAddr::V6(_) => {
                v6_cfg = Some(ext_ip_cfg!(
                    ip,
                    "Expected IPv6",
                    IpAddr::V6,
                    IpCidr::Ip6,
                    IpCfg::Ipv6,
                    Ipv6Cfg,
                    SNat6Cfg
                ))
            }
        }

        let inet_gw_map = if let Some(map) = inet_gw_map {
            Some(
                map.into_iter()
                    .map(|(k, v)| (k.into(), v.into_iter().collect()))
                    .collect(),
            )
        } else {
            None
        };

        let req = SetExternalIpsReq {
            port_name: port.name().into(),
            external_ips_v4: v4_cfg,
            external_ips_v6: v6_cfg,
            inet_gw_map,
        };
        let hdl = Handle::new()?;
        hdl.set_external_ips(&req)?;

        Ok(())
    }

    /// Validate multicast group memberships for an OPTE port.
    ///
    /// This method validates multicast group configurations but does not yet
    /// configure OPTE port-level multicast group membership. The actual
    /// multicast forwarding is currently handled by the reconciler + DPD
    /// at the dataplane switch level.
    ///
    /// TODO: Once OPTE kernel module supports multicast group APIs, this
    /// method should be updated to configure OPTE port-level multicast
    /// group membership. Note: multicast groups are fleet-wide and can span
    /// across VPCs.
    pub fn multicast_groups_ensure(
        &self,
        nic_id: Uuid,
        nic_kind: NetworkInterfaceKind,
        multicast_groups: &[MulticastGroupCfg],
    ) -> Result<(), Error> {
        let ports = self.inner.ports.lock().unwrap();
        let port = ports.get(&(nic_id, nic_kind)).ok_or_else(|| {
            Error::MulticastUpdateMissingPort(nic_id, nic_kind)
        })?;

        debug!(
            self.inner.log,
            "Validating multicast group configuration for OPTE port";
            "port_name" => port.name(),
            "nic_id" => ?nic_id,
            "groups" => ?multicast_groups,
        );

        // Validate multicast group configurations
        for group in multicast_groups {
            if !group.group_ip.is_multicast() {
                error!(
                    self.inner.log,
                    "Invalid multicast IP address";
                    "group_ip" => %group.group_ip,
                    "port_name" => port.name(),
                );
                return Err(Error::InvalidPortIpConfig);
            }
        }

        // TODO: Configure firewall rules to allow multicast traffic.
        // Add exceptions in source/dest MAC/L3 addr checking for multicast
        // addresses matching known groups, only doing cidr-checking on the
        // multicasst destination side.

        info!(
            self.inner.log,
            "OPTE port configured for multicast traffic";
            "port_name" => port.name(),
            "ipv4_range" => %IPV4_MULTICAST_RANGE,
            "ipv6_range" => %IPV6_MULTICAST_RANGE,
            "multicast_groups" => multicast_groups.len(),
        );

        // TODO: Configure OPTE port for specific multicast group membership
        // once OPTE kernel module APIs are available. This is distinct from
        // zone vNIC underlay configuration (see instance.rs
        // `join_multicast_group_inner`).

        Ok(())
    }

    pub fn firewall_rules_ensure(
        &self,
        vni: external::Vni,
        rules: &[ResolvedVpcFirewallRule],
    ) -> Result<(), Error> {
        info!(
            self.inner.log,
            "Ensuring VPC firewall rules";
            "vni" => ?vni,
            "rules" => ?&rules,
        );

        let hdl = Handle::new()?;
        let ports = self.inner.ports.lock().unwrap();

        // We update VPC rules as a set so grab only
        // the relevant ports using the VPC's VNI.
        let vpc_ports = ports
            .iter()
            .filter(|((_, _), port)| u32::from(vni) == u32::from(*port.vni()));
        for ((_, _), port) in vpc_ports {
            let rules = opte_firewall_rules(rules, port.vni(), port.mac());
            let port_name = port.name().to_string();
            info!(
                self.inner.log,
                "Setting OPTE firewall rules";
                "port" => ?&port_name,
                "rules" => ?&rules,
            );
            hdl.set_firewall_rules(&oxide_vpc::api::SetFwRulesReq {
                port_name,
                rules,
            })?;
        }
        Ok(())
    }

    pub fn list_virtual_nics(
        &self,
    ) -> Result<Vec<VirtualNetworkInterfaceHost>, Error> {
        let hdl = Handle::new()?;
        let v2p = hdl.dump_v2p()?;
        let mut mappings: Vec<_> = vec![];

        for mapping in v2p.mappings {
            let vni = mapping
                .vni
                .as_u32()
                .try_into()
                .expect("opte VNI should be 24 bits");

            for entry in mapping.ip4 {
                mappings.push(VirtualNetworkInterfaceHost {
                    virtual_ip: IpAddr::V4(entry.0.into()),
                    virtual_mac: MacAddr6::from(entry.1.ether.bytes()).into(),
                    physical_host_ip: entry.1.ip.into(),
                    vni,
                });
            }

            for entry in mapping.ip6 {
                mappings.push(VirtualNetworkInterfaceHost {
                    virtual_ip: IpAddr::V6(entry.0.into()),
                    virtual_mac: MacAddr6::from(entry.1.ether.bytes()).into(),
                    physical_host_ip: entry.1.ip.into(),
                    vni,
                });
            }
        }

        Ok(mappings)
    }

    pub fn set_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        info!(
            self.inner.log,
            "Mapping virtual NIC to physical host";
            "mapping" => ?&mapping,
        );
        let hdl = Handle::new()?;
        hdl.set_v2p(&oxide_vpc::api::SetVirt2PhysReq {
            vip: mapping.virtual_ip.into(),
            phys: oxide_vpc::api::PhysNet {
                ether: oxide_vpc::api::MacAddr::from(
                    (*mapping.virtual_mac).into_array(),
                ),
                ip: mapping.physical_host_ip.into(),
                vni: Vni::new(mapping.vni).unwrap(),
            },
        })?;

        Ok(())
    }

    pub fn unset_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        info!(
            self.inner.log,
            "Clearing mapping of virtual NIC to physical host";
            "mapping" => ?&mapping,
        );

        let hdl = Handle::new()?;
        hdl.clear_v2p(&oxide_vpc::api::ClearVirt2PhysReq {
            vip: mapping.virtual_ip.into(),
            phys: oxide_vpc::api::PhysNet {
                ether: oxide_vpc::api::MacAddr::from(
                    (*mapping.virtual_mac).into_array(),
                ),
                ip: mapping.physical_host_ip.into(),
                vni: Vni::new(mapping.vni).unwrap(),
            },
        })?;

        Ok(())
    }
}

pub struct PortTicket {
    id: Uuid,
    kind: NetworkInterfaceKind,
    manager: Arc<PortManagerInner>,
}

impl std::fmt::Debug for PortTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("PortTicket")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("manager", &"{ .. }")
            .finish()
    }
}

impl PortTicket {
    fn new(
        id: Uuid,
        kind: NetworkInterfaceKind,
        manager: Arc<PortManagerInner>,
    ) -> Self {
        Self { id, kind, manager }
    }

    fn release_inner(&mut self) -> Result<(), Error> {
        let mut ports = self.manager.ports.lock().unwrap();
        let Some(port) = ports.remove(&(self.id, self.kind)) else {
            error!(
                self.manager.log,
                "Tried to release non-existent port";
                "id" => ?&self.id,
                "kind" => ?&self.kind,
            );
            return Err(Error::ReleaseMissingPort(self.id, self.kind));
        };
        drop(ports);

        // Cleanup the set of subnets we want to receive routes for.
        let mut routes = self.manager.routes.lock().unwrap();
        for key in [port.system_router_key(), port.custom_router_key()] {
            let should_remove = routes
                .get_mut(&key)
                .map(|v| {
                    v.active_ports = v.active_ports.saturating_sub(1);
                    v.active_ports == 0
                })
                .unwrap_or_default();

            if should_remove {
                routes.remove(&key);
                info!(
                    self.manager.log,
                    "Removed route set for subnet";
                    "id" => ?&key,
                );
            }
        }

        debug!(
            self.manager.log,
            "Removed OPTE port from manager";
            "id" => ?&self.id,
            "kind" => ?&self.kind,
            "port" => ?&port,
        );
        Ok(())
    }

    pub fn release(mut self) {
        // There can only be a single `PortTicket` per-port
        // and we've taken it here by value, so the port must
        // still exist in the manager.
        self.release_inner()
            .expect("failed to release Port with valid PortTicket");

        // NOTE: We've already called `release_inner` so let's
        // skip the Drop impl which also calls `release_inner`.
        std::mem::forget(self);
    }
}

impl Drop for PortTicket {
    fn drop(&mut self) {
        // We're ignoring the value since (1) it's already logged and (2) we
        // can't do anything with it anyway.
        let _ = self.release_inner();
    }
}

#[cfg(test)]
mod tests {
    use crate::opte::Handle;

    use super::{PortCreateParams, PortManager};
    use macaddr::MacAddr6;
    use omicron_common::api::{
        external::{MacAddr, Vni},
        internal::shared::{
            InternetGatewayRouterTarget, NetworkInterface,
            NetworkInterfaceKind, ResolvedVpcRoute, ResolvedVpcRouteSet,
            RouterTarget, RouterVersion, SourceNatConfig,
        },
    };
    use omicron_test_utils::dev::test_setup_log;
    use oxide_vpc::api::DhcpCfg;
    use oxnet::{IpNet, Ipv4Net, Ipv6Net};
    use std::{
        collections::HashSet,
        net::{IpAddr, Ipv4Addr, Ipv6Addr},
    };
    use uuid::Uuid;

    // Regression for https://github.com/oxidecomputer/omicron/issues/7541.
    #[test]
    fn multiple_ports_does_not_destroy_default_route() {
        let logctx =
            test_setup_log("multiple_ports_does_not_destroy_default_route");
        let manager = PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST);
        let default_ipv4_route =
            IpNet::V4(Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).unwrap());
        let default_ipv6_route =
            IpNet::V6(Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 0).unwrap());

        // Information about our builtin services VPC System Router.
        //
        // This comes from nexus/db-fixed-data/src/vpc.rs. It _should_ stay in
        // sync with that for clarity, but the correctness of this test does not
        // rely on it.
        const SERVICES_INTERNET_GATEWAY_ID: Uuid =
            uuid::uuid!("001de000-074c-4000-8000-000000000002");
        const SERVICES_VPC_VNI: Vni = Vni::SERVICES_VNI;

        let handle = Handle::new().unwrap();
        handle.set_xde_underlay("foo0", "foo1").unwrap();

        // First, create a port for a service.
        //
        // At this point, we'll insert a single default route, because this is a
        // service point, from `0.0.0.0/0 -> InternetGateway(None)`, and then
        // add this route to OPTE.
        let private_ipv4_addr0 = IpAddr::V4(Ipv4Addr::new(172, 20, 0, 4));
        let private_ipv4_addr1 = IpAddr::V4(Ipv4Addr::new(172, 20, 0, 5));
        let public_ipv4_addr0 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4));
        let public_ipv4_addr1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5));
        let private_subnet =
            IpNet::V4(Ipv4Net::new(Ipv4Addr::new(172, 20, 0, 0), 24).unwrap());
        const MAX_PORT: u16 = (1 << 14) - 1;
        let (port0, _ticket0) = manager
            .create_port(PortCreateParams {
                nic: &NetworkInterface {
                    id: Uuid::new_v4(),
                    kind: NetworkInterfaceKind::Service { id: Uuid::new_v4() },
                    name: "opte0".parse().unwrap(),
                    ip: private_ipv4_addr0,
                    mac: MacAddr(MacAddr6::new(
                        0xa8, 0x40, 0x25, 0x00, 0x00, 0x01,
                    )),
                    subnet: private_subnet,
                    vni: SERVICES_VPC_VNI,
                    primary: true,
                    slot: 0,
                    transit_ips: Vec::new(),
                },
                source_nat: Some(
                    SourceNatConfig::new(public_ipv4_addr0, 0, MAX_PORT)
                        .unwrap(),
                ),
                ephemeral_ip: None,
                floating_ips: &[],
                firewall_rules: &[],
                dhcp_config: DhcpCfg {
                    hostname: None,
                    host_domain: None,
                    domain_search_list: Vec::new(),
                    dns4_servers: Vec::new(),
                    dns6_servers: Vec::new(),
                },
            })
            .unwrap();

        // At this point, we should have inserted a single default route. That
        // is because the port is for an Oxide service, and so we automatically
        // add a default route to the IGW. This doesn't have an ID for the IGW,
        // since we haven't launched Nexus or the database -- until that time,
        // we don't know that IGW's ID.
        let system_routes = manager
            .inner
            .routes
            .lock()
            .unwrap()
            .get(&port0.system_router_key())
            .unwrap()
            .clone();

        // We actually have two route-sets, one for the system and one for the
        // custom router. We're only interested in the former though.
        assert_eq!(
            system_routes.routes.len(),
            2,
            "We should have two default routes in the VPC's System Router"
        );
        for route in system_routes.routes.iter() {
            assert!(
                route.dest == default_ipv4_route
                    || route.dest == default_ipv6_route,
                "VPC System Router should have a default route"
            );
            assert_eq!(
                route.target,
                RouterTarget::InternetGateway(
                    InternetGatewayRouterTarget::System
                ),
                "VPC System Router default route should target the \
                System Internet Gateway"
            );
        }

        // In OPTE, we should have one route, also squished down to this
        // default route.
        //
        // NOTE: When we're doing these assertions, we hold a lock on the OPTE
        // port state, so we need to do it in a scope before we do other
        // operations.
        {
            let state = handle.state().lock().unwrap();
            assert_eq!(state.ports.len(), 1);
            let rt = state
                .ports
                .get("opte0")
                .unwrap()
                .routes
                .iter()
                .filter(|rt| rt.is_system_default_ipv4_route())
                .collect::<Vec<_>>();
            assert_eq!(
                rt.len(),
                1,
                "OPTE should have exactly one default system route for \
                the first port on creation"
            );
        }

        // PUT some routes.
        //
        // Simulate a PUT /vpc-routes from Nexus. Now that Nexus has launched
        // and loaded builtin data to the database, it knows the ID of our the
        // IGW of the System Router in the builtin services VPC. This ID is
        // included in the list of routes. Because that set does _not_ contain
        // the implicit route added above, that one is _removed_ from the one
        // existing port. An equivalent one is added though, since the IGW is
        // ignored at this point when setting the route in OPTE.
        let mut new_routes = vec![ResolvedVpcRouteSet {
            id: port0.system_router_key(),
            version: Some(RouterVersion {
                router_id: SERVICES_INTERNET_GATEWAY_ID,
                version: 1,
            }),
            routes: HashSet::from([ResolvedVpcRoute {
                dest: default_ipv4_route,
                target: RouterTarget::InternetGateway(
                    InternetGatewayRouterTarget::System,
                ),
            }]),
        }];
        manager.vpc_routes_ensure(new_routes.clone()).unwrap();

        // At this point, the in-memory state of the manager should have one
        // route, for the _explicit_ IGW of the services VPC; and our OPTE state
        // should have just one for the IGW with _no_ ID, because we always
        // throw away the UUID when we apply the rule there.
        let system_routes = manager
            .inner
            .routes
            .lock()
            .unwrap()
            .get(&port0.system_router_key())
            .unwrap()
            .clone();
        assert_eq!(
            system_routes.routes.len(),
            1,
            "We should have only a single route in the VPC's System Router"
        );
        let route = system_routes.routes.iter().next().unwrap();
        assert_eq!(
            route.dest, default_ipv4_route,
            "VPC System Router should have a default route"
        );
        assert_eq!(
            route.target,
            RouterTarget::InternetGateway(InternetGatewayRouterTarget::System),
            "VPC System Router default route should target the explicit \
            services Internet Gateway after vpc_routes_ensure"
        );

        {
            let state = handle.state().lock().unwrap();
            assert_eq!(state.ports.len(), 1);
            let rt = state
                .ports
                .get("opte0")
                .unwrap()
                .routes
                .iter()
                .filter(|rt| rt.is_system_default_ipv4_route())
                .collect::<Vec<_>>();
            assert_eq!(
                rt.len(),
                1,
                "OPTE should have exactly one default system route for \
                the first port on creation"
            );
        }

        // Create a new port.
        //
        // Now, when we create this new port, we'll again implicitly create that
        // default route. Since we _also_ have the route in the previous step
        // pointing to an explicit IGW, we'll call add_router_entry twice for
        // this point on creation, but not the other port since we don't modify
        // it when we create this second port. That happens when we call
        // `vpc_routes_ensure` below.
        let (port1, _ticket1) = manager
            .create_port(PortCreateParams {
                nic: &NetworkInterface {
                    id: Uuid::new_v4(),
                    kind: NetworkInterfaceKind::Service { id: Uuid::new_v4() },
                    name: "opte1".parse().unwrap(),
                    ip: private_ipv4_addr1,
                    mac: MacAddr(MacAddr6::new(
                        0xa8, 0x40, 0x25, 0x00, 0x00, 0x02,
                    )),
                    subnet: private_subnet,
                    vni: SERVICES_VPC_VNI,
                    primary: true,
                    slot: 0,
                    transit_ips: Vec::new(),
                },
                source_nat: Some(
                    SourceNatConfig::new(public_ipv4_addr1, 0, MAX_PORT)
                        .unwrap(),
                ),
                ephemeral_ip: None,
                floating_ips: &[],
                firewall_rules: &[],
                dhcp_config: DhcpCfg {
                    hostname: None,
                    host_domain: None,
                    domain_search_list: Vec::new(),
                    dns4_servers: Vec::new(),
                    dns6_servers: Vec::new(),
                },
            })
            .unwrap();

        // When creating the system port, we automatically added a default route
        // pointing to IGW(None). In the previous behavior, this was considered
        // different from IGW(ID) -- that's incorrect, because we throw away the
        // ID when we set route in OPTE itself.
        //
        // We should have exactly one default route here and at OPTE, pointing
        // to the services VPC System Router's IGW, without an explicit ID.
        let system_routes = manager
            .inner
            .routes
            .lock()
            .unwrap()
            .get(&port1.system_router_key())
            .unwrap()
            .clone();
        assert_eq!(
            system_routes.routes.len(),
            1,
            "We should always have 1 default route, pointing to the services \
            VPC System Router's IGW, even after adding a new port",
        );
        let _ = system_routes
            .routes
            .iter()
            .find(|rt| {
                rt.dest == default_ipv4_route
                    && rt.target
                        == RouterTarget::InternetGateway(
                            InternetGatewayRouterTarget::System,
                        )
            })
            .expect(
                "Should have default route targeting the explicit services IGW",
            );

        {
            let state = handle.state().lock().unwrap();
            assert_eq!(state.ports.len(), 2);
            for p in 0..2 {
                let port_name = format!("opte{p}");
                let rt = state
                    .ports
                    .get(&port_name)
                    .unwrap()
                    .routes
                    .iter()
                    .filter(|rt| rt.is_system_default_ipv4_route())
                    .collect::<Vec<_>>();
                assert_eq!(
                    rt.len(),
                    1,
                    "{port_name} should have exactly one default system route \
                    pointing to the IGW(None), after creating the second port",
                );
            }
        }

        // Now, PUT /vpc-routes again, but with a higher version so that things
        // are replaced internally.
        new_routes[0].version.as_mut().expect("Set above").version = 2;
        manager.vpc_routes_ensure(new_routes).unwrap();

        // Previously, this is where things blew up. Nexus told us to add a new
        // route to the explicit IGW, which we already have. That means our set
        // of routes to add is empty. But our set of routes to delete still has
        // the _implicit_ route we added when we created the second port, the
        // one pointing to IGW(None).
        //
        // Since Nexus's request didn't include that, we deleted it from both
        // ports, which destroyed the (only) default route on the first port.
        // The second port still had a route because we made a distinction
        // between the implicit and explicit routes.
        let system_routes = manager
            .inner
            .routes
            .lock()
            .unwrap()
            .get(&port0.system_router_key())
            .unwrap()
            .clone();
        assert_eq!(
            system_routes.routes.len(),
            1,
            "We should now have only 1 default route, since Nexus sent \
            us a request with an explicit IGW. We should have deleted \
            the one pointing to IGW(None)."
        );
        let _ = system_routes
            .routes
            .iter()
            .find(|rt| {
                rt.dest == default_ipv4_route
                    && rt.target
                        == RouterTarget::InternetGateway(
                            InternetGatewayRouterTarget::System,
                        )
            })
            .expect(
                "Should have default route targeting the explicit services IGW",
            );

        // As before, we should still have a default route pointing to IGW(None)
        // for both OPTE ports. We shouldn't delete the default route on the
        // first.
        {
            let state = handle.state().lock().unwrap();
            assert_eq!(state.ports.len(), 2);
            for p in 0..2 {
                let port_name = format!("opte{p}");
                let rt = state
                    .ports
                    .get(&port_name)
                    .unwrap()
                    .routes
                    .iter()
                    .filter(|rt| rt.is_system_default_ipv4_route())
                    .collect::<Vec<_>>();
                assert_eq!(
                    rt.len(),
                    1,
                    "{port_name} should have exactly one default system route \
                    target the services IGW, after creating the second port",
                );
            }
        }

        logctx.cleanup_successful();
    }
}
