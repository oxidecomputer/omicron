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
use ipnetwork::Ipv4Network;
use ipnetwork::Ipv6Network;
use macaddr::MacAddr6;
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
use omicron_common::api::internal::shared::RouterKind;
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
use oxide_vpc::api::Ipv4Cfg;
use oxide_vpc::api::Ipv4Cidr;
use oxide_vpc::api::Ipv6Cfg;
use oxide_vpc::api::Ipv6Cidr;
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
    // TODO-completeness: These should ideally be grouped together into a type
    // that ensures they're all of the same IP version, and that the IP stack
    // for the external and VPC-private addresses match.
    //
    // See https://github.com/oxidecomputer/omicron/issues/9318.
    pub source_nat: Option<SourceNatConfig>,
    pub ephemeral_ip: Option<IpAddr>,
    pub floating_ips: &'a [IpAddr],
    pub firewall_rules: &'a [ResolvedVpcFirewallRule],
    pub dhcp_config: DhcpCfg,
}

impl<'a> TryFrom<&PortCreateParams<'a>> for IpCfg {
    type Error = Error;

    fn try_from(params: &PortCreateParams) -> Result<IpCfg, Error> {
        let ip_config = &params.nic.ip_config;
        let has_ipv4_stack = ip_config.ipv4_addr().is_some();
        let has_ipv6_stack = ip_config.ipv6_addr().is_some();

        // First, attempt to convert the IPv4 configuration.
        //
        // The important part here is ensuring that we have an external and
        // VPC-private configuration for each IP stack. That is, if we have an
        // IPv4 external Ephemeral IP, we also have an IPv4 VPC-private address.
        let v4 = match ip_config.ipv4_config() {
            None => None,
            Some(ipv4_config) => {
                let gateway_ip = ipv4_config.subnet().first_addr().into();
                let vpc_subnet =
                    Ipv4Cidr::from(Ipv4Network::from(*ipv4_config.subnet()));
                let private_ip = (*ipv4_config.ip()).into();
                let external_ips = build_external_ipv4_config(
                    params.source_nat.as_ref(),
                    params.ephemeral_ip.as_ref(),
                    &params.floating_ips,
                    has_ipv4_stack,
                )?;
                Some(Ipv4Cfg {
                    vpc_subnet,
                    private_ip,
                    gateway_ip,
                    external_ips,
                })
            }
        };

        // Now the same conversion, but for IPv6. Again, we need to ensure that,
        // if we have an external IPv6 address, we also have a VPC-private IPv6
        // address to translate that into.
        let v6 = match ip_config.ipv6_config() {
            None => None,
            Some(ipv6_config) => {
                let gateway_ip = ipv6_config.subnet().first_addr().into();
                let vpc_subnet =
                    Ipv6Cidr::from(Ipv6Network::from(*ipv6_config.subnet()));
                let private_ip = (*ipv6_config.ip()).into();
                let external_ips = build_external_ipv6_config(
                    params.source_nat.as_ref(),
                    params.ephemeral_ip.as_ref(),
                    &params.floating_ips,
                    has_ipv6_stack,
                )?;
                Some(Ipv6Cfg {
                    vpc_subnet,
                    private_ip,
                    gateway_ip,
                    external_ips,
                })
            }
        };

        // Now build the full configuration, either single- or dual-stack.
        let cfg = match (v4, v6) {
            (Some(ipv4), Some(ipv6)) => IpCfg::DualStack { ipv4, ipv6 },
            (Some(v4), None) => IpCfg::Ipv4(v4),
            (None, Some(v6)) => IpCfg::Ipv6(v6),
            (None, None) => unreachable!(),
        };
        Ok(cfg)
    }
}

// Build an ExternalIpCfg from parameters.
fn build_external_ipv4_config(
    source_nat: Option<&SourceNatConfig>,
    ephemeral_ip: Option<&IpAddr>,
    floating_ips: &[IpAddr],
    has_ipv4_stack: bool,
) -> Result<ExternalIpCfg<oxide_vpc::api::Ipv4Addr>, Error> {
    // Convert the SNAT configuration.
    //
    // It's not an error to have an SNAT address of a different IP
    // version right now, _as long as_ we have a VPC-private address
    // that _does_ have the right version.
    let snat = match source_nat {
        None => None,
        Some(snat) => match snat.ip {
            IpAddr::V4(ipv4) if has_ipv4_stack => Some(SNat4Cfg {
                external_ip: ipv4.into(),
                ports: snat.port_range(),
            }),
            IpAddr::V4(_) => {
                return Err(Error::InvalidPortIpConfig);
            }
            IpAddr::V6(_) => None,
        },
    };

    // Convert the Ephemeral address, again ensuring that we have a
    // VPC-private IP stack to support the external address.
    let ephemeral_ip = match ephemeral_ip {
        Some(IpAddr::V4(ipv4)) if has_ipv4_stack => Some((*ipv4).into()),
        Some(IpAddr::V4(_)) => return Err(Error::InvalidPortIpConfig),
        None | Some(IpAddr::V6(_)) => None,
    };

    // And all the Floating IPs, still ensuring we can support them.
    let floating_ips = floating_ips
        .iter()
        .filter_map(|ip| match ip {
            IpAddr::V4(ipv4) if has_ipv4_stack => Some(Ok((*ipv4).into())),
            IpAddr::V4(_) => Some(Err(Error::InvalidPortIpConfig)),
            IpAddr::V6(_) => None,
        })
        .collect::<Result<_, _>>()?;
    Ok(ExternalIpCfg { snat, ephemeral_ip, floating_ips })
}

// Build an OPTE External IPv6 configuration from parameters.
fn build_external_ipv6_config(
    source_nat: Option<&SourceNatConfig>,
    ephemeral_ip: Option<&IpAddr>,
    floating_ips: &[IpAddr],
    has_ipv6_stack: bool,
) -> Result<ExternalIpCfg<oxide_vpc::api::Ipv6Addr>, Error> {
    // Convert the SNAT configuration.
    let snat = match source_nat {
        None => None,
        Some(snat) => match snat.ip {
            IpAddr::V6(ipv6) if has_ipv6_stack => Some(SNat6Cfg {
                external_ip: ipv6.into(),
                ports: snat.port_range(),
            }),
            IpAddr::V6(_) => {
                return Err(Error::InvalidPortIpConfig);
            }
            IpAddr::V4(_) => None,
        },
    };

    // Convert the Ephemeral address.
    let ephemeral_ip = match ephemeral_ip {
        Some(IpAddr::V6(ipv6)) if has_ipv6_stack => Some((*ipv6).into()),
        Some(IpAddr::V6(_)) => return Err(Error::InvalidPortIpConfig),
        None | Some(IpAddr::V4(_)) => None,
    };

    // And all the Floating IPs, still ensuring we can support them.
    let floating_ips = floating_ips
        .iter()
        .filter_map(|ip| match ip {
            IpAddr::V6(ipv6) if has_ipv6_stack => Some(Ok((*ipv6).into())),
            IpAddr::V6(_) => Some(Err(Error::InvalidPortIpConfig)),
            IpAddr::V4(_) => None,
        })
        .collect::<Result<_, _>>()?;
    Ok(ExternalIpCfg { snat, ephemeral_ip, floating_ips })
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
        let ip_cfg = IpCfg::try_from(&params)?;
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
        let gateway = Gateway::from_ip_config(&nic.ip_config);
        let vpc_cfg = VpcCfg {
            ip_cfg,
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
                ip: nic.ip_config.clone(),
                mac,
                slot: nic.slot,
                vni,
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

        // Create the default set of routes for a new port.
        //
        // This creates an empty route set for instance ports, but adds a rule
        // targeting the System VPC Internet Gateway for service ports.
        //
        // We need this during bootstrapping NTP or other very early services,
        // before the control plane database has started.
        let default_route_set = |is_service: bool| {
            let mut route_set = RouteSet {
                version: None,
                routes: HashSet::new(),
                active_ports: 0,
            };
            if !is_service {
                return route_set;
            }
            let target = ApiRouterTarget::InternetGateway(
                InternetGatewayRouterTarget::System,
            );
            route_set.routes.insert(ResolvedVpcRoute {
                dest: IpNet::V4(
                    Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).unwrap(),
                ),
                target,
            });
            route_set.routes.insert(ResolvedVpcRoute {
                dest: IpNet::V6(
                    Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 0).unwrap(),
                ),
                target,
            });
            route_set
        };

        // Add routes to a new port and the shared set of all routes in this
        // port manager.
        //
        // As new ports are created, they need both System router entries, but
        // also Custom entries targeting things like the VPC Subnet or another
        // instance. These are added to the port, but after updating the shared
        // set of all routes maintained by this manager.
        let add_routes = |route_map: &mut HashMap<RouterId, RouteSet>,
                          key: RouterId|
         -> Result<(), Error> {
            let route_set = route_map
                .entry(key)
                .or_insert_with(|| default_route_set(is_service));
            route_set.active_ports += 1;
            let class = match key.kind {
                RouterKind::System => RouterClass::System,
                RouterKind::Custom(_) => RouterClass::Custom,
            };
            for route in &route_set.routes {
                let request = AddRouterEntryReq {
                    class,
                    port_name: port_name.clone(),
                    dest: super::net_to_cidr(route.dest),
                    target: super::router_target_opte(&route.target),
                };
                hdl.add_router_entry(&request)?;
                debug!(
                    self.inner.log,
                    "Added router entry";
                    "port_name" => &port_name,
                    "route" => ?request,
                );
            }
            Ok(())
        };

        // Actually add all system and custom router entries relevant for this
        // new port.
        let mut route_map = self.inner.routes.lock().unwrap();
        add_routes(&mut route_map, port.system_router_key())?;
        if let Some(key) = port.custom_ipv4_router_key() {
            add_routes(&mut route_map, key)?;
        }
        if let Some(key) = port.custom_ipv6_router_key() {
            add_routes(&mut route_map, key)?;
        }
        drop(route_map);

        // If there are any transit IPs set, allow them through.
        // TODO: Currently set only in initial state.
        //       This, external IPs, and cfg'able state
        //       (DHCP?) are probably worth being managed by an RPW.
        for block in nic.ip_config.all_transit_ips() {
            // In principle if this were an operation on an existing
            // port, we would explicitly undo the In addition if the
            // Out addition fails.
            // However, failure here will just destroy the port
            // outright -- this should only happen if an excessive
            // number of rules are specified.
            hdl.allow_cidr(
                &port_name,
                super::net_to_cidr(block),
                Direction::In,
            )?;
            hdl.allow_cidr(
                &port_name,
                super::net_to_cidr(block),
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
                        new.routes.difference(&old.routes).copied().collect(),
                        old.routes.difference(&new.routes).copied().collect(),
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
            // Fetch deltas for all router keys: system, IPv4 subnet, and IPv6
            // subnet.
            let system_delta = deltas.get(&port.system_router_key());
            let custom_ipv4_delta =
                port.custom_ipv4_router_key().and_then(|k| deltas.get(&k));
            let custom_ipv6_delta =
                port.custom_ipv6_router_key().and_then(|k| deltas.get(&k));

            for (class, delta) in [
                (RouterClass::System, system_delta),
                (RouterClass::Custom, custom_ipv4_delta),
                (RouterClass::Custom, custom_ipv6_delta),
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
    /// by attached `NetworkInterface`s.
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

        let has_ipv4_stack = port.ipv4_addr().is_some();
        let has_ipv6_stack = port.ipv6_addr().is_some();
        let external_ips_v4 = build_external_ipv4_config(
            source_nat.as_ref(),
            ephemeral_ip.as_ref(),
            floating_ips,
            has_ipv4_stack,
        )?;
        let external_ips_v6 = build_external_ipv6_config(
            source_nat.as_ref(),
            ephemeral_ip.as_ref(),
            floating_ips,
            has_ipv6_stack,
        )?;

        // The above functions building the external address configuration only
        // fail if we're provided external addresses from an IP version we don't
        // have a VPC-private IP stack for. E.g., IPv6 external IPs for an
        // IPv4-only interface. If we're provided IPv6 external IPs and we have
        // both an IPv4 and IPv6 interface, then those methods succeed, but
        // return an `ExternalIpCfg` where all the fields are empty.
        //
        // However, the `SetExternalIpsReq` method accepts an _option_ around
        // those values. Those should be None if there are zero addresses of the
        // corresponding version in the parameters. In that case, all the fields
        // of the `ExternalIpCfg`s are None or empty. This function does that
        // conversion for us.
        fn convert_empty_ip_cfg<T>(
            cfg: ExternalIpCfg<T>,
        ) -> Option<ExternalIpCfg<T>> {
            if cfg.snat.is_none()
                && cfg.ephemeral_ip.is_none()
                && cfg.floating_ips.is_empty()
            {
                None
            } else {
                Some(cfg)
            }
        }
        let external_ips_v4 = convert_empty_ip_cfg(external_ips_v4);
        let external_ips_v6 = convert_empty_ip_cfg(external_ips_v6);

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
            external_ips_v4,
            external_ips_v6,
            inet_gw_map,
        };
        let hdl = Handle::new()?;
        hdl.set_external_ips(&req)?;

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
        let remove_key = |routes: &mut HashMap<RouterId, RouteSet>,
                          key: RouterId| {
            let should_remove = routes
                .get_mut(&key)
                .map(|v| {
                    v.active_ports = v.active_ports.saturating_sub(1);
                    v.active_ports == 0
                })
                .unwrap_or(false);

            if should_remove {
                routes.remove(&key);
                info!(
                    self.manager.log,
                    "Removed route set for subnet";
                    "id" => ?&key,
                );
            }
        };
        let mut routes = self.manager.routes.lock().unwrap();
        remove_key(&mut routes, port.system_router_key());
        if let Some(key) = port.custom_ipv4_router_key() {
            remove_key(&mut routes, key);
        }
        if let Some(key) = port.custom_ipv6_router_key() {
            remove_key(&mut routes, key);
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
            NetworkInterfaceKind, PrivateIpConfig, ResolvedVpcRoute,
            ResolvedVpcRouteSet, RouterTarget, RouterVersion, SourceNatConfig,
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
        let private_subnet =
            Ipv4Net::new(Ipv4Addr::new(172, 20, 0, 0), 24).unwrap();
        let private_ipv4_addr0 = Ipv4Addr::new(172, 20, 0, 4);
        let ip_config0 =
            PrivateIpConfig::new_ipv4(private_ipv4_addr0, private_subnet)
                .unwrap();
        let private_ipv4_addr1 = Ipv4Addr::new(172, 20, 0, 5);
        let ip_config1 =
            PrivateIpConfig::new_ipv4(private_ipv4_addr1, private_subnet)
                .unwrap();
        let public_ipv4_addr0 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4));
        let public_ipv4_addr1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5));
        const MAX_PORT: u16 = (1 << 14) - 1;
        let (port0, _ticket0) = manager
            .create_port(PortCreateParams {
                nic: &NetworkInterface {
                    id: Uuid::new_v4(),
                    kind: NetworkInterfaceKind::Service { id: Uuid::new_v4() },
                    name: "opte0".parse().unwrap(),
                    ip_config: ip_config0,
                    mac: MacAddr(MacAddr6::new(
                        0xa8, 0x40, 0x25, 0x00, 0x00, 0x01,
                    )),
                    vni: SERVICES_VPC_VNI,
                    primary: true,
                    slot: 0,
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
                    ip_config: ip_config1,
                    mac: MacAddr(MacAddr6::new(
                        0xa8, 0x40, 0x25, 0x00, 0x00, 0x02,
                    )),
                    vni: SERVICES_VPC_VNI,
                    primary: true,
                    slot: 0,
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
