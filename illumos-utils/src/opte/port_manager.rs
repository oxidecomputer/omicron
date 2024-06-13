// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manager for all OPTE ports on a Helios system

use crate::opte::opte_firewall_rules;
use crate::opte::params::VirtualNetworkInterfaceHost;
use crate::opte::params::VpcFirewallRule;
use crate::opte::port::PortData;
use crate::opte::Error;
use crate::opte::Gateway;
use crate::opte::Port;
use crate::opte::Vni;
use ipnetwork::IpNetwork;
use omicron_common::api::external;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_common::api::internal::shared::ResolvedVpcRoute;
use omicron_common::api::internal::shared::ResolvedVpcRouteSet;
use omicron_common::api::internal::shared::ResolvedVpcRouteState;
use omicron_common::api::internal::shared::RouterId;
use omicron_common::api::internal::shared::RouterTarget as ApiRouterTarget;
use omicron_common::api::internal::shared::RouterVersion;
use omicron_common::api::internal::shared::SourceNatConfig;
use oxide_vpc::api::AddRouterEntryReq;
use oxide_vpc::api::DelRouterEntryReq;
use oxide_vpc::api::DhcpCfg;
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
use slog::debug;
use slog::error;
use slog::info;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

// Prefix used to identify xde data links.
const XDE_LINK_PREFIX: &str = "opte";

/// Stored routes (and usage count) for a given VPC/subnet.
#[derive(Debug, Clone)]
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
}

impl PortManagerInner {
    fn next_port_name(&self) -> String {
        format!(
            "{}{}",
            XDE_LINK_PREFIX,
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
    pub firewall_rules: &'a [VpcFirewallRule],
    pub dhcp_config: DhcpCfg,
    pub is_service: bool,
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
        });

        Self { inner }
    }

    pub fn underlay_ip(&self) -> &Ipv6Addr {
        &self.inner.underlay_ip
    }

    /// Create an OPTE port
    #[cfg_attr(not(target_os = "illumos"), allow(unused_variables))]
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
            is_service,
        } = params;

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
        // - create the vnic, cleaning up the xde device if that fails
        // - add both to the Port
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
        #[cfg(target_os = "illumos")]
        let hdl = {
            let hdl = opte_ioctl::OpteHdl::open(opte_ioctl::OpteHdl::XDE_CTL)?;
            hdl.create_xde(
                &port_name,
                vpc_cfg,
                dhcp_config,
                /* passthru = */ false,
            )?;
            hdl
        };

        // Initialize firewall rules for the new port.
        let rules = opte_firewall_rules(firewall_rules, &vni, &mac);
        debug!(
            self.inner.log,
            "Setting firewall rules";
            "port_name" => &port_name,
            "rules" => ?&rules,
        );
        #[cfg(target_os = "illumos")]
        hdl.set_fw_rules(&oxide_vpc::api::SetFwRulesReq {
            port_name: port_name.clone(),
            rules,
        })?;

        // TODO-remove(#2932): Create a VNIC on top of this device, to hook Viona into.
        //
        // Viona is the illumos MAC provider that implements the VIRTIO
        // specification. It sits on top of a MAC provider, which is responsible
        // for delivering frames to the underlying data link. The guest includes
        // a driver that handles the virtio-net specification on their side,
        // which talks to Viona.
        //
        // In theory, Viona works with any MAC provider. However, there are
        // implicit assumptions, in both Viona _and_ MAC, that require Viona to
        // be built on top of a VNIC specifically. There is probably a good deal
        // of work required to relax that assumption, so in the meantime, we
        // create a superfluous VNIC on the OPTE device, solely so Viona can use
        // it.
        let vnic = {
            let vnic_name = format!("v{}", port_name);
            #[cfg(target_os = "illumos")]
            if let Err(e) = crate::dladm::Dladm::create_vnic(
                &crate::dladm::PhysicalLink(port_name.clone()),
                &vnic_name,
                Some(nic.mac),
                None,
                1500,
            ) {
                slog::warn!(
                    self.inner.log,
                    "Failed to create overlay VNIC for xde device";
                    "port_name" => &port_name,
                    "err" => ?e
                );
                if let Err(e) = hdl.delete_xde(&port_name) {
                    slog::warn!(
                        self.inner.log,
                        "Failed to clean up xde device after failure to create overlay VNIC";
                        "err" => ?e
                    );
                }
                return Err(e.into());
            }
            debug!(
                self.inner.log,
                "Created overlay VNIC for xde device";
                "port_name" => &port_name,
                "vnic_name" => &vnic_name,
            );

            // NOTE: We intentionally use a string rather than the Vnic type
            // here. See the notes on the `opte::PortInner::vnic` field.
            vnic_name
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
                vnic,
            });
            let old = ports.insert((nic.id, nic.kind), port.clone());
            assert!(
                old.is_none(),
                "Duplicate OPTE port detected: interface_id = {}, kind = {:?}",
                nic.id,
                nic.kind,
            );
            (port, ticket)
        };

        // Check locally to see whether we have any routes from the
        // control plane for this port already installed. If not,
        // create a record to show that we're interested in receiving
        // those routes.
        let mut routes = self.inner.routes.lock().unwrap();
        let system_routes =
            routes.entry(port.system_router_key()).or_insert_with(|| {
                let mut routes = HashSet::new();

                // Services do not talk to one another via OPTE, but do need
                // to reach out over the Internet *before* nexus is up to give
                // us real rules. The easiest bet is to instantiate these here.
                if is_service {
                    routes.insert(ResolvedVpcRoute {
                        dest: "0.0.0.0/0".parse().unwrap(),
                        target: ApiRouterTarget::InternetGateway,
                    });
                    routes.insert(ResolvedVpcRoute {
                        dest: "::/0".parse().unwrap(),
                        target: ApiRouterTarget::InternetGateway,
                    });
                }

                RouteSet { version: None, routes, active_ports: 0 }
            });
        system_routes.active_ports += 1;
        // Clone is needed to get borrowck on our side, sadly.
        let system_routes = system_routes.clone();

        let custom_routes = routes
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

                #[cfg(target_os = "illumos")]
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
        if let Some(blocks) = &nic.transit_ips {
            for block in blocks {
                #[cfg(target_os = "illumos")]
                {
                    use oxide_vpc::api::Direction;

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
                }

                debug!(
                    self.inner.log,
                    "Added CIDR to in/out allowlist";
                    "port_name" => &port_name,
                    "cidr" => ?block,
                );
            }
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
        for new in new_routes {
            // Disregard any route information for a subnet we don't have.
            let Some(old) = routes.get(&new.id) else {
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
        #[cfg(target_os = "illumos")]
        let hdl = opte_ioctl::OpteHdl::open(opte_ioctl::OpteHdl::XDE_CTL)?;

        // Propagate deltas out to all ports.
        for port in ports.values() {
            let system_id = port.system_router_key();
            let system_delta = deltas.get(&system_id);

            let custom_id = port.custom_router_key();
            let custom_delta = deltas.get(&custom_id);

            #[cfg_attr(not(target_os = "illumos"), allow(unused_variables))]
            for (class, delta) in [
                (RouterClass::System, system_delta),
                (RouterClass::Custom, custom_delta),
            ] {
                let Some((to_add, to_delete)) = delta else {
                    continue;
                };

                for route in to_delete {
                    let route = DelRouterEntryReq {
                        class,
                        port_name: port.name().into(),
                        dest: super::net_to_cidr(route.dest),
                        target: super::router_target_opte(&route.target),
                    };

                    #[cfg(target_os = "illumos")]
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

                    #[cfg(target_os = "illumos")]
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

    /// Ensure external IPs for an OPTE port are up to date.
    #[cfg_attr(not(target_os = "illumos"), allow(unused_variables))]
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

        let req = SetExternalIpsReq {
            port_name: port.name().into(),
            external_ips_v4: v4_cfg,
            external_ips_v6: v6_cfg,
        };

        #[cfg(target_os = "illumos")]
        let hdl = opte_ioctl::OpteHdl::open(opte_ioctl::OpteHdl::XDE_CTL)?;

        #[cfg(target_os = "illumos")]
        hdl.set_external_ips(&req)?;

        Ok(())
    }

    #[cfg(target_os = "illumos")]
    pub fn firewall_rules_ensure(
        &self,
        vni: external::Vni,
        rules: &[VpcFirewallRule],
    ) -> Result<(), Error> {
        use opte_ioctl::OpteHdl;

        info!(
            self.inner.log,
            "Ensuring VPC firewall rules";
            "vni" => ?vni,
            "rules" => ?&rules,
        );

        let hdl = OpteHdl::open(OpteHdl::XDE_CTL)?;

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
            hdl.set_fw_rules(&oxide_vpc::api::SetFwRulesReq {
                port_name,
                rules,
            })?;
        }
        Ok(())
    }

    #[cfg(not(target_os = "illumos"))]
    pub fn firewall_rules_ensure(
        &self,
        vni: external::Vni,
        rules: &[VpcFirewallRule],
    ) -> Result<(), Error> {
        info!(
            self.inner.log,
            "Ensuring VPC firewall rules (ignored)";
            "vni" => ?vni,
            "rules" => ?&rules,
        );
        Ok(())
    }

    #[cfg(target_os = "illumos")]
    pub fn list_virtual_nics(
        &self,
    ) -> Result<Vec<VirtualNetworkInterfaceHost>, Error> {
        use macaddr::MacAddr6;
        use opte_ioctl::OpteHdl;

        let hdl = OpteHdl::open(OpteHdl::XDE_CTL)?;
        let v2p =
            hdl.dump_v2p(&oxide_vpc::api::DumpVirt2PhysReq { unused: 99 })?;
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

    #[cfg(not(target_os = "illumos"))]
    pub fn list_virtual_nics(
        &self,
    ) -> Result<Vec<VirtualNetworkInterfaceHost>, Error> {
        info!(
            self.inner.log,
            "Listing virtual nics (ignored)";
        );
        Ok(vec![])
    }

    #[cfg(target_os = "illumos")]
    pub fn set_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        use opte_ioctl::OpteHdl;

        info!(
            self.inner.log,
            "Mapping virtual NIC to physical host";
            "mapping" => ?&mapping,
        );
        let hdl = OpteHdl::open(OpteHdl::XDE_CTL)?;
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

    #[cfg(not(target_os = "illumos"))]
    pub fn set_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        info!(
            self.inner.log,
            "Mapping virtual NIC to physical host (ignored)";
            "mapping" => ?&mapping,
        );
        Ok(())
    }

    #[cfg(target_os = "illumos")]
    pub fn unset_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        use opte_ioctl::OpteHdl;

        info!(
            self.inner.log,
            "Clearing mapping of virtual NIC to physical host";
            "mapping" => ?&mapping,
        );

        let hdl = OpteHdl::open(OpteHdl::XDE_CTL)?;
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

    #[cfg(not(target_os = "illumos"))]
    pub fn unset_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        info!(
            self.inner.log,
            "Ignoring unset of virtual NIC mapping";
            "mapping" => ?&mapping,
        );
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
