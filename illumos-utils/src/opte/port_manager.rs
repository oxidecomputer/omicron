// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manager for all OPTE ports on a Helios system

use crate::opte::opte_firewall_rules;
use crate::opte::params::DeleteVirtualNetworkInterfaceHost;
use crate::opte::params::SetVirtualNetworkInterfaceHost;
use crate::opte::params::VpcFirewallRule;
use crate::opte::Error;
use crate::opte::Gateway;
use crate::opte::Port;
use crate::opte::Vni;
use ipnetwork::IpNetwork;
use omicron_common::api::external;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_common::api::internal::shared::ReifiedVpcRoute;
use omicron_common::api::internal::shared::ReifiedVpcRouteSet;
use omicron_common::api::internal::shared::RouterId;
use omicron_common::api::internal::shared::RouterTarget as ApiRouterTarget;
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
use oxide_vpc::api::RouterTarget;
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

#[derive(Debug)]
struct PortManagerInner {
    log: Logger,

    // Sequential identifier for each port on the system.
    next_port_id: AtomicU64,

    // IP address of the hosting sled on the underlay.
    underlay_ip: Ipv6Addr,

    // Map of all ports, keyed on the interface Uuid and its kind
    // (which includes the Uuid of the parent instance or service)
    ports: Mutex<BTreeMap<(Uuid, NetworkInterfaceKind), Port>>,

    // XX: vs. Hashmap?
    // XX: Should this be the UUID of the VPC? The rulesets are
    //     arguably shared v4+v6, although today we don't yet
    //     allow dual-stack, let alone v6.
    // Map of all current resolved routes
    routes: Mutex<HashMap<RouterId, HashSet<ReifiedVpcRoute>>>,
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
        nic: &NetworkInterface,
        source_nat: Option<SourceNatConfig>,
        ephemeral_ip: Option<IpAddr>,
        floating_ips: &[IpAddr],
        firewall_rules: &[VpcFirewallRule],
        dhcp_config: DhcpCfg,
    ) -> Result<(Port, PortTicket), Error> {
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
            let port = Port::new(
                port_name.clone(),
                nic.ip,
                mac,
                nic.slot,
                vni,
                nic.subnet,
                gateway,
                vnic,
            );
            let old = ports.insert((nic.id, nic.kind), port.clone());
            assert!(
                old.is_none(),
                "Duplicate OPTE port detected: interface_id = {}, kind = {:?}",
                nic.id,
                nic.kind,
            );
            (port, ticket)
        };

        // TODO: These should not be filled in like this, and should be informed
        //       by either our existing knowledge of current knowledge of system + custom
        //       routers OR we just await the router RPW filling this in for us.
        //       In future, ∃ VPCs *without* an Internet Gateway so we can't just
        //       plumb that in as well...

        // Add a router entry for this interface's subnet, directing traffic to the
        // VPC subnet.
        let route = AddRouterEntryReq {
            class: RouterClass::System,
            port_name: port_name.clone(),
            dest: vpc_subnet,
            target: RouterTarget::VpcSubnet(vpc_subnet),
        };
        #[cfg(target_os = "illumos")]
        hdl.add_router_entry(&route)?;
        debug!(
            self.inner.log,
            "Added VPC Subnet router entry";
            "port_name" => &port_name,
            "route" => ?route,
        );

        // TODO-remove
        //
        // See https://github.com/oxidecomputer/omicron/issues/1336
        //
        // This is another part of the workaround, allowing reply traffic from
        // the guest back out. Normally, OPTE would drop such traffic at the
        // router layer, as it has no route for that external IP address. This
        // allows such traffic through.
        //
        // Note that this exact rule will eventually be included, since it's one
        // of the default routing rules in the VPC System Router. However, that
        // will likely be communicated in a different way, or could be modified,
        // and this specific call should be removed in favor of sending the
        // routing rules the control plane provides.
        //
        // This rule sends all traffic that has no better match to the gateway.
        let dest = match vpc_subnet {
            IpCidr::Ip4(_) => "0.0.0.0/0",
            IpCidr::Ip6(_) => "::/0",
        }
        .parse()
        .unwrap();
        let route = AddRouterEntryReq {
            class: RouterClass::System,
            port_name: port_name.clone(),
            dest,
            target: RouterTarget::InternetGateway,
        };
        #[cfg(target_os = "illumos")]
        hdl.add_router_entry(&route)?;
        debug!(
            self.inner.log,
            "Added default internet gateway route entry";
            "port_name" => &port_name,
            "route" => ?route,
        );

        // XX: this is probably not the right initialisation here...
        // XX: VPC rules should probably come from ctl plane.
        // XX: need to delete safely after.
        let mut routes = self.inner.routes.lock().unwrap();
        routes.entry(RouterId { vni: nic.vni, subnet: None }).or_insert_with(
            || {
                let mut out = HashSet::new();
                out.insert(ReifiedVpcRoute {
                    dest: "0.0.0.0/0".parse().unwrap(),
                    target: ApiRouterTarget::InternetGateway,
                });
                out.insert(ReifiedVpcRoute {
                    dest: "::/0".parse().unwrap(),
                    target: ApiRouterTarget::InternetGateway,
                });
                out
            },
        );
        routes
            .entry(RouterId { vni: nic.vni, subnet: Some(nic.subnet) })
            .or_insert_with(|| HashSet::default());

        info!(
            self.inner.log,
            "Created OPTE port";
            "port" => ?&port,
        );
        Ok((port, ticket))
    }

    pub fn vpc_routes_list(&self) -> Vec<ReifiedVpcRouteSet> {
        let routes = self.inner.routes.lock().unwrap();
        routes
            .iter()
            .map(|(k, v)| ReifiedVpcRouteSet { id: *k, routes: v.clone() })
            .collect()
    }

    pub fn vpc_routes_ensure(
        &self,
        new_routes: Vec<ReifiedVpcRouteSet>,
    ) -> Result<(), Error> {
        let mut routes = self.inner.routes.lock().unwrap();
        let mut deltas = HashMap::new();
        for set in new_routes {
            let old = routes.get(&set.id);

            let (to_add, to_delete) = if let Some(old) = old {
                (
                    set.routes.difference(old).cloned().collect(),
                    old.difference(&set.routes).cloned().collect(),
                )
            } else {
                (set.routes.clone(), HashSet::new())
            };
            deltas.insert(set.id, (to_add, to_delete));

            routes.insert(set.id, set.routes);
        }
        drop(routes);

        let ports = self.inner.ports.lock().unwrap();
        #[cfg(target_os = "illumos")]
        let hdl = opte_ioctl::OpteHdl::open(opte_ioctl::OpteHdl::XDE_CTL)?;

        for port in ports.values() {
            let vni = external::Vni::try_from(port.vni().as_u32()).unwrap();
            let system_id = RouterId { vni, subnet: None };
            let system_delta = deltas.get(&system_id);

            let custom_id = RouterId { vni, subnet: Some(*port.subnet()) };

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
    pub fn set_virtual_nic_host(
        &self,
        mapping: &SetVirtualNetworkInterfaceHost,
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
        mapping: &SetVirtualNetworkInterfaceHost,
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
        _mapping: &DeleteVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        // TODO requires https://github.com/oxidecomputer/opte/issues/332

        slog::warn!(self.inner.log, "unset_virtual_nic_host unimplmented");
        Ok(())
    }

    #[cfg(not(target_os = "illumos"))]
    pub fn unset_virtual_nic_host(
        &self,
        _mapping: &DeleteVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        info!(self.inner.log, "Ignoring unset of virtual NIC mapping");
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
