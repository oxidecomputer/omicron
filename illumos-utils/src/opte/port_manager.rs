// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manager for all OPTE ports on a Helios system

use crate::addrobj::AddrObject;
use crate::dladm::OPTE_LINK_PREFIX;
use crate::opte::AttachedSubnet;
use crate::opte::EnsureAttachedSubnetResult;
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
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_common::api::internal::shared::PrivateIpv4Config;
use omicron_common::api::internal::shared::PrivateIpv6Config;
use omicron_common::api::internal::shared::ResolvedVpcRoute;
use omicron_common::api::internal::shared::ResolvedVpcRouteSet;
use omicron_common::api::internal::shared::ResolvedVpcRouteState;
use omicron_common::api::internal::shared::RouterId;
use omicron_common::api::internal::shared::RouterKind;
use omicron_common::api::internal::shared::RouterTarget as ApiRouterTarget;
use omicron_common::api::internal::shared::RouterVersion;
use omicron_common::api::internal::shared::VirtualNetworkInterfaceHost;
use omicron_generation_kinds::Generation;
use oxide_vpc::api::AddRouterEntryReq;
use oxide_vpc::api::AttachedSubnetConfig;
use oxide_vpc::api::ClearMcast2PhysReq;
use oxide_vpc::api::ClearMcastForwardingReq;
use oxide_vpc::api::DelRouterEntryReq;
use oxide_vpc::api::DetachSubnetResp;
use oxide_vpc::api::DhcpCfg;
use oxide_vpc::api::ExternalIpCfg;
use oxide_vpc::api::FilterMode;
use oxide_vpc::api::IpCfg;
use oxide_vpc::api::IpCidr;
use oxide_vpc::api::Ipv4Cfg;
use oxide_vpc::api::Ipv4Cidr;
use oxide_vpc::api::Ipv6Cfg;
use oxide_vpc::api::Ipv6Cidr;
use oxide_vpc::api::MacAddr;
use oxide_vpc::api::McastSubscribeReq;
use oxide_vpc::api::McastUnsubscribeReq;
use oxide_vpc::api::MulticastUnderlay;
use oxide_vpc::api::RouterClass;
use oxide_vpc::api::SNat4Cfg;
use oxide_vpc::api::SNat6Cfg;
use oxide_vpc::api::SetExternalIpsReq;
use oxide_vpc::api::SetMcast2PhysReq;
use oxide_vpc::api::SetMcastForwardingReq;
use oxide_vpc::api::SourceFilter;
use oxide_vpc::api::TransitIpConfig;
use oxide_vpc::api::VpcCfg;
use oxnet::IpNet;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use sled_agent_types::instance::ExternalIpConfig;
use sled_agent_types::instance::ExternalIpv4Config;
use sled_agent_types::instance::ExternalIpv6Config;
use sled_agent_types::instance::ResolvedVpcFirewallRule;
use sled_agent_types::inventory::NetworkInterface;
use sled_agent_types::inventory::NetworkInterfaceKind;
use sled_agent_types::multicast::ClearMcast2Phys;
use sled_agent_types::multicast::ClearMcastForwarding;
use sled_agent_types::multicast::Mcast2PhysMapping;
use sled_agent_types::multicast::McastFilterMode;
use sled_agent_types::multicast::McastForwardingEntry;
use sled_agent_types::multicast::McastForwardingNextHop;
use sled_agent_types::multicast::McastReplication;
use sled_agent_types::multicast::McastSourceFilter;
use sled_agent_types::multicast::MulticastGroupCfg;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use uuid::Uuid;

use super::AttachedSubnetKind;

/// Stored routes (and usage count) for a given VPC/subnet.
#[derive(Debug, Default, Clone)]
struct RouteSet {
    version: Option<RouterVersion>,
    routes: HashSet<ResolvedVpcRoute>,
    active_ports: usize,
}

/// The System VPC Internet Gateway default routes seeded onto service
/// ports, whose routes the control plane does not manage dynamically.
///
/// See the seeding rationale in `create_port`. The seed is a per-port need,
/// so paths that mutate shared route sets must preserve these entries on
/// seeded ports.
fn seeded_igw_defaults() -> [ResolvedVpcRoute; 2] {
    let target =
        ApiRouterTarget::InternetGateway(InternetGatewayRouterTarget::System);
    [
        ResolvedVpcRoute {
            dest: IpNet::V4(Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).unwrap()),
            target,
        },
        ResolvedVpcRoute {
            dest: IpNet::V6(Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 0).unwrap()),
            target,
        },
    ]
}

/// Mutable per-port state tracked alongside the immutable `Port`.
#[derive(Debug)]
struct PortState {
    port: Port,
    /// Active multicast subscriptions, mapping group IP to source filter.
    mcast_subscriptions: HashMap<IpAddr, SourceFilter>,
    /// The last-applied external IP configuration.
    ///
    /// When the external-IP-to-internet-gateway (EIP-to-IGW) mappings
    /// change, NAT rules need fresh gateway tags. Instance ports get theirs
    /// from the instance manager, but no equivalent exists for probe ports,
    /// so `external_ips_refresh_probes` replays this stored configuration
    /// instead.
    external_ips: ExternalIpConfig,
}

impl PortState {
    fn new(port: Port, external_ips: ExternalIpConfig) -> Self {
        Self { port, mcast_subscriptions: HashMap::new(), external_ips }
    }
}

/// This owns the UDP sockets that hold open NIC multicast MAC filters for
/// underlay multicast addresses (see [opte#908] for context).
///
/// Encapsulated so the leaf-lock invariant is structural rather than
/// manually enforced: methods on this type take only what they need by
/// parameter (`underlay_nics`) and have no back-ref to `PortManager` or
/// `PortManagerInner`. Code inside the locked region cannot reach the other
/// inner locks because it does not have access to them.
///
/// [opte#908]: https://github.com/oxidecomputer/opte/issues/908
#[derive(Debug)]
struct MulticastFilterMap {
    sockets: Mutex<HashMap<Ipv6Addr, UdpSocket>>,
}

impl MulticastFilterMap {
    fn new() -> Self {
        Self { sockets: Mutex::new(HashMap::new()) }
    }

    /// Join the underlay multicast group `addr` on every configured NIC,
    /// holding a UDP socket open per address to keep the NIC MAC filters
    /// installed.
    ///
    /// The operation is all-or-nothing: callers can keep the corresponding
    /// M2P entry only when every configured NIC has joined successfully.
    fn join(
        &self,
        log: &Logger,
        addr: Ipv6Addr,
        underlay_nics: &[AddrObject],
    ) -> bool {
        if underlay_nics.is_empty() {
            return false;
        }

        let mut sockets = self.sockets.lock().unwrap();
        if sockets.contains_key(&addr) {
            return true;
        }

        let sock = match UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0)) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    log,
                    "Failed to bind UDP socket for underlay multicast filter";
                    "addr" => %addr,
                    "error" => %e,
                );
                return false;
            }
        };

        // Minimize the receive buffer.
        //
        // This socket exists solely to trigger MAC filter programming. xde
        // intercepts packets before they reach the socket. The small buffer
        // limits resource waste if that invariant is ever violated.
        if let Err(e) = sock.set_nonblocking(true) {
            warn!(
                log,
                "Failed to set underlay multicast socket non-blocking";
                "addr" => %addr,
                "error" => %e,
            );
        }

        // The kernel may round up from 1 to its own minimum.
        let _ = nix::sys::socket::setsockopt(
            &sock,
            nix::sys::socket::sockopt::RcvBuf,
            &1,
        );

        if !Self::join_nics(log, addr, &sock, underlay_nics) {
            warn!(
                log,
                "not all NIC joins succeeded for underlay multicast group, \
                 will retry on next call";
                "addr" => %addr,
            );
            return false;
        }

        sockets.insert(addr, sock);
        true
    }

    /// Join `addr` on every configured NIC.
    fn join_nics(
        log: &Logger,
        addr: Ipv6Addr,
        sock: &UdpSocket,
        underlay_nics: &[AddrObject],
    ) -> bool {
        for nic in underlay_nics {
            let nic_name = nic.interface();
            let if_index = match nix::net::if_::if_nametoindex(nic_name) {
                Ok(if_index) => if_index,
                Err(e) => {
                    warn!(
                        log,
                        "Failed to resolve underlay NIC index";
                        "nic" => nic_name,
                        "error" => %e,
                    );
                    return false;
                }
            };

            match sock.join_multicast_v6(&addr, if_index) {
                Ok(()) => {
                    debug!(
                        log,
                        "Joined underlay multicast group on NIC";
                        "addr" => %addr,
                        "nic" => nic_name,
                        "if_index" => if_index,
                    );
                }
                Err(e) => {
                    warn!(
                        log,
                        "Failed to join underlay multicast group on NIC";
                        "addr" => %addr,
                        "nic" => nic_name,
                        "if_index" => if_index,
                        "error" => %e,
                    );
                    return false;
                }
            }
        }

        true
    }

    /// Report whether a filter socket is already held for `addr`, which
    /// means every configured NIC joined successfully on a prior call.
    fn is_joined(&self, addr: &Ipv6Addr) -> bool {
        self.sockets.lock().unwrap().contains_key(addr)
    }

    /// Drop the UDP socket for an underlay multicast address, removing the
    /// NIC MAC filter entries with it.
    fn leave(&self, log: &Logger, addr: Ipv6Addr) {
        let mut sockets = self.sockets.lock().unwrap();
        if sockets.remove(&addr).is_some() {
            debug!(
                log,
                "Removed underlay multicast filter socket";
                "addr" => %addr,
            );
        }
    }
}

/// Convert a `MulticastGroupCfg` into OPTE's `SourceFilter`.
///
/// Empty sources maps to ASM (EXCLUDE with no entries, accepting all
/// sources). Non-empty sources maps to SSM (INCLUDE with the listed
/// sources).
fn multicast_cfg_to_source_filter(cfg: &MulticastGroupCfg) -> SourceFilter {
    if cfg.sources.is_empty() {
        SourceFilter::default()
    } else {
        SourceFilter::Include(
            cfg.sources
                .iter()
                .map(|s| oxide_vpc::api::IpAddr::from(*s))
                .collect(),
        )
    }
}

/// Internet Gateway mappings for external IPs, plus the bookkeeping
/// needed to retry a failed port refresh.
#[derive(Debug)]
struct EipGatewayState {
    /// Mappings of associated Internet Gateways for all External IPs
    /// attached to each NIC.
    ///
    /// IGW IDs are specific to the VPC of each NIC.
    mappings: HashMap<Uuid, HashMap<IpAddr, HashSet<Uuid>>>,
    /// Advanced each time the mappings change.
    generation: Generation,
    /// Latest generation whose port refresh completed successfully.
    /// While this trails `generation`, a refresh remains pending, so an
    /// identical re-push after a failed refresh retries instead of
    /// leaving ports keyed to stale mappings until the next change.
    refreshed_generation: Generation,
}

impl Default for EipGatewayState {
    fn default() -> Self {
        Self {
            mappings: HashMap::new(),
            generation: Generation::new(),
            refreshed_generation: Generation::new(),
        }
    }
}

#[derive(Debug)]
struct PortManagerInner {
    log: Logger,

    /// Sequential identifier for each port on the system.
    next_port_id: AtomicU64,

    /// IP address of the hosting sled on the underlay.
    underlay_ip: Ipv6Addr,

    /// Map of all ports and their mutable state, keyed on the interface
    /// Uuid and its kind (which includes the Uuid of the parent instance
    /// or service).
    ports: Mutex<BTreeMap<(Uuid, NetworkInterfaceKind), PortState>>,

    /// Map of all current resolved routes.
    routes: Mutex<HashMap<RouterId, RouteSet>>,

    /// Internet Gateway mappings for external IPs. See [`EipGatewayState`].
    eip_gateways: Mutex<EipGatewayState>,

    /// Underlay NIC address objects (e.g., for "cxgbe0", "cxgbe1").
    ///
    /// This is used to program NIC multicast MAC filters via
    /// [`UdpSocket::join_multicast_v6`].
    // Note: Empty in tests where no real underlay NICs exist.
    underlay_nics: Vec<AddrObject>,

    /// UDP sockets held open to maintain NIC multicast MAC filters.
    ///
    /// On Chelsio T6 hardware the NIC will not deliver multicast frames to
    /// xde unless the corresponding multicast MAC filter is programmed.
    /// Joining an IPv6 multicast group on a UDP socket causes the
    /// kernel to call `mac_multicast_add` on the interface, which
    /// programs the filter. The socket receives no data (xde's
    /// siphon/flow hook intercepts first) and exists solely to hold
    /// the filter entry.
    ///
    /// Dropping the socket removes the filter.
    ///
    /// See <https://github.com/oxidecomputer/opte/issues/908>.
    mcast_underlay_sockets: MulticastFilterMap,
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
    pub external_ips: &'a ExternalIpConfig,
    pub firewall_rules: &'a [ResolvedVpcFirewallRule],
    pub dhcp_config: DhcpCfg,
    pub attached_subnets: Vec<AttachedSubnet>,
    pub multicast_groups: &'a [MulticastGroupCfg],
    /// MTU to set on the xde device, in bytes. If `None`, OPTE applies its
    /// default (1500). Used by jumbo-frame opt-in.
    pub mtu: Option<u32>,
}

impl<'a> TryFrom<&PortCreateParams<'a>> for IpCfg {
    type Error = Error;

    fn try_from(params: &PortCreateParams) -> Result<IpCfg, Error> {
        omicron_to_opte_ip_config(
            &params.nic.ip_config,
            params.external_ips,
            &params.attached_subnets,
        )
    }
}

fn omicron_to_opte_ip_config(
    private_ips: &PrivateIpConfig,
    external_ips: &ExternalIpConfig,
    attached_subnets: &[AttachedSubnet],
) -> Result<IpCfg, Error> {
    let cfg = match private_ips {
        PrivateIpConfig::V4(private_v4) => {
            if external_ips.v6.is_some() {
                return Err(Error::InvalidPortIpConfig(String::from(
                    "No private IPv6 stack for external IPv6 IP config",
                )));
            }
            IpCfg::Ipv4(build_opte_ipv4_config(
                private_v4,
                external_ips.v4.as_ref(),
                attached_subnets,
            ))
        }
        PrivateIpConfig::V6(private_v6) => {
            if external_ips.v4.is_some() {
                return Err(Error::InvalidPortIpConfig(String::from(
                    "No private IPv4 stack for external IPv4 IP config",
                )));
            }
            IpCfg::Ipv6(build_opte_ipv6_config(
                private_v6,
                external_ips.v6.as_ref(),
                attached_subnets,
            ))
        }
        PrivateIpConfig::DualStack { v4: private_v4, v6: private_v6 } => {
            let ipv4 = build_opte_ipv4_config(
                private_v4,
                external_ips.v4.as_ref(),
                attached_subnets,
            );
            let ipv6 = build_opte_ipv6_config(
                private_v6,
                external_ips.v6.as_ref(),
                attached_subnets,
            );
            IpCfg::DualStack { ipv4, ipv6 }
        }
    };
    Ok(cfg)
}

fn build_opte_ipv4_config(
    private_v4: &PrivateIpv4Config,
    external_v4: Option<&ExternalIpv4Config>,
    attached_subnets: &[AttachedSubnet],
) -> Ipv4Cfg {
    let gateway_ip = private_v4.opte_gateway().into();
    let vpc_subnet = Ipv4Cidr::from(Ipv4Network::from(*private_v4.subnet()));
    let private_ip = (*private_v4.ip()).into();
    let external_ips = build_external_ipv4_config(external_v4);
    let transit_ips = private_v4
        .transit_ips
        .iter()
        .map(|ip| {
            let cidr =
                Ipv4Cidr::new(ip.addr().into(), ip.width().try_into().unwrap());
            let cfg = TransitIpConfig { allow_in: true, allow_out: true };
            (cidr, cfg)
        })
        .collect();
    let attached_subnets = attached_subnets
        .iter()
        .filter_map(|subnet| match subnet.cidr {
            IpCidr::Ip4(ipv4) => {
                let is_external = match subnet.kind {
                    AttachedSubnetKind::Vpc => false,
                    AttachedSubnetKind::External => true,
                };
                Some((ipv4, AttachedSubnetConfig { is_external }))
            }
            IpCidr::Ip6(_) => None,
        })
        .collect();
    Ipv4Cfg {
        vpc_subnet,
        private_ip,
        gateway_ip,
        external_ips,
        attached_subnets,
        transit_ips,
    }
}

fn build_opte_ipv6_config(
    private_v6: &PrivateIpv6Config,
    external_v6: Option<&ExternalIpv6Config>,
    attached_subnets: &[AttachedSubnet],
) -> Ipv6Cfg {
    let gateway_ip = private_v6.opte_gateway().into();
    let vpc_subnet = Ipv6Cidr::from(Ipv6Network::from(*private_v6.subnet()));
    let private_ip = (*private_v6.ip()).into();
    let external_ips = build_external_ipv6_config(external_v6);
    let transit_ips = private_v6
        .transit_ips
        .iter()
        .map(|ip| {
            let cidr =
                Ipv6Cidr::new(ip.addr().into(), ip.width().try_into().unwrap());
            let cfg = TransitIpConfig { allow_in: true, allow_out: true };
            (cidr, cfg)
        })
        .collect();
    let attached_subnets = attached_subnets
        .iter()
        .filter_map(|subnet| match subnet.cidr {
            IpCidr::Ip4(_) => None,
            IpCidr::Ip6(ipv6) => {
                let is_external = match subnet.kind {
                    AttachedSubnetKind::Vpc => false,
                    AttachedSubnetKind::External => true,
                };
                Some((ipv6, AttachedSubnetConfig { is_external }))
            }
        })
        .collect();
    Ipv6Cfg {
        vpc_subnet,
        private_ip,
        gateway_ip,
        external_ips,
        attached_subnets,
        transit_ips,
    }
}

// Build an ExternalIpCfg from parameters.
fn build_external_ipv4_config(
    external_v4: Option<&ExternalIpv4Config>,
) -> ExternalIpCfg<oxide_vpc::api::Ipv4Addr> {
    let Some(v4) = external_v4 else {
        return ExternalIpCfg {
            snat: None,
            ephemeral_ip: None,
            floating_ips: vec![],
        };
    };
    let snat = v4.source_nat.map(|snat| SNat4Cfg {
        external_ip: snat.ip.into(),
        ports: snat.port_range(),
    });
    let ephemeral_ip = v4.ephemeral_ip.map(Into::into);
    let floating_ips =
        v4.floating_ips.iter().copied().map(Into::into).collect();
    ExternalIpCfg { snat, ephemeral_ip, floating_ips }
}

// Build an OPTE External IPv6 configuration from parameters.
fn build_external_ipv6_config(
    external_v6: Option<&ExternalIpv6Config>,
) -> ExternalIpCfg<oxide_vpc::api::Ipv6Addr> {
    let Some(v6) = external_v6 else {
        return ExternalIpCfg {
            snat: None,
            ephemeral_ip: None,
            floating_ips: vec![],
        };
    };
    let snat = v6.source_nat.map(|snat| SNat6Cfg {
        external_ip: snat.ip.into(),
        ports: snat.port_range(),
    });
    let ephemeral_ip = v6.ephemeral_ip.map(Into::into);
    let floating_ips =
        v6.floating_ips.iter().copied().map(Into::into).collect();
    ExternalIpCfg { snat, ephemeral_ip, floating_ips }
}

/// The port manager controls all OPTE ports on a single host.
#[derive(Debug, Clone)]
pub struct PortManager {
    inner: Arc<PortManagerInner>,
}

impl PortManager {
    /// Create a new manager, for creating OPTE ports.
    ///
    /// # Arguments
    ///
    /// * `log`: Logger inherited by the manager and its ports.
    /// * `underlay_ip`: This sled's underlay IPv6 address.
    /// * `underlay_nics`: Underlay NIC interfaces for multicast MAC
    ///   filter rehydration. When non-empty, the constructor performs
    ///   kernel I/O: one ioctl to list existing multicast-to-physical
    ///   (M2P) mappings, then one `setsockopt(IPV6_JOIN_GROUP)` per
    ///   mapping per NIC.
    pub fn new(
        log: Logger,
        underlay_ip: Ipv6Addr,
        underlay_nics: &[AddrObject],
    ) -> Self {
        let inner = Arc::new(PortManagerInner {
            log,
            next_port_id: AtomicU64::new(0),
            underlay_ip,
            ports: Mutex::new(BTreeMap::new()),
            routes: Mutex::new(Default::default()),
            eip_gateways: Mutex::new(Default::default()),
            underlay_nics: underlay_nics.to_vec(),
            mcast_underlay_sockets: MulticastFilterMap::new(),
        });

        let mgr = Self { inner };

        // Re-open MAC filter sockets for any M2P mappings that
        // survived in the xde kernel module across a sled-agent
        // restart. Without this, the NIC's multicast MAC filters
        // are lost when the old process exits.
        //
        // This runs eagerly at startup rather than deferring to the
        // reconciler. The Nexus convergence loop re-runs `set_mcast_m2p`
        // for active groups and would eventually re-create the filters,
        // but that waits on the next reconciler pass and covers only
        // groups still active in the database. Re-opening here closes
        // that window.
        //
        // Cost accrued: one `dump_m2p` ioctl + one `setsockopt(IPV6_JOIN_GROUP)`
        // per remaining group per underlay NIC. This is bounded by active
        // groups on this sled and runs only at sled-agent startup.
        mgr.rehydrate_underlay_multicast_filters();

        mgr
    }

    /// Re-open underlay multicast filter sockets for M2P mappings
    /// that already exist in the xde kernel module.
    ///
    /// Called at startup to cover the sled-agent restart case where
    /// OPTE kernel state persists but userspace socket state is lost.
    ///
    /// On a cold boot (no prior xde state), `list_mcast_m2p` returns
    /// an error or an empty list.
    fn rehydrate_underlay_multicast_filters(&self) {
        if self.inner.underlay_nics.is_empty() {
            return;
        }

        let mappings = match self.list_mcast_m2p() {
            Ok(m) => m,
            Err(e) => {
                // Expected on cold boot when xde has no prior state.
                debug!(
                    self.inner.log,
                    "No M2P mappings to rehydrate";
                    "error" => InlineErrorChain::new(&e),
                );
                return;
            }
        };

        let mut failed: Vec<String> = Vec::new();
        for mapping in &mappings {
            if self.inner.mcast_underlay_sockets.join(
                &self.inner.log,
                mapping.underlay,
                &self.inner.underlay_nics,
            ) {
                continue;
            }
            // Clear the surviving xde M2P entry so `converge_m2p` sees
            // the gap on its next pass and re-issues `set_mcast_m2p`,
            // which retries the underlay join. Without this, the entry
            // stays in xde and convergence treats it as already
            // converged, silently dropping traffic for the group until
            // it is cycled inactive→active.
            let clear_req = ClearMcast2Phys {
                group: mapping.group,
                underlay: mapping.underlay,
            };
            if let Err(e) = self.clear_mcast_m2p(&clear_req) {
                warn!(
                    self.inner.log,
                    "Failed to clear M2P after rehydration join failure, \
                     group will silently drop traffic until convergence \
                     retries";
                    "group" => %mapping.group,
                    "underlay" => %mapping.underlay,
                    "error" => InlineErrorChain::new(&e),
                );
            }
            failed.push(mapping.underlay.to_string());
        }

        let total = mappings.len();
        let succeeded = total - failed.len();
        if !mappings.is_empty() {
            info!(
                self.inner.log,
                "Rehydrated underlay multicast filter sockets";
                "succeeded" => succeeded,
                "total" => total,
            );
        }
        if !failed.is_empty() {
            warn!(
                self.inner.log,
                "Some underlay multicast filter sockets failed to \
                 rehydrate, convergence retries those joins on the \
                 next pass, M2P entries whose clear failed may remain \
                 in xde until then";
                "failed_count" => failed.len(),
                "total" => total,
                "failed_underlay_addrs" => ?failed,
            );
        }
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
            external_ips,
            firewall_rules,
            dhcp_config,
            attached_subnets: _,
            multicast_groups,
            mtu,
        } = params;
        let is_service =
            matches!(nic.kind, NetworkInterfaceKind::Service { .. });
        let is_instance =
            matches!(nic.kind, NetworkInterfaceKind::Instance { .. });
        let is_probe = matches!(nic.kind, NetworkInterfaceKind::Probe { .. });
        let mac = *nic.mac;
        let vni = Vni::new(nic.vni).unwrap();
        let gateway = Gateway::from_ip_config(&nic.ip_config);
        let vpc_cfg = VpcCfg {
            ip_cfg,
            guest_mac: MacAddr::from(nic.mac.into_array()),
            gateway_mac: MacAddr::from(gateway.mac.into_array()),
            vni,
            phys_ip: self.inner.underlay_ip.into(),
            dhcp: dhcp_config,
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
        );
        let hdl = {
            let hdl = Handle::new()?;
            hdl.create_xde(&port_name, vpc_cfg, mtu)?;
            hdl
        };
        let (port, ticket) = {
            let ticket = PortTicket::new(nic.id, nic.kind, self.inner.clone());
            let port = Port::new(PortData {
                name: port_name.clone(),
                ip: nic.ip_config.clone(),
                mac,
                slot: nic.slot,
                vni,
                gateway,
            });

            // NOTE: We may add external IPs below, which can fail. If that
            // does, we drop the `ticket` on the way out of this block. That
            // attempts to acquire this lock, in order to remove itself on drop.
            // We need to drop the lock before that, to avoid a deadlock, so
            // let's do it right away, after inserting.
            let old = self.inner.ports.lock().unwrap().insert(
                (nic.id, nic.kind),
                PortState::new(port.clone(), external_ips.clone()),
            );
            assert!(
                old.is_none(),
                "Duplicate OPTE port detected: interface_id = {}, kind = {:?}",
                nic.id,
                nic.kind,
            );

            // Service ports have no dynamically maintained EIP-to-IGW
            // mappings today. A gateway tag without a mapping would break
            // their external IPs, so their entries stay untagged at both
            // the `nat` and `router` layer.
            if is_instance || is_probe {
                // This is effectively re-asserting the external IP config in order to
                // set the EIP<->IGW mapping. While this should be part of `vpc_cfg`,
                // this currently needs to happen here to prevent a case where an old
                // mapping is not yet removed (and so no 'change' happens to trigger
                // `Instance::refresh_external_ips_inner`), and to prevent updates
                // racing with nexus before an instance/port are reachable from their
                // respective managers.
                self.external_ips_ensure_port(&port, nic.id, external_ips)?;
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
        // The shared route set for a router key holds only routes the
        // control plane pushes through the vpc_routes RPW, so it starts
        // empty for every port kind. Instance and probe ports receive all
        // of their routes that way. Probe ports' outbound NAT rules match
        // the Internet Gateway tag exactly, so they also depend on the
        // EIP-to-IGW mappings ensured above and refreshed via
        // `external_ips_refresh_probes`.
        //
        // Service ports additionally need default routes targeting the
        // System VPC Internet Gateway before the control plane database has
        // started (bootstrapping NTP or other very early services). These
        // are programmed onto the service port only. Merging them into the
        // shared set would leak the seed onto instance or probe ports
        // sharing the router key and make the next route-set replacement
        // compute deletions against routes Nexus never issued.

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
            let route_set = route_map.entry(key).or_default();
            route_set.active_ports += 1;
            let class = match key.kind {
                RouterKind::System => RouterClass::System,
                RouterKind::Custom(_) => RouterClass::Custom,
            };
            // The IGW defaults for a service port go onto this port only,
            // skipping any identical route the RPW has already placed in
            // the shared set.
            let port_only_seed = if is_service {
                seeded_igw_defaults()
                    .into_iter()
                    .filter(|route| !route_set.routes.contains(route))
                    .collect()
            } else {
                Vec::new()
            };
            for route in route_set.routes.iter().chain(&port_only_seed) {
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

        // Configure multicast group subscriptions if any were
        // provided at instance start.
        if !multicast_groups.is_empty() {
            self.multicast_groups_ensure(nic.id, nic.kind, multicast_groups)?;
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
        // to prevent several nexuses computing and applying deltas
        // out of order.
        let ports = self.inner.ports.lock().unwrap();
        let hdl = Handle::new()?;

        // Propagate deltas out to all ports.
        for ((_, nic_kind), port_state) in ports.iter() {
            let port = &port_state.port;
            // Service ports carry seeded IGW defaults that Nexus does not
            // manage. A route-set replacement must not strip the seed from
            // these ports, nor program a duplicate when the new set holds
            // an identical default.
            let keep_seed =
                matches!(nic_kind, NetworkInterfaceKind::Service { .. });
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
                    if keep_seed && seeded_igw_defaults().contains(route) {
                        continue;
                    }
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
                    if keep_seed && seeded_igw_defaults().contains(route) {
                        continue;
                    }
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
    /// Returns `Some(generation)` when attached ports must have their
    /// external IPs re-ensured, either because the mappings changed or a
    /// prior refresh never completed. Callers acknowledge a completed
    /// refresh via [`Self::eip_gateways_refreshed`].
    pub fn set_eip_gateways(
        &self,
        mappings: ExternalIpGatewayMap,
    ) -> Option<Generation> {
        let mut state = self.inner.eip_gateways.lock().unwrap();
        if state.mappings != mappings.mappings {
            state.mappings = mappings.mappings;
            state.generation = state.generation.next();
        }
        (state.generation > state.refreshed_generation)
            .then_some(state.generation)
    }

    /// Record that ports were successfully refreshed against the mappings
    /// at `generation`. A newer push may have raced the refresh, in which
    /// case the pending state is preserved.
    pub fn eip_gateways_refreshed(&self, generation: Generation) {
        let mut state = self.inner.eip_gateways.lock().unwrap();
        state.refreshed_generation = state.refreshed_generation.max(generation);
    }

    /// Lookup an OPTE port, and ensure its external IP config is up to date.
    pub fn external_ips_ensure(
        &self,
        nic_id: Uuid,
        nic_kind: NetworkInterfaceKind,
        external_ips: &ExternalIpConfig,
    ) -> Result<(), Error> {
        let mut ports = self.inner.ports.lock().unwrap();
        let port_state =
            ports.get_mut(&(nic_id, nic_kind)).ok_or_else(|| {
                Error::ExternalIpUpdateMissingPort(nic_id, nic_kind)
            })?;

        self.external_ips_ensure_port(&port_state.port, nic_id, external_ips)?;
        port_state.external_ips = external_ips.clone();
        Ok(())
    }

    /// Re-apply the external IP config of every probe port, picking up the
    /// current Internet Gateway mappings.
    ///
    /// Instance ports are refreshed through the instance manager, which
    /// serializes this against in-flight external IP changes. Probe external
    /// IPs are fixed at provisioning and have no other writer, so they are
    /// re-ensured here from the per-port stored config.
    ///
    /// Every probe port is attempted before reporting failure so one bad
    /// port does not leave the rest keyed to stale mappings.
    ///
    /// Note: this returns the first error encountered.
    pub fn external_ips_refresh_probes(&self) -> Result<(), Error> {
        // Snapshot probe ports so the OPTE ioctls below run without holding
        // the port map lock, which would stall unrelated port operations.
        let probes: Vec<(Uuid, Port, ExternalIpConfig)> = {
            let ports = self.inner.ports.lock().unwrap();
            ports
                .iter()
                .filter(|((_, nic_kind), _)| {
                    matches!(nic_kind, NetworkInterfaceKind::Probe { .. })
                })
                .map(|((nic_id, _), port_state)| {
                    (
                        *nic_id,
                        port_state.port.clone(),
                        port_state.external_ips.clone(),
                    )
                })
                .collect()
        };

        // Attempt every port even after a failure (no short-circuiting),
        // surfacing the first error.
        probes
            .into_iter()
            .map(|(nic_id, port, external_ips)| {
                self.external_ips_ensure_port(&port, nic_id, &external_ips)
                    .inspect_err(|e| {
                        error!(
                            self.inner.log,
                            "failed to refresh external IPs for probe port";
                            "nic_id" => %nic_id,
                            "port" => port.name(),
                            "error" => %e,
                        );
                    })
            })
            .fold(Ok(()), Result::and)
    }

    /// Ensure external IPs for an OPTE port are up to date.
    pub fn external_ips_ensure_port(
        &self,
        port: &Port,
        nic_id: Uuid,
        external_ips: &ExternalIpConfig,
    ) -> Result<(), Error> {
        let egw_lock = self.inner.eip_gateways.lock().unwrap();
        let inet_gw_map = egw_lock.mappings.get(&nic_id).cloned();
        drop(egw_lock);

        // NOTE: The Option::map() call here is a bit confusing.
        //
        // The `SetExternalIpsReq` type uses an `Option` around the IP
        // configuration. However, `build_external_ipv{4,6}_config` accept an
        // option and "push" that into the returned configuration type, i.e.,
        // its fields are optional rather than returning an option.
        //
        // We map the option so we can get the `None` we need for the
        // `SetExternalIpsReq`. But that does mean we always call
        // `build_external_ipv{4,6}_config` with `Some(_)`.
        let external_ips_v4 = external_ips
            .v4
            .as_ref()
            .map(|v4| build_external_ipv4_config(Some(v4)));
        let external_ips_v6 = external_ips
            .v6
            .as_ref()
            .map(|v6| build_external_ipv6_config(Some(v6)));
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

    /// Ensure multicast group subscriptions for an OPTE port match the
    /// requested set. This diffs current vs new state and issues
    /// subscribe/unsubscribe ioctls as needed.
    pub fn multicast_groups_ensure(
        &self,
        nic_id: Uuid,
        nic_kind: NetworkInterfaceKind,
        multicast_groups: &[MulticastGroupCfg],
    ) -> Result<(), Error> {
        // Validate and build the new subscription set before acquiring locks.
        let new_subs: HashMap<IpAddr, SourceFilter> = multicast_groups
            .iter()
            .map(|group| {
                if !group.group_ip.is_multicast() {
                    return Err(Error::InvalidPortIpConfig(format!(
                        "not a multicast address: {}",
                        group.group_ip,
                    )));
                }
                Ok((group.group_ip, multicast_cfg_to_source_filter(group)))
            })
            .collect::<Result<_, Error>>()?;

        let hdl = Handle::new()?;

        let mut ports = self.inner.ports.lock().unwrap();
        let port_state =
            ports.get_mut(&(nic_id, nic_kind)).ok_or_else(|| {
                Error::MulticastUpdateMissingPort(nic_id, nic_kind)
            })?;
        let port_name = port_state.port.name().to_string();

        // Unsubscribe groups that are no longer requested.
        let to_remove: Vec<IpAddr> = port_state
            .mcast_subscriptions
            .keys()
            .filter(|g| !new_subs.contains_key(g))
            .copied()
            .collect();

        for group_ip in &to_remove {
            debug!(
                self.inner.log,
                "unsubscribing from multicast group";
                "port" => &port_name,
                "group" => %group_ip,
            );

            hdl.mcast_unsubscribe(&McastUnsubscribeReq {
                port_name: port_name.clone(),
                group: (*group_ip).into(),
            })?;

            port_state.mcast_subscriptions.remove(group_ip);
        }

        // Subscribe to new groups or update changed filters.
        let added: Vec<IpAddr> = new_subs
            .iter()
            .filter(|(group_ip, filter)| {
                port_state
                    .mcast_subscriptions
                    .get(group_ip)
                    .is_none_or(|current| current != *filter)
            })
            .map(|(group_ip, _)| *group_ip)
            .collect();

        for group_ip in &added {
            let filter = &new_subs[group_ip];
            debug!(
                self.inner.log,
                "subscribing to multicast group";
                "port" => &port_name,
                "group" => %group_ip,
                "filter" => ?filter,
            );

            hdl.mcast_subscribe(&McastSubscribeReq {
                port_name: port_name.clone(),
                group: (*group_ip).into(),
                filter: filter.clone(),
            })?;

            port_state.mcast_subscriptions.insert(*group_ip, filter.clone());
        }

        if !added.is_empty() || !to_remove.is_empty() {
            info!(
                self.inner.log,
                "multicast subscriptions updated";
                "port" => &port_name,
                "added" => ?added,
                "removed" => ?to_remove,
                "active_groups" => port_state.mcast_subscriptions.len(),
            );
        } else {
            debug!(
                self.inner.log,
                "multicast subscriptions reconciled, no change";
                "port" => &port_name,
                "active_groups" => port_state.mcast_subscriptions.len(),
            );
        }

        Ok(())
    }

    /// Install a multicast overlay-to-underlay (M2P) mapping in OPTE.
    ///
    /// Also installs the corresponding multicast MAC filter on each underlay
    /// NIC by joining the underlay IPv6 multicast group on a UDP socket, so
    /// the NIC delivers frames to xde. See `mcast_underlay_sockets` docs.
    ///
    /// Out-of-band state (e.g., set via `opteadm`) is not consulted on this
    /// path: the xde `set_m2p` ioctl is upsert by group, so any prior entry
    /// for `req.group` is overwritten. The convergence loop reads xde via
    /// `list_mcast_m2p` and reconciles against desired state on each pass,
    /// so even unmanaged entries do not wedge convergence.
    pub fn set_mcast_m2p(&self, req: &Mcast2PhysMapping) -> Result<(), Error> {
        let addr: Ipv6Addr = req.underlay;

        let underlay = MulticastUnderlay::new(addr.into())
            .map_err(|_| Error::InvalidMcastUnderlay(addr))?;

        // The convergence loop re-runs this for active groups on every
        // pass, even when the mapping is already listed, so that a mapping
        // stranded by a failed join rollback is retried.
        //
        // We return early when both halves are already in place: the NIC
        // MAC filters (tracked by the filter socket) and the xde entry.
        // This keeps the repeated calls from re-issuing the `set_m2p` upsert
        // ioctl when nothing changed.
        let nics_joined = self.inner.underlay_nics.is_empty()
            || self.inner.mcast_underlay_sockets.is_joined(&addr);

        if nics_joined
            && self
                .list_mcast_m2p()?
                .iter()
                .any(|m| m.group == req.group && m.underlay == addr)
        {
            debug!(
                self.inner.log,
                "multicast overlay-to-underlay mapping already in place";
                "group" => %req.group,
                "underlay" => %addr,
            );
            return Ok(());
        }

        info!(
            self.inner.log,
            "Setting multicast overlay-to-underlay mapping";
            "group" => %req.group,
            "underlay" => %addr,
        );

        let hdl = Handle::new()?;
        hdl.set_m2p(&SetMcast2PhysReq { group: req.group.into(), underlay })?;

        // In legit scenarios, install the NIC multicast MAC filter via a
        // UDP socket join. If no NIC accepts the join, roll back the
        // xde M2P entry so the failure is visible in `list_mcast_m2p`
        // right away. The convergence loop re-runs `set_mcast_m2p` for
        // active groups even when the mapping is listed, so a failed
        // rollback leaves a stale xde entry but does not block the
        // group indefinitely. The join is retried on the next pass.
        //
        // In tests / sim mode, `underlay_nics` is empty and there is no
        // MAC filter to install, so this whole step is skipped.
        if !self.inner.underlay_nics.is_empty() {
            let joined = self.inner.mcast_underlay_sockets.join(
                &self.inner.log,
                addr,
                &self.inner.underlay_nics,
            );
            if !joined {
                warn!(
                    self.inner.log,
                    "underlay NIC join failed, rolling back xde M2P \
                     entry to force convergence retry";
                    "group" => %req.group,
                    "underlay" => %addr,
                );
                if let Err(e) = hdl.clear_m2p(&ClearMcast2PhysReq {
                    group: req.group.into(),
                    underlay,
                }) {
                    warn!(
                        self.inner.log,
                        "failed to roll back xde M2P entry after NIC \
                         join failure, next convergence pass retries \
                         the join";
                        "group" => %req.group,
                        "underlay" => %addr,
                        "error" => %e,
                    );
                }
                return Err(Error::UnderlayMcastJoinFailed(addr));
            }
        }

        Ok(())
    }

    /// Remove a multicast overlay-to-underlay (M2P) mapping from OPTE.
    ///
    /// Drops the corresponding underlay MAC filter socket, removing the
    /// NIC multicast MAC filter entry.
    pub fn clear_mcast_m2p(&self, req: &ClearMcast2Phys) -> Result<(), Error> {
        let addr: Ipv6Addr = req.underlay;

        info!(
            self.inner.log,
            "Clearing multicast overlay-to-underlay mapping";
            "group" => %req.group,
            "underlay" => %addr,
        );

        let underlay = MulticastUnderlay::new(addr.into())
            .map_err(|_| Error::InvalidMcastUnderlay(addr))?;
        let hdl = Handle::new()?;
        hdl.clear_m2p(&ClearMcast2PhysReq {
            group: req.group.into(),
            underlay,
        })?;

        self.inner.mcast_underlay_sockets.leave(&self.inner.log, addr);

        Ok(())
    }

    /// Set multicast forwarding next hops for an underlay group address.
    pub fn set_mcast_fwd(
        &self,
        req: &McastForwardingEntry,
    ) -> Result<(), Error> {
        // Safe to unwrap: 77 is well within the 24-bit VNI range.
        let mcast_vni =
            Vni::new(oxide_vpc::api::DEFAULT_MULTICAST_VNI).unwrap();
        let addr: Ipv6Addr = req.underlay;

        info!(
            self.inner.log,
            "Setting multicast forwarding";
            "underlay" => %addr,
            "next_hops" => req.next_hops.len(),
        );

        let underlay = MulticastUnderlay::new(addr.into())
            .map_err(|_| Error::InvalidMcastUnderlay(addr))?;
        let next_hops = req
            .next_hops
            .iter()
            .map(|nexthop| oxide_vpc::api::McastForwardingNextHop {
                next_hop: oxide_vpc::api::NextHopV6 {
                    addr: nexthop.next_hop.into(),
                    vni: mcast_vni,
                },
                replication: match nexthop.replication {
                    McastReplication::External => {
                        oxide_vpc::api::Replication::External
                    }
                    McastReplication::Underlay => {
                        oxide_vpc::api::Replication::Underlay
                    }
                    McastReplication::Both => oxide_vpc::api::Replication::Both,
                },
                source_filter: match nexthop.filter.mode {
                    McastFilterMode::Include => SourceFilter::Include(
                        nexthop
                            .filter
                            .sources
                            .iter()
                            .copied()
                            .map(Into::into)
                            .collect(),
                    ),
                    McastFilterMode::Exclude => SourceFilter::Exclude(
                        nexthop
                            .filter
                            .sources
                            .iter()
                            .copied()
                            .map(Into::into)
                            .collect(),
                    ),
                },
            })
            .collect();
        let hdl = Handle::new()?;
        hdl.set_mcast_fwd(&SetMcastForwardingReq { underlay, next_hops })?;
        Ok(())
    }

    /// Remove all multicast forwarding entries for an underlay group address.
    pub fn clear_mcast_fwd(
        &self,
        req: &ClearMcastForwarding,
    ) -> Result<(), Error> {
        let addr: Ipv6Addr = req.underlay;

        info!(
            self.inner.log,
            "Clearing multicast forwarding";
            "underlay" => %addr,
        );

        let underlay = MulticastUnderlay::new(addr.into())
            .map_err(|_| Error::InvalidMcastUnderlay(addr))?;
        let hdl = Handle::new()?;
        hdl.clear_mcast_fwd(&ClearMcastForwardingReq { underlay })?;
        Ok(())
    }

    /// Dump all multicast overlay-to-underlay (M2P) mappings from OPTE.
    pub fn list_mcast_m2p(&self) -> Result<Vec<Mcast2PhysMapping>, Error> {
        let hdl = Handle::new()?;
        let resp = hdl.dump_m2p()?;
        let mappings = resp
            .ip4
            .into_iter()
            .map(|(group, underlay)| Mcast2PhysMapping {
                group: IpAddr::V4(group.into()),
                underlay: Ipv6Addr::from(underlay.addr()),
            })
            .chain(resp.ip6.into_iter().map(|(group, underlay)| {
                Mcast2PhysMapping {
                    group: IpAddr::V6(group.into()),
                    underlay: Ipv6Addr::from(underlay.addr()),
                }
            }))
            .collect();
        Ok(mappings)
    }

    /// Dump all multicast forwarding entries from OPTE.
    pub fn list_mcast_fwd(&self) -> Result<Vec<McastForwardingEntry>, Error> {
        let hdl = Handle::new()?;
        let resp = hdl.dump_mcast_fwd()?;
        resp.entries
            .into_iter()
            .map(|entry| {
                let next_hops = entry
                    .next_hops
                    .into_iter()
                    .filter_map(|nexthop| {
                        let replication = match nexthop.replication {
                            oxide_vpc::api::Replication::External => {
                                McastReplication::External
                            }
                            oxide_vpc::api::Replication::Underlay => {
                                McastReplication::Underlay
                            }
                            oxide_vpc::api::Replication::Both => {
                                McastReplication::Both
                            }
                            oxide_vpc::api::Replication::Reserved => {
                                // Reserved is a 2-bit padding value with
                                // no valid semantic meaning. Its presence
                                // in the forwarding table indicates a bug
                                // or manual opteadm intervention. Skip
                                // this hop rather than failing the entire
                                // list so the reconciler can still program
                                // valid next-hops.
                                warn!(
                                    self.inner.log,
                                    "skipping next hop with Reserved \
                                     replication mode";
                                    "next_hop" => %nexthop.next_hop.addr
                                );
                                return None;
                            }
                        };

                        Some(McastForwardingNextHop {
                            next_hop: nexthop.next_hop.addr.into(),
                            replication,
                            filter: McastSourceFilter {
                                mode: match nexthop.source_filter.mode() {
                                    FilterMode::Include => {
                                        McastFilterMode::Include
                                    }
                                    FilterMode::Exclude => {
                                        McastFilterMode::Exclude
                                    }
                                },
                                sources: nexthop
                                    .source_filter
                                    .sources()
                                    .iter()
                                    .copied()
                                    .map(Into::into)
                                    .collect(),
                            },
                        })
                    })
                    .collect();

                Ok(McastForwardingEntry {
                    underlay: Ipv6Addr::from(entry.underlay.addr()),
                    next_hops,
                })
            })
            .collect()
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
        let vpc_ports = ports.iter().filter(|((_, _), port_state)| {
            u32::from(vni) == u32::from(*port_state.port.vni())
        });
        for ((_, _), port_state) in vpc_ports {
            let port = &port_state.port;
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

    pub fn attached_subnets_ensure(
        &self,
        nic_id: Uuid,
        nic_kind: NetworkInterfaceKind,
        ensure_removed: Vec<IpCidr>,
        ensure_added: Vec<AttachedSubnet>,
    ) -> EnsureAttachedSubnetResult {
        let ports = self.inner.ports.lock().unwrap();
        let Some(port_state) = ports.get(&(nic_id, nic_kind)) else {
            return EnsureAttachedSubnetResult {
                diff: Default::default(),
                error: Some(Error::AttachedSubnetUpdateMissingPort(
                    nic_id, nic_kind,
                )),
            };
        };
        self.attached_subnets_ensure_port(
            &port_state.port,
            ensure_removed,
            ensure_added,
        )
    }

    fn attached_subnets_ensure_port(
        &self,
        port: &Port,
        ensure_removed: Vec<IpCidr>,
        ensure_added: Vec<AttachedSubnet>,
    ) -> EnsureAttachedSubnetResult {
        debug!(
            self.inner.log,
            "ensuring attached subnets for port";
            "port_name" => %port.name(),
        );
        let hdl = match Handle::new() {
            Ok(h) => h,
            Err(e) => {
                return EnsureAttachedSubnetResult {
                    diff: Default::default(),
                    error: Some(e.into()),
                };
            }
        };
        let mut result = EnsureAttachedSubnetResult::default();
        for cidr in ensure_removed.into_iter() {
            match hdl.detach_subnet(port.name(), cidr) {
                Ok(_) => result.diff.detached.push(cidr),
                Err(e) => {
                    assert!(result.error.replace(e.into()).is_none());
                    return result;
                }
            }
        }
        for subnet in ensure_added.into_iter() {
            match self.attach_subnet_port(port, subnet) {
                Ok(_) => result.diff.attached.push(subnet),
                Err(e) => {
                    assert!(result.error.replace(e).is_none());
                    return result;
                }
            }
        }
        result
    }

    pub fn attach_subnet(
        &self,
        nic_id: Uuid,
        nic_kind: NetworkInterfaceKind,
        subnet: AttachedSubnet,
    ) -> Result<(), Error> {
        let ports = self.inner.ports.lock().unwrap();
        let port_state = ports.get(&(nic_id, nic_kind)).ok_or_else(|| {
            Error::AttachedSubnetUpdateMissingPort(nic_id, nic_kind)
        })?;
        self.attach_subnet_port(&port_state.port, subnet)
    }

    fn attach_subnet_port(
        &self,
        port: &Port,
        subnet: AttachedSubnet,
    ) -> Result<(), Error> {
        let hdl = Handle::new()?;
        let AttachedSubnet { cidr, kind } = subnet;
        let is_external = match kind {
            AttachedSubnetKind::Vpc => false,
            AttachedSubnetKind::External => true,
        };
        match hdl.attach_subnet(port.name(), cidr, is_external) {
            Ok(_) => {
                debug!(
                    self.inner.log,
                    "attached subnet";
                    "port_name" => %port.name(),
                    "subnet" => %cidr,
                    "kind" => ?kind,
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    self.inner.log,
                    "failed to attach subnet";
                    "port_name" => %port.name(),
                    "subnet" => %cidr,
                    "kind" => ?kind,
                    InlineErrorChain::new(&e),
                );
                Err(Error::from(e))
            }
        }
    }

    pub fn detach_subnet(
        &self,
        nic_id: Uuid,
        nic_kind: NetworkInterfaceKind,
        subnet: IpCidr,
    ) -> Result<(), Error> {
        let ports = self.inner.ports.lock().unwrap();
        let port_state = ports.get(&(nic_id, nic_kind)).ok_or_else(|| {
            Error::AttachedSubnetUpdateMissingPort(nic_id, nic_kind)
        })?;
        self.detach_subnet_port(&port_state.port, subnet)
    }

    fn detach_subnet_port(
        &self,
        port: &Port,
        subnet: IpCidr,
    ) -> Result<(), Error> {
        let hdl = Handle::new()?;
        // This returns an Error if the actual request failed. The
        // `DetachSubnetResp` it returns in the Ok(_) variant is either
        // `NotFound` or `Ok(IpCidr)`, so in both cases we've "detached" it. We
        // return success either way.
        match hdl.detach_subnet(port.name(), subnet) {
            Ok(DetachSubnetResp::Ok(_)) => {
                debug!(
                    self.inner.log,
                    "detached subnet";
                    "port_name" => %port.name(),
                    "subnet" => %subnet,
                );
                Ok(())
            }
            Ok(DetachSubnetResp::NotFound) => {
                warn!(
                    self.inner.log,
                    "subnet is already detached";
                    "port_name" => %port.name(),
                    "subnet" => %subnet,
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    self.inner.log,
                    "failed to detach subnet";
                    "port_name" => %port.name(),
                    "subnet" => %subnet,
                    InlineErrorChain::new(&e),
                );
                Err(Error::from(e))
            }
        }
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

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn kind(&self) -> NetworkInterfaceKind {
        self.kind
    }

    fn release_inner(&mut self) -> Result<(), Error> {
        let mut ports = self.manager.ports.lock().unwrap();
        let Some(port_state) = ports.remove(&(self.id, self.kind)) else {
            error!(
                self.manager.log,
                "Tried to release non-existent port";
                "id" => ?&self.id,
                "kind" => ?&self.kind,
            );
            return Err(Error::ReleaseMissingPort(self.id, self.kind));
        };
        let port = &port_state.port;
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
        drop(routes);
        debug!(
            self.manager.log,
            "Removed OPTE port from manager";
            "id" => ?&self.id,
            "kind" => ?&self.kind,
            "port" => ?&port_state,
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
    use super::PortCreateParams;
    use super::PortManager;
    use super::PortTicket;
    #[cfg(target_os = "illumos")]
    use crate::addrobj::AddrObject;
    use crate::opte::Error;
    use crate::opte::Handle;
    use crate::opte::Port;
    use macaddr::MacAddr6;
    use omicron_common::api::external::{MacAddr, Vni};
    use omicron_common::api::internal::shared::ExternalIpGatewayMap;
    use omicron_common::api::internal::shared::InternetGatewayRouterTarget;
    use omicron_common::api::internal::shared::PrivateIpConfig;
    use omicron_common::api::internal::shared::PrivateIpv4Config;
    use omicron_common::api::internal::shared::PrivateIpv6Config;
    use omicron_common::api::internal::shared::ResolvedVpcRoute;
    use omicron_common::api::internal::shared::ResolvedVpcRouteSet;
    use omicron_common::api::internal::shared::RouterTarget;
    use omicron_common::api::internal::shared::RouterVersion;
    use omicron_test_utils::dev::test_setup_log;
    use oxide_vpc::api::DhcpCfg;
    use oxide_vpc::api::FilterMode;
    use oxide_vpc::api::IpCfg;
    use oxide_vpc::api::Ipv4Cidr;
    use oxide_vpc::api::Ipv6Cidr;
    use oxide_vpc::api::SourceFilter;
    use oxnet::IpNet;
    use oxnet::Ipv4Net;
    use oxnet::Ipv6Net;
    use sled_agent_types::instance::ExternalIpConfig;
    use sled_agent_types::instance::ExternalIpv4Config;
    use sled_agent_types::instance::ExternalIpv6Config;
    use sled_agent_types::inventory::NetworkInterface;
    use sled_agent_types::inventory::NetworkInterfaceKind;
    use sled_agent_types::inventory::SourceNatConfigV4;
    use sled_agent_types::inventory::SourceNatConfigV6;
    use sled_agent_types::multicast::MulticastGroupCfg;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::sync::Mutex;
    use std::sync::MutexGuard;
    use std::sync::Once;
    use std::sync::PoisonError;
    #[cfg(target_os = "illumos")]
    use std::time::Duration;
    #[cfg(target_os = "illumos")]
    use std::time::Instant;
    use uuid::Uuid;

    // Maximum ephemeral port number for source NAT (14-bit range).
    const MAX_PORT: u16 = (1 << 14) - 1;

    /// Initialize the simulated xde underlay exactly once per process and
    /// serialize tests that touch the process-global fake OPTE state.
    ///
    /// The fake `Handle` keeps its state in a process-global, and
    /// `set_xde_underlay` fails on a second call. When testing, tests in
    /// this module share one process, so they route through this helper
    /// instead of calling `set_xde_underlay` directly and hold the
    /// returned guard for the duration of the test.
    #[must_use]
    fn ensure_xde_underlay(handle: &Handle) -> MutexGuard<'static, ()> {
        static UNDERLAY_INIT: Once = Once::new();
        static OPTE_STATE_LOCK: Mutex<()> = Mutex::new(());
        // A test panicking with the guard held poisons the lock, but the
        // global state stays usable for the remaining tests.
        let guard =
            OPTE_STATE_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        UNDERLAY_INIT.call_once(|| {
            handle.set_xde_underlay("underlay0", "underlay1").unwrap();
        });
        guard
    }

    /// Loopback interface name on illumos. Tests that verify kernel
    /// IPv6 multicast membership are illumos-only because they shell
    /// out to illumos's `netstat -g -f inet6`.
    #[cfg(target_os = "illumos")]
    const LOOPBACK_IF: &str = "lo0";

    /// Reports whether `netstat -g -f inet6` lists `group` as a
    /// membership on `interface`.
    ///
    /// Used to verify that `join_multicast_v6`/leave on the filter
    /// socket actually reached the kernel's IP layer for the named
    /// underlay NIC, rather than just updating the in-process
    /// `mcast_underlay_sockets` map.
    #[cfg(target_os = "illumos")]
    fn netstat_v6_has_membership(interface: &str, group: &Ipv6Addr) -> bool {
        let out = std::process::Command::new("netstat")
            .args(["-g", "-n", "-f", "inet6"])
            .output()
            .expect("netstat -g invocation failed");
        let group_str = group.to_string();
        String::from_utf8_lossy(&out.stdout).lines().any(|line| {
            let mut fields = line.split_whitespace();
            if let (Some(iface), Some(grp)) = (fields.next(), fields.next()) {
                iface == interface && grp == group_str
            } else {
                false
            }
        })
    }

    /// Poll `netstat -g` until membership matches `expected`, panicking
    /// on timeout. The kernel should update synchronously on the join
    /// or leave syscall, but polling tolerates any transient delay.
    #[cfg(target_os = "illumos")]
    fn poll_v6_membership(interface: &str, group: &Ipv6Addr, expected: bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if netstat_v6_has_membership(interface, group) == expected {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!(
            "timeout: membership for {group} on {interface} expected {}",
            if expected { "present" } else { "absent" }
        );
    }

    // Regression for https://github.com/oxidecomputer/omicron/issues/7541.
    #[test]
    fn multiple_ports_does_not_destroy_default_route() {
        let logctx =
            test_setup_log("multiple_ports_does_not_destroy_default_route");
        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);
        let default_ipv4_route =
            IpNet::V4(Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).unwrap());

        // Information about our builtin services VPC System Router.
        //
        // This comes from nexus/db-fixed-data/src/vpc.rs. It _should_ stay in
        // sync with that for clarity, but the correctness of this test does not
        // rely on it.
        const SERVICES_INTERNET_GATEWAY_ID: Uuid =
            uuid::uuid!("001de000-074c-4000-8000-000000000002");
        const SERVICES_VPC_VNI: Vni = Vni::SERVICES_VNI;

        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        // First, create a port for a service.
        //
        // At this point, the seeded default routes targeting the System IGW
        // are programmed onto the port itself, while the shared route set
        // for the router key stays empty until Nexus pushes routes.
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
        let public_ipv4_addr0 = Ipv4Addr::new(10, 0, 0, 4);
        let public_ipv4_addr1 = Ipv4Addr::new(10, 0, 0, 5);
        let external_ip_config0 = ExternalIpConfig {
            v4: Some(ExternalIpv4Config {
                source_nat: Some(
                    SourceNatConfigV4::new(public_ipv4_addr0, 0, MAX_PORT)
                        .unwrap(),
                ),
                ..Default::default()
            }),
            v6: None,
        };
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
                external_ips: &external_ip_config0,
                firewall_rules: &[],
                dhcp_config: DhcpCfg {
                    hostname: None,
                    host_domain: None,
                    domain_search_list: Vec::new(),
                    dns4_servers: Vec::new(),
                    dns6_servers: Vec::new(),
                },
                attached_subnets: vec![],
                multicast_groups: &[],
                mtu: None,
            })
            .unwrap();

        // The seeded defaults are a per-port need for services, applied to
        // the port only. The shared route set tracks what Nexus pushes, and
        // nothing has been pushed yet.
        let system_routes = manager
            .inner
            .routes
            .lock()
            .unwrap()
            .get(&port0.system_router_key())
            .unwrap()
            .clone();
        assert!(
            system_routes.routes.is_empty(),
            "The shared System Router route set should start empty, with \
            the seeded service defaults programmed onto the port only"
        );

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
        // and loaded builtin data to the database, it knows the ID of the
        // IGW of the System Router in the builtin services VPC. The pushed
        // default is identical to the seed already programmed onto the
        // service port, so the port is left untouched rather than receiving
        // a duplicate.
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
        // The new port receives the pushed default from the shared set plus
        // its own seeded IPv6 default, with the IPv4 seed skipped because
        // the shared set already holds an identical route. The first port
        // is not modified when this second port is created.
        let external_ip_config1 = ExternalIpConfig {
            v4: Some(ExternalIpv4Config {
                source_nat: Some(
                    SourceNatConfigV4::new(public_ipv4_addr1, 0, MAX_PORT)
                        .unwrap(),
                ),
                ..Default::default()
            }),
            v6: None,
        };
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
                external_ips: &external_ip_config1,
                firewall_rules: &[],
                dhcp_config: DhcpCfg {
                    hostname: None,
                    host_domain: None,
                    domain_search_list: Vec::new(),
                    dns4_servers: Vec::new(),
                    dns6_servers: Vec::new(),
                },
                attached_subnets: vec![],
                multicast_groups: &[],
                mtu: None,
            })
            .unwrap();

        // The shared set still holds only the pushed default. The per-port
        // seeds never enter it, so adding the second port changes nothing
        // here.
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

    // A probe port is created with no routes, like an instance port and
    // unlike a service port. Nexus's VPC route RPW pushes its routes, and
    // its NAT rules are keyed by the EIP-to-IGW mappings pushed alongside
    // them. This holds in both branches of `create_port`: creating the
    // shared route set and joining one that already exists.
    #[test]
    fn probe_port_created_without_seeded_routes() {
        let logctx = test_setup_log("probe_port_created_without_seeded_routes");
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        // Create a probe port. Probes only support an Ephemeral external IP.
        let (probe_port, _probe_ticket) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Probe { id: Uuid::new_v4() },
            Uuid::new_v4(),
            PRIVATE_IP0,
            EIP0,
            0x01,
        );

        // The shared route set is created empty. The RPW fills it.
        let system_routes = manager
            .inner
            .routes
            .lock()
            .unwrap()
            .get(&probe_port.system_router_key())
            .unwrap()
            .clone();
        assert!(
            system_routes.routes.is_empty(),
            "a probe's System Router should start empty"
        );

        {
            let state = handle.state().lock().unwrap();
            let port_data = state.ports.get("opte0").unwrap();
            assert!(
                port_data.routes.is_empty(),
                "a probe port should have no routes at OPTE on creation"
            );
            // External IPs are ensured at creation so that a mapping already
            // known to the port manager applies immediately. None have been
            // pushed here.
            let external_ips = port_data.external_ips.as_ref().unwrap();
            assert!(
                external_ips.inet_gw_map.is_none(),
                "no IGW mappings should be applied before Nexus pushes them"
            );
        }

        // Push a resolved default route so the shared set is non-empty
        // before a second probe joins it.
        let igw_id = Uuid::new_v4();
        let default_ipv4_route =
            IpNet::V4(Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).unwrap());
        manager
            .vpc_routes_ensure(vec![ResolvedVpcRouteSet {
                id: probe_port.system_router_key(),
                version: Some(RouterVersion {
                    router_id: Uuid::new_v4(),
                    version: 1,
                }),
                routes: HashSet::from([ResolvedVpcRoute {
                    dest: default_ipv4_route,
                    target: RouterTarget::InternetGateway(
                        InternetGatewayRouterTarget::Instance(igw_id),
                    ),
                }]),
            }])
            .unwrap();

        // The second probe shares the same VNI and subnet, so it joins the
        // pre-existing route set rather than creating a fresh one.
        let (_probe_port1, _probe_ticket1) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Probe { id: Uuid::new_v4() },
            Uuid::new_v4(),
            PRIVATE_IP1,
            EIP1,
            0x02,
        );

        {
            let state = handle.state().lock().unwrap();
            assert_eq!(state.ports.len(), 2);

            for name in ["opte0", "opte1"] {
                let port_data = state.ports.get(name).unwrap();
                assert_eq!(
                    port_data.routes.len(),
                    1,
                    "{name} should carry only the resolved route"
                );
                assert!(
                    matches!(
                        port_data.routes[0].target,
                        oxide_vpc::api::RouterTarget::InternetGateway(Some(id))
                            if id == igw_id
                    ),
                    "{name} should target the resolved gateway, not a seed"
                );
            }
        }

        logctx.cleanup_successful();
    }

    // A service port created first seeds its IGW defaults onto itself only.
    // A probe port that later shares the same router key must not inherit
    // them. It starts with no routes until the RPW pushes some.
    #[test]
    fn probe_port_after_service_port_gets_no_seeded_routes() {
        let logctx = test_setup_log(
            "probe_port_after_service_port_gets_no_seeded_routes",
        );
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let (_service_port, _service_ticket) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Service { id: Uuid::new_v4() },
            Uuid::new_v4(),
            PRIVATE_IP0,
            EIP0,
            0x01,
        );

        let (probe_port, _probe_ticket) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Probe { id: Uuid::new_v4() },
            Uuid::new_v4(),
            PRIVATE_IP1,
            EIP1,
            0x02,
        );

        let system_routes = manager
            .inner
            .routes
            .lock()
            .unwrap()
            .get(&probe_port.system_router_key())
            .unwrap()
            .clone();
        assert!(
            system_routes.routes.is_empty(),
            "the shared route set should not hold the service seed"
        );

        {
            let state = handle.state().lock().unwrap();
            let service_data = state.ports.get("opte0").unwrap();
            assert!(
                service_data
                    .routes
                    .iter()
                    .any(|route| route.is_system_default_ipv4_route()),
                "the service port should carry its seeded default"
            );
            let probe_data = state.ports.get("opte1").unwrap();
            assert!(
                probe_data.routes.is_empty(),
                "a probe port created after a service port should not \
                 inherit the seeded service defaults"
            );
        }

        logctx.cleanup_successful();
    }

    // Nexus's VPC route RPW resolves a project VPC's default route to that
    // VPC's own gateway UUID and pushes EIP-to-IGW mappings that key probe
    // NAT rules to the same UUID. Outbound NAT rules match the router tag
    // exactly, so the route target and the NAT keying must agree, whether
    // the probe port exists at push time or is created after.
    #[test]
    fn probe_ports_follow_igw_mappings() {
        let logctx = test_setup_log("probe_ports_follow_igw_mappings");
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);
        let default_ipv4_route =
            IpNet::V4(Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).unwrap());

        let igw_id = Uuid::new_v4();
        let nic_id0 = Uuid::new_v4();
        let nic_id1 = Uuid::new_v4();

        let assert_tagged_for_igw = |port_name: &str, eip: Ipv4Addr| {
            let state = handle.state().lock().unwrap();
            let port_data = state.ports.get(port_name).unwrap();

            assert!(
                !port_data
                    .routes
                    .iter()
                    .any(|route| route.is_system_default_ipv4_route()),
                "{port_name} should carry no bare System default route"
            );
            assert_eq!(
                port_data.routes.len(),
                1,
                "{port_name} should carry exactly the RPW-resolved default \
                route"
            );
            assert!(
                matches!(
                    port_data.routes[0].target,
                    oxide_vpc::api::RouterTarget::InternetGateway(Some(id))
                        if id == igw_id
                ),
                "{port_name} default route should target the resolved gateway"
            );

            let external_ips = port_data.external_ips.as_ref().unwrap();
            let gw_map = external_ips.inet_gw_map.as_ref().unwrap();
            let gws = gw_map
                .get(&IpAddr::V4(eip).into())
                .expect("the probe's ephemeral IP should be mapped");
            assert!(
                gws.contains(&igw_id),
                "{port_name} NAT keying should name the same gateway as the \
                router tag"
            );
        };

        let (probe_port, _probe_ticket) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Probe { id: Uuid::new_v4() },
            nic_id0,
            PRIVATE_IP0,
            EIP0,
            0x01,
        );

        // Simulate the RPW's mapping push and the refresh sled-agent runs
        // when the mappings change.
        let mappings =
            igw_mappings(&[(nic_id0, EIP0, igw_id), (nic_id1, EIP1, igw_id)]);
        assert!(
            manager
                .set_eip_gateways(ExternalIpGatewayMap { mappings })
                .is_some()
        );
        manager.external_ips_refresh_probes().unwrap();

        // Simulate the RPW route push. The resolved default route names the
        // project VPC's own gateway rather than the System gateway.
        let new_routes = vec![ResolvedVpcRouteSet {
            id: probe_port.system_router_key(),
            version: Some(RouterVersion {
                router_id: Uuid::new_v4(),
                version: 1,
            }),
            routes: HashSet::from([ResolvedVpcRoute {
                dest: default_ipv4_route,
                target: RouterTarget::InternetGateway(
                    InternetGatewayRouterTarget::Instance(igw_id),
                ),
            }]),
        }];
        manager.vpc_routes_ensure(new_routes).unwrap();

        assert_tagged_for_igw("opte0", EIP0);

        // A probe port created after the pushes receives the shared route
        // set and the already-known mapping at creation.
        //
        // Dropping a PortTicket releases its port, and the assertion below
        // needs the port alive.
        let (_probe_port1, _probe_ticket1) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Probe { id: Uuid::new_v4() },
            nic_id1,
            PRIVATE_IP1,
            EIP1,
            0x02,
        );

        assert_tagged_for_igw("opte1", EIP1);

        logctx.cleanup_successful();
    }

    /// Ephemeral external IPs for ports created via `create_v4_port`.
    const EIP0: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 4);
    const EIP1: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 5);
    const EIP2: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 6);

    /// Private IPs within the fixture subnet 172.30.0.0/24.
    const PRIVATE_IP0: Ipv4Addr = Ipv4Addr::new(172, 30, 0, 4);
    const PRIVATE_IP1: Ipv4Addr = Ipv4Addr::new(172, 30, 0, 5);
    const PRIVATE_IP2: Ipv4Addr = Ipv4Addr::new(172, 30, 0, 6);

    /// Create a v4-only port with an ephemeral external IP.
    ///
    /// Port names are assigned by the manager in creation order (opte0,
    /// opte1, ...), independent of the NIC name.
    fn create_v4_port(
        manager: &PortManager,
        kind: NetworkInterfaceKind,
        nic_id: Uuid,
        private_ip: Ipv4Addr,
        ephemeral_ip: Ipv4Addr,
        mac_last_octet: u8,
    ) -> (Port, PortTicket) {
        let private_subnet =
            Ipv4Net::new(Ipv4Addr::new(172, 30, 0, 0), 24).unwrap();
        let ip_config =
            PrivateIpConfig::new_ipv4(private_ip, private_subnet).unwrap();
        let external_ips = ExternalIpConfig {
            v4: Some(ExternalIpv4Config {
                ephemeral_ip: Some(ephemeral_ip),
                ..Default::default()
            }),
            v6: None,
        };
        manager
            .create_port(PortCreateParams {
                nic: &NetworkInterface {
                    id: nic_id,
                    kind,
                    name: "net0".parse().unwrap(),
                    ip_config,
                    mac: MacAddr(MacAddr6::new(
                        0xa8,
                        0x40,
                        0x25,
                        0x00,
                        0x00,
                        mac_last_octet,
                    )),
                    vni: 100.try_into().unwrap(),
                    primary: true,
                    slot: 0,
                },
                external_ips: &external_ips,
                firewall_rules: &[],
                dhcp_config: DhcpCfg {
                    hostname: None,
                    host_domain: None,
                    domain_search_list: Vec::new(),
                    dns4_servers: Vec::new(),
                    dns6_servers: Vec::new(),
                },
                attached_subnets: vec![],
                multicast_groups: &[],
                mtu: None,
            })
            .unwrap()
    }

    /// Build a nested EIP-to-IGW mapping table from (nic, eip, igw) entries.
    fn igw_mappings(
        entries: &[(Uuid, Ipv4Addr, Uuid)],
    ) -> HashMap<Uuid, HashMap<IpAddr, HashSet<Uuid>>> {
        let mut mappings: HashMap<Uuid, HashMap<IpAddr, HashSet<Uuid>>> =
            HashMap::new();
        for (nic_id, eip, igw_id) in entries {
            mappings
                .entry(*nic_id)
                .or_default()
                .entry(IpAddr::V4(*eip))
                .or_default()
                .insert(*igw_id);
        }
        mappings
    }

    // `external_ips_refresh_probes` re-ensures probe ports only. Instance
    // ports are refreshed through the instance manager, and service ports
    // never have external IPs ensured, so neither should be touched even
    // when mappings exist for their NICs.
    #[test]
    fn external_ips_refresh_probes_skips_non_probe_ports() {
        let logctx =
            test_setup_log("external_ips_refresh_probes_skips_non_probe_ports");
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let igw_id = Uuid::new_v4();
        let instance_nic = Uuid::new_v4();
        let service_nic = Uuid::new_v4();
        let probe_nic = Uuid::new_v4();

        let (_instance_port, _instance_ticket) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Instance { id: Uuid::new_v4() },
            instance_nic,
            PRIVATE_IP0,
            EIP0,
            0x01,
        );
        let (_service_port, _service_ticket) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Service { id: Uuid::new_v4() },
            service_nic,
            PRIVATE_IP1,
            EIP1,
            0x02,
        );
        let (_probe_port, _probe_ticket) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Probe { id: Uuid::new_v4() },
            probe_nic,
            PRIVATE_IP2,
            EIP2,
            0x03,
        );

        // Mappings cover all three NICs, so an errant non-probe refresh
        // would show up as a populated gateway map below.
        let mappings = igw_mappings(&[
            (instance_nic, EIP0, igw_id),
            (service_nic, EIP1, igw_id),
            (probe_nic, EIP2, igw_id),
        ]);

        assert!(
            manager
                .set_eip_gateways(ExternalIpGatewayMap { mappings })
                .is_some()
        );
        manager.external_ips_refresh_probes().unwrap();

        {
            let state = handle.state().lock().unwrap();
            assert_eq!(state.ports.len(), 3);

            // The instance port was ensured at creation, before the
            // mappings arrived, and the refresh must not have re-ensured
            // it since.
            let instance_data = state.ports.get("opte0").unwrap();
            assert!(
                instance_data
                    .external_ips
                    .as_ref()
                    .unwrap()
                    .inet_gw_map
                    .is_none(),
                "instance port should not be re-ensured by the probe refresh"
            );

            // Service ports never have external IPs ensured at all.
            let service_data = state.ports.get("opte1").unwrap();
            assert!(
                service_data.external_ips.is_none(),
                "service port should not be re-ensured by the probe refresh"
            );

            // The probe port picks up the new mapping.
            let probe_data = state.ports.get("opte2").unwrap();
            let gw_map = probe_data
                .external_ips
                .as_ref()
                .unwrap()
                .inet_gw_map
                .as_ref()
                .unwrap();
            let gws = gw_map
                .get(&IpAddr::V4(EIP2).into())
                .expect("the probe's ephemeral IP should be mapped");
            assert!(gws.contains(&igw_id));
        }

        logctx.cleanup_successful();
    }

    // A failure ensuring one probe port must not prevent refreshing the
    // rest. Removing a port from the simulated OPTE state makes its
    // `set_external_ips` fail with `NoPort` while the other succeeds.
    #[test]
    fn external_ips_refresh_probes_attempts_all_ports() {
        let logctx =
            test_setup_log("external_ips_refresh_probes_attempts_all_ports");
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let igw_id = Uuid::new_v4();
        let nic_id0 = Uuid::new_v4();
        let nic_id1 = Uuid::new_v4();

        let (_probe_port0, _probe_ticket0) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Probe { id: Uuid::new_v4() },
            nic_id0,
            PRIVATE_IP0,
            EIP0,
            0x01,
        );

        let (_probe_port1, _probe_ticket1) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Probe { id: Uuid::new_v4() },
            nic_id1,
            PRIVATE_IP1,
            EIP1,
            0x02,
        );

        let mappings =
            igw_mappings(&[(nic_id0, EIP0, igw_id), (nic_id1, EIP1, igw_id)]);
        assert!(
            manager
                .set_eip_gateways(ExternalIpGatewayMap { mappings })
                .is_some()
        );

        // Simulate a per-port OPTE failure by removing the first port
        // from the simulated kernel state. The port ticket's later
        // cleanup tolerates the missing port.
        {
            let mut state = handle.state().lock().unwrap();
            state
                .ports
                .remove("opte0")
                .expect("the first probe port should exist before removal");
        }

        let result = manager.external_ips_refresh_probes();
        assert!(result.is_err(), "refresh should report the failed port");

        // The surviving probe must have been refreshed despite the other
        // port's failure, regardless of iteration order.
        {
            let state = handle.state().lock().unwrap();
            assert_eq!(state.ports.len(), 1);
            let port_data = state.ports.get("opte1").unwrap();
            let gw_map = port_data
                .external_ips
                .as_ref()
                .unwrap()
                .inet_gw_map
                .as_ref()
                .unwrap();
            let gws = gw_map
                .get(&IpAddr::V4(EIP1).into())
                .expect("the surviving probe's ephemeral IP should be mapped");
            assert!(gws.contains(&igw_id));
        }

        logctx.cleanup_successful();
    }

    // An unacknowledged mapping push stays pending, so an identical
    // re-push after a failed refresh retries instead of stranding ports
    // on stale IGW keying. Acknowledging a stale generation number must not
    // clear a newer pending change.
    #[test]
    fn set_eip_gateways_tracks_pending_refresh() {
        let logctx = test_setup_log("set_eip_gateways_tracks_pending_refresh");
        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let igw_id = Uuid::new_v4();
        let nic_id = Uuid::new_v4();
        let mappings = igw_mappings(&[(nic_id, EIP0, igw_id)]);

        // The first push changes the mappings and leaves a refresh pending.
        let first = manager
            .set_eip_gateways(ExternalIpGatewayMap {
                mappings: mappings.clone(),
            })
            .expect("first push should leave a refresh pending");

        // An identical re-push before acknowledgment reports the same
        // pending refresh.
        assert_eq!(
            manager.set_eip_gateways(ExternalIpGatewayMap {
                mappings: mappings.clone(),
            }),
            Some(first),
        );

        // Once acknowledged, an identical re-push reports nothing pending.
        manager.eip_gateways_refreshed(first);
        assert_eq!(
            manager.set_eip_gateways(ExternalIpGatewayMap {
                mappings: mappings.clone(),
            }),
            None,
        );

        // A changed push advances to a new generation, and acknowledging
        // the stale one leaves the refresh pending.
        let mut updated = mappings;
        updated
            .get_mut(&nic_id)
            .unwrap()
            .get_mut(&IpAddr::V4(EIP0))
            .unwrap()
            .insert(Uuid::new_v4());

        let second = manager
            .set_eip_gateways(ExternalIpGatewayMap {
                mappings: updated.clone(),
            })
            .expect("changed push should leave a refresh pending");

        assert!(second > first);
        manager.eip_gateways_refreshed(first);
        assert_eq!(
            manager
                .set_eip_gateways(ExternalIpGatewayMap { mappings: updated }),
            Some(second),
        );

        logctx.cleanup_successful();
    }

    // A route set replacement must not delete the seeded System default
    // routes on a service port. The keep_seed check in `vpc_routes_ensure`
    // skips them during the deletion pass.
    #[test]
    fn service_port_seed_survives_route_set_replacement() {
        let logctx =
            test_setup_log("service_port_seed_survives_route_set_replacement");
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let igw_id = Uuid::new_v4();
        let (port, _ticket) = create_v4_port(
            &manager,
            NetworkInterfaceKind::Service { id: Uuid::new_v4() },
            Uuid::new_v4(),
            PRIVATE_IP0,
            EIP0,
            0x01,
        );

        {
            let state = handle.state().lock().unwrap();
            let port_data = state.ports.get("opte0").unwrap();
            assert!(
                port_data
                    .routes
                    .iter()
                    .any(|route| route.is_system_default_ipv4_route()),
                "service port should carry the seeded System default route"
            );
        }

        // Replace the route set with one naming only a non-default route.
        // The seeded defaults are absent from the new set but must survive
        // the deletion pass.
        let pushed_dest =
            IpNet::V4(Ipv4Net::new(Ipv4Addr::new(10, 1, 0, 0), 16).unwrap());
        manager
            .vpc_routes_ensure(vec![ResolvedVpcRouteSet {
                id: port.system_router_key(),
                version: Some(RouterVersion {
                    router_id: Uuid::new_v4(),
                    version: 1,
                }),
                routes: HashSet::from([ResolvedVpcRoute {
                    dest: pushed_dest,
                    target: RouterTarget::InternetGateway(
                        InternetGatewayRouterTarget::Instance(igw_id),
                    ),
                }]),
            }])
            .unwrap();

        {
            let state = handle.state().lock().unwrap();
            let port_data = state.ports.get("opte0").unwrap();

            assert!(
                port_data
                    .routes
                    .iter()
                    .any(|route| route.is_system_default_ipv4_route()),
                "seeded System default should survive the replacement"
            );
            assert!(
                port_data.routes.iter().any(|route| matches!(
                    route.target,
                    oxide_vpc::api::RouterTarget::InternetGateway(Some(id))
                        if id == igw_id
                )),
                "the pushed route should be programmed"
            );
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn ip_cfg_from_ipv4_params() {
        let priv_ip = Ipv4Addr::new(172, 30, 2, 5);
        let priv_subnet =
            Ipv4Net::new(Ipv4Addr::new(172, 30, 2, 0), 24).unwrap();
        let ip_config =
            PrivateIpConfig::new_ipv4(priv_ip, priv_subnet).unwrap();
        let mac = "a8:40:25:ff:ff:ff".parse().unwrap();
        let ext_ip = Ipv4Addr::new(10, 151, 2, 169);
        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Instance { id: Uuid::new_v4() },
            name: "opte0".parse().unwrap(),
            ip_config,
            mac,
            vni: 100.try_into().unwrap(),
            primary: true,
            slot: 0,
        };
        let source_nat = SourceNatConfigV4::new(ext_ip, 0, 16383).unwrap();
        let external_ips = ExternalIpConfig {
            v4: Some(ExternalIpv4Config {
                source_nat: Some(source_nat),
                ..Default::default()
            }),
            v6: None,
        };
        let prs = PortCreateParams {
            nic: &nic,
            external_ips: &external_ips,
            firewall_rules: &[],
            dhcp_config: DhcpCfg {
                hostname: None,
                host_domain: None,
                domain_search_list: vec![],
                dns4_servers: vec![],
                dns6_servers: vec![],
            },
            attached_subnets: vec![],
            multicast_groups: &[],
            mtu: None,
        };
        let IpCfg::Ipv4(oxide_vpc::api::Ipv4Cfg {
            vpc_subnet,
            private_ip,
            gateway_ip,
            external_ips:
                oxide_vpc::api::ExternalIpCfg { snat, ephemeral_ip, floating_ips },
            attached_subnets,
            transit_ips,
        }) = IpCfg::try_from(&prs).unwrap()
        else {
            panic!("Expected IPv4 config")
        };

        assert_eq!(private_ip, priv_ip.into());
        assert_eq!(
            vpc_subnet,
            Ipv4Cidr::new(
                priv_subnet.network().unwrap().into(),
                priv_subnet.width().try_into().unwrap()
            )
        );
        assert_eq!(gateway_ip, priv_subnet.first_host().into());
        let oxide_vpc::api::SNatCfg { external_ip, ports } =
            snat.expect("SNAT config for this port should be Some(_)");
        assert_eq!(external_ip, ext_ip.into());
        assert_eq!(ports, source_nat.port_range());
        assert!(ephemeral_ip.is_none());
        assert!(floating_ips.is_empty());
        assert!(attached_subnets.is_empty());
        assert!(transit_ips.is_empty());
    }

    #[test]
    fn ip_cfg_from_ipv6_params() {
        let priv_ip = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 5);
        let priv_subnet =
            Ipv6Net::new(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0), 64)
                .unwrap();
        let ip_config =
            PrivateIpConfig::new_ipv6(priv_ip, priv_subnet).unwrap();
        let mac = "a8:40:25:ff:ff:ff".parse().unwrap();
        let ext_ip = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1);
        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Instance { id: Uuid::new_v4() },
            name: "opte0".parse().unwrap(),
            ip_config,
            mac,
            vni: 100.try_into().unwrap(),
            primary: true,
            slot: 0,
        };
        let source_nat = SourceNatConfigV6::new(ext_ip, 0, 16383).unwrap();
        let external_ips = ExternalIpConfig {
            v4: None,
            v6: Some(ExternalIpv6Config {
                source_nat: Some(source_nat),
                ..Default::default()
            }),
        };
        let prs = PortCreateParams {
            nic: &nic,
            external_ips: &external_ips,
            firewall_rules: &[],
            dhcp_config: DhcpCfg {
                hostname: None,
                host_domain: None,
                domain_search_list: vec![],
                dns4_servers: vec![],
                dns6_servers: vec![],
            },
            attached_subnets: vec![],
            multicast_groups: &[],
            mtu: None,
        };
        let IpCfg::Ipv6(oxide_vpc::api::Ipv6Cfg {
            vpc_subnet,
            private_ip,
            gateway_ip,
            external_ips:
                oxide_vpc::api::ExternalIpCfg { snat, ephemeral_ip, floating_ips },
            attached_subnets: _,
            transit_ips: _,
        }) = IpCfg::try_from(&prs).unwrap()
        else {
            panic!("Expected IPv4 config")
        };

        assert_eq!(private_ip, priv_ip.into());
        assert_eq!(
            vpc_subnet,
            Ipv6Cidr::new(
                priv_subnet.first_addr().into(),
                priv_subnet.width().try_into().unwrap()
            )
        );
        assert_eq!(gateway_ip, priv_subnet.iter().nth(1).unwrap().into());
        let oxide_vpc::api::SNatCfg { external_ip, ports } =
            snat.expect("SNAT config for this port should be Some(_)");
        assert_eq!(external_ip, ext_ip.into());
        assert_eq!(ports, source_nat.port_range());
        assert!(ephemeral_ip.is_none());
        assert!(floating_ips.is_empty());
    }

    #[test]
    fn ip_cfg_from_dual_stack_params() {
        let priv_ipv4 = Ipv4Addr::new(172, 30, 2, 5);
        let priv_ipv4_subnet =
            Ipv4Net::new(Ipv4Addr::new(172, 30, 2, 0), 24).unwrap();
        let ipv4_config =
            PrivateIpv4Config::new(priv_ipv4, priv_ipv4_subnet).unwrap();
        let mac = "a8:40:25:ff:ff:ff".parse().unwrap();
        let ext_ipv4 = Ipv4Addr::new(10, 151, 2, 169);
        let priv_ipv6 = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 5);
        let priv_ipv6_subnet =
            Ipv6Net::new(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0), 64)
                .unwrap();
        let ipv6_config =
            PrivateIpv6Config::new(priv_ipv6, priv_ipv6_subnet).unwrap();
        let ext_ipv6 = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1);
        let ip_config =
            PrivateIpConfig::DualStack { v4: ipv4_config, v6: ipv6_config };
        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Instance { id: Uuid::new_v4() },
            name: "opte0".parse().unwrap(),
            ip_config,
            mac,
            vni: 100.try_into().unwrap(),
            primary: true,
            slot: 0,
        };

        // Ipv4 source NAT, Ipv6 ephemeral
        let source_nat = SourceNatConfigV4::new(ext_ipv4, 0, 16383).unwrap();
        let external_ips = ExternalIpConfig {
            v4: Some(ExternalIpv4Config {
                source_nat: Some(source_nat),
                ..Default::default()
            }),
            v6: Some(ExternalIpv6Config {
                ephemeral_ip: Some(ext_ipv6),
                ..Default::default()
            }),
        };
        let prs = PortCreateParams {
            nic: &nic,
            external_ips: &external_ips,
            firewall_rules: &[],
            dhcp_config: DhcpCfg {
                hostname: None,
                host_domain: None,
                domain_search_list: vec![],
                dns4_servers: vec![],
                dns6_servers: vec![],
            },
            attached_subnets: vec![],
            multicast_groups: &[],
            mtu: None,
        };
        let IpCfg::DualStack { ipv4, ipv6 } = IpCfg::try_from(&prs).unwrap()
        else {
            panic!("Expected DualStack config")
        };

        assert_eq!(ipv4.private_ip, priv_ipv4.into());
        assert_eq!(
            ipv4.vpc_subnet,
            Ipv4Cidr::new(
                priv_ipv4_subnet.network().unwrap().into(),
                priv_ipv4_subnet.width().try_into().unwrap()
            )
        );
        assert_eq!(ipv4.gateway_ip, priv_ipv4_subnet.first_host().into());
        let oxide_vpc::api::SNatCfg { external_ip, ports } = ipv4
            .external_ips
            .snat
            .expect("SNAT config for this port should be Some(_)");
        assert_eq!(external_ip, ext_ipv4.into());
        assert_eq!(ports, source_nat.port_range());
        assert!(ipv4.external_ips.ephemeral_ip.is_none());
        assert!(ipv4.external_ips.floating_ips.is_empty());

        assert_eq!(ipv6.private_ip, priv_ipv6.into());
        assert_eq!(
            ipv6.vpc_subnet,
            Ipv6Cidr::new(
                priv_ipv6_subnet.first_addr().into(),
                priv_ipv6_subnet.width().try_into().unwrap()
            )
        );
        assert_eq!(
            ipv6.gateway_ip,
            priv_ipv6_subnet.iter().nth(1).unwrap().into()
        );
        assert!(
            ipv6.external_ips.snat.is_none(),
            "Should not have SNAT config for the IPv6 stack"
        );
        assert_eq!(
            ipv6.external_ips
                .ephemeral_ip
                .expect("Should have IPv6 ephemeral address"),
            ext_ipv6.into(),
        );
        assert!(ipv6.external_ips.floating_ips.is_empty());
    }

    #[test]
    fn ip_cfg_from_port_params_fails_with_private_ipv4_and_public_ipv6() {
        let priv_ip = Ipv4Addr::new(172, 30, 2, 5);
        let priv_subnet =
            Ipv4Net::new(Ipv4Addr::new(172, 30, 2, 0), 24).unwrap();
        let ip_config =
            PrivateIpConfig::new_ipv4(priv_ip, priv_subnet).unwrap();
        let mac = "a8:40:25:ff:ff:ff".parse().unwrap();
        let ext_ip = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1);
        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Instance { id: Uuid::new_v4() },
            name: "opte0".parse().unwrap(),
            ip_config,
            mac,
            vni: 100.try_into().unwrap(),
            primary: true,
            slot: 0,
        };
        let source_nat = SourceNatConfigV6::new(ext_ip, 0, 16383).unwrap();
        let external_ips = ExternalIpConfig {
            v4: None,
            v6: Some(ExternalIpv6Config {
                source_nat: Some(source_nat),
                ..Default::default()
            }),
        };
        let prs = PortCreateParams {
            nic: &nic,
            external_ips: &external_ips,
            firewall_rules: &[],
            dhcp_config: DhcpCfg {
                hostname: None,
                host_domain: None,
                domain_search_list: vec![],
                dns4_servers: vec![],
                dns6_servers: vec![],
            },
            attached_subnets: vec![],
            multicast_groups: &[],
            mtu: None,
        };
        let _ = IpCfg::try_from(&prs).expect_err(
            "Should fail to convert with public IPv6 and private IPv4",
        );
    }

    #[test]
    fn ip_cfg_from_port_params_fails_with_private_ipv6_and_public_ipv4() {
        let priv_ip = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 5);
        let priv_subnet =
            Ipv6Net::new(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0), 64)
                .unwrap();
        let ip_config =
            PrivateIpConfig::new_ipv6(priv_ip, priv_subnet).unwrap();
        let mac = "a8:40:25:ff:ff:ff".parse().unwrap();
        let ext_ip = Ipv4Addr::new(1, 1, 1, 1);
        let nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Instance { id: Uuid::new_v4() },
            name: "opte0".parse().unwrap(),
            ip_config,
            mac,
            vni: 100.try_into().unwrap(),
            primary: true,
            slot: 0,
        };
        let source_nat = SourceNatConfigV4::new(ext_ip, 0, 16383).unwrap();
        let external_ips = ExternalIpConfig {
            v4: Some(ExternalIpv4Config {
                source_nat: Some(source_nat),
                ..Default::default()
            }),
            v6: None,
        };
        let prs = PortCreateParams {
            nic: &nic,
            external_ips: &external_ips,
            firewall_rules: &[],
            dhcp_config: DhcpCfg {
                hostname: None,
                host_domain: None,
                domain_search_list: vec![],
                dns4_servers: vec![],
                dns6_servers: vec![],
            },
            attached_subnets: vec![],
            multicast_groups: &[],
            mtu: None,
        };
        let _ = IpCfg::try_from(&prs).expect_err(
            "Should fail to convert with public IPv4 and private IPv6",
        );
    }

    #[test]
    fn multicast_groups_ensure_diffing() {
        let logctx = test_setup_log("multicast_groups_ensure_diffing");
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let nic_id = Uuid::new_v4();
        let nic_kind = NetworkInterfaceKind::Service { id: Uuid::new_v4() };

        let private_subnet =
            Ipv4Net::new(Ipv4Addr::new(172, 20, 0, 0), 24).unwrap();
        let private_ip = Ipv4Addr::new(172, 20, 0, 4);
        let ip_config =
            PrivateIpConfig::new_ipv4(private_ip, private_subnet).unwrap();
        let public_ip = Ipv4Addr::new(10, 0, 0, 4);

        let external_ips = ExternalIpConfig {
            v4: Some(ExternalIpv4Config {
                source_nat: Some(
                    SourceNatConfigV4::new(public_ip, 0, MAX_PORT).unwrap(),
                ),
                ..Default::default()
            }),
            v6: None,
        };

        // Bindings keep the port registered in the manager for this scope.
        let (_port, _ticket) = manager
            .create_port(PortCreateParams {
                nic: &NetworkInterface {
                    id: nic_id,
                    kind: nic_kind,
                    name: "opte0".parse().unwrap(),
                    ip_config,
                    mac: MacAddr(MacAddr6::new(
                        0xa8, 0x40, 0x25, 0x00, 0x00, 0x01,
                    )),
                    vni: Vni::SERVICES_VNI,
                    primary: true,
                    slot: 0,
                },
                external_ips: &external_ips,
                firewall_rules: &[],
                dhcp_config: DhcpCfg {
                    hostname: None,
                    host_domain: None,
                    domain_search_list: Vec::new(),
                    dns4_servers: Vec::new(),
                    dns6_servers: Vec::new(),
                },
                attached_subnets: vec![],
                multicast_groups: &[],
                mtu: None,
            })
            .unwrap();

        let group1: IpAddr = "239.1.1.1".parse().unwrap();
        let group2: IpAddr = "239.1.1.2".parse().unwrap();
        let source_a: IpAddr = "10.0.0.1".parse().unwrap();

        // Subscribe to two groups: one ASM, one SSM.
        manager
            .multicast_groups_ensure(
                nic_id,
                nic_kind,
                &[
                    MulticastGroupCfg { group_ip: group1, sources: vec![] },
                    MulticastGroupCfg {
                        group_ip: group2,
                        sources: vec![source_a],
                    },
                ],
            )
            .unwrap();

        // Verify port manager tracking.
        {
            let ports = manager.inner.ports.lock().unwrap();
            let port_state = ports.get(&(nic_id, nic_kind)).unwrap();
            assert_eq!(port_state.mcast_subscriptions.len(), 2);
            assert_eq!(
                *port_state.mcast_subscriptions.get(&group1).unwrap(),
                SourceFilter::default(),
            );
            assert_eq!(
                port_state.mcast_subscriptions.get(&group2).unwrap().mode(),
                FilterMode::Include,
            );
        }

        // Verify mock OPTE state matches.
        {
            let opte = handle.state().lock().unwrap();
            let port = opte.ports.get("opte0").unwrap();
            assert_eq!(port.mcast_subscriptions.len(), 2);
            assert!(port.mcast_subscriptions.contains_key(&group1));
            assert!(port.mcast_subscriptions.contains_key(&group2));
        }

        // Remove group2, keep group1.
        manager
            .multicast_groups_ensure(
                nic_id,
                nic_kind,
                &[MulticastGroupCfg { group_ip: group1, sources: vec![] }],
            )
            .unwrap();

        {
            let ports = manager.inner.ports.lock().unwrap();
            let port_state = ports.get(&(nic_id, nic_kind)).unwrap();
            assert_eq!(port_state.mcast_subscriptions.len(), 1);
            assert!(port_state.mcast_subscriptions.contains_key(&group1));
            assert!(!port_state.mcast_subscriptions.contains_key(&group2));
        }

        {
            let opte = handle.state().lock().unwrap();
            let port = opte.ports.get("opte0").unwrap();
            assert_eq!(port.mcast_subscriptions.len(), 1);
            assert!(!port.mcast_subscriptions.contains_key(&group2));
        }

        // Remove all groups.
        manager.multicast_groups_ensure(nic_id, nic_kind, &[]).unwrap();

        {
            let ports = manager.inner.ports.lock().unwrap();
            let port_state = ports.get(&(nic_id, nic_kind)).unwrap();
            assert!(port_state.mcast_subscriptions.is_empty());
        }

        {
            let opte = handle.state().lock().unwrap();
            let port = opte.ports.get("opte0").unwrap();
            assert!(port.mcast_subscriptions.is_empty());
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn multicast_port_deletion_cleanup() {
        let logctx = test_setup_log("multicast_port_deletion_cleanup");
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let nic_id = Uuid::new_v4();
        let nic_kind = NetworkInterfaceKind::Service { id: Uuid::new_v4() };

        let private_subnet =
            Ipv4Net::new(Ipv4Addr::new(172, 20, 0, 0), 24).unwrap();
        let private_ip = Ipv4Addr::new(172, 20, 0, 4);
        let ip_config =
            PrivateIpConfig::new_ipv4(private_ip, private_subnet).unwrap();
        let public_ip = Ipv4Addr::new(10, 0, 0, 4);

        let external_ips = ExternalIpConfig {
            v4: Some(ExternalIpv4Config {
                source_nat: Some(
                    SourceNatConfigV4::new(public_ip, 0, MAX_PORT).unwrap(),
                ),
                ..Default::default()
            }),
            v6: None,
        };

        let (_port, ticket) = manager
            .create_port(PortCreateParams {
                nic: &NetworkInterface {
                    id: nic_id,
                    kind: nic_kind,
                    name: "opte0".parse().unwrap(),
                    ip_config,
                    mac: MacAddr(MacAddr6::new(
                        0xa8, 0x40, 0x25, 0x00, 0x00, 0x01,
                    )),
                    vni: Vni::SERVICES_VNI,
                    primary: true,
                    slot: 0,
                },
                external_ips: &external_ips,
                firewall_rules: &[],
                dhcp_config: DhcpCfg {
                    hostname: None,
                    host_domain: None,
                    domain_search_list: Vec::new(),
                    dns4_servers: Vec::new(),
                    dns6_servers: Vec::new(),
                },
                attached_subnets: vec![],
                multicast_groups: &[],
                mtu: None,
            })
            .unwrap();

        let group1: IpAddr = "239.2.2.1".parse().unwrap();

        manager
            .multicast_groups_ensure(
                nic_id,
                nic_kind,
                &[MulticastGroupCfg { group_ip: group1, sources: vec![] }],
            )
            .unwrap();

        {
            let ports = manager.inner.ports.lock().unwrap();
            let port_state = ports.get(&(nic_id, nic_kind)).unwrap();
            assert_eq!(
                port_state.mcast_subscriptions.len(),
                1,
                "subscription tracking should exist before release"
            );
        }

        // Release the port ticket, which should clean up the port
        // and its subscription tracking.
        ticket.release();

        {
            let ports = manager.inner.ports.lock().unwrap();
            assert!(
                !ports.contains_key(&(nic_id, nic_kind)),
                "port should be removed after release"
            );
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn multicast_ensure_missing_port_error() {
        let logctx = test_setup_log("multicast_ensure_missing_port_error");
        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let nic_id = Uuid::new_v4();
        let nic_kind = NetworkInterfaceKind::Instance { id: Uuid::new_v4() };
        let group: IpAddr = "239.3.3.1".parse().unwrap();

        let res = manager.multicast_groups_ensure(
            nic_id,
            nic_kind,
            &[MulticastGroupCfg { group_ip: group, sources: vec![] }],
        );

        match res {
            Err(Error::MulticastUpdateMissingPort(id, kind)) => {
                assert_eq!(id, nic_id);
                assert_eq!(kind, nic_kind);
            }
            other => {
                panic!("expected MulticastUpdateMissingPort, got {other:?}")
            }
        }

        logctx.cleanup_successful();
    }

    /// Verify that `set_mcast_m2p` installs the multicast MAC filter on
    /// each underlay NIC via UDP socket join and that `clear_mcast_m2p`
    /// removes them.
    ///
    /// Asserts both the in-process `mcast_underlay_sockets` bookkeeping
    /// and kernel-level IPv6 group membership on the underlay interface
    /// (observable via `netstat -g -f inet6`). Kernel-level verification
    /// is what ensures `join_multicast_v6` actually reached IP and, on
    /// actual hardware, would drive `mac_multicast_add` to program the
    /// NIC filter.
    #[cfg(target_os = "illumos")]
    #[test]
    fn underlay_multicast_mac_filter_lifecycle() {
        let logctx = test_setup_log("underlay_multicast_mac_filter_lifecycle");
        let nics = vec![AddrObject::new_control(LOOPBACK_IF).unwrap()];
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &nics);

        // ff04::1 is within the underlay multicast subnet.
        let underlay: Ipv6Addr = "ff04::1".parse().unwrap();
        let group: IpAddr = "239.10.10.1".parse().unwrap();

        let req =
            sled_agent_types::multicast::Mcast2PhysMapping { group, underlay };

        // We perform an Up-front check, confirming that the group must not
        // already be joined on the underlay interface.
        assert!(
            !netstat_v6_has_membership(LOOPBACK_IF, &underlay),
            "unexpected pre-existing membership {underlay} on {LOOPBACK_IF}",
        );

        // Set M2P -> socket should be created and kernel should show join.
        manager.set_mcast_m2p(&req).unwrap();
        {
            let sockets =
                manager.inner.mcast_underlay_sockets.sockets.lock().unwrap();
            assert!(
                sockets.contains_key(&underlay),
                "Socket should exist after set_mcast_m2p"
            );
        }
        poll_v6_membership(LOOPBACK_IF, &underlay, true);

        // Setting the same M2P again should be idempotent.
        manager.set_mcast_m2p(&req).unwrap();
        {
            let sockets =
                manager.inner.mcast_underlay_sockets.sockets.lock().unwrap();
            assert_eq!(
                sockets.len(),
                1,
                "Duplicate set_mcast_m2p should not create extra sockets"
            );
        }
        assert!(
            netstat_v6_has_membership(LOOPBACK_IF, &underlay),
            "membership should still be present after idempotent re-set"
        );

        // Clear M2P -> socket should be removed and kernel membership gone.
        let clear_req =
            sled_agent_types::multicast::ClearMcast2Phys { group, underlay };
        manager.clear_mcast_m2p(&clear_req).unwrap();
        {
            let sockets =
                manager.inner.mcast_underlay_sockets.sockets.lock().unwrap();
            assert!(
                !sockets.contains_key(&underlay),
                "Socket should be removed after clear_mcast_m2p"
            );
        }
        poll_v6_membership(LOOPBACK_IF, &underlay, false);

        logctx.cleanup_successful();
    }

    /// Verify that rehydration at startup reopens filter sockets for
    /// M2P mappings that survived in mock xde state across a
    /// PortManager drop (which simulates sled-agent restart).
    #[cfg(target_os = "illumos")]
    #[test]
    fn underlay_multicast_mac_filter_rehydration() {
        let logctx =
            test_setup_log("underlay_multicast_mac_filter_rehydration");
        let nics = vec![AddrObject::new_control(LOOPBACK_IF).unwrap()];

        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        // Use a distinct underlay address to avoid collisions with
        // other tests sharing the static OPTE_STATE.
        let underlay: Ipv6Addr = "ff04::99".parse().unwrap();
        let group: IpAddr = "239.10.10.99".parse().unwrap();

        let req =
            sled_agent_types::multicast::Mcast2PhysMapping { group, underlay };

        // First phase: first PortManager sets M2P (populates mock xde state).
        {
            let mgr1 = PortManager::new(
                logctx.log.clone(),
                Ipv6Addr::LOCALHOST,
                &nics,
            );
            mgr1.set_mcast_m2p(&req).unwrap();
            {
                let sockets =
                    mgr1.inner.mcast_underlay_sockets.sockets.lock().unwrap();
                assert!(sockets.contains_key(&underlay));
            }
            poll_v6_membership(LOOPBACK_IF, &underlay, true);
        }

        // mgr1 dropped: socket closed, kernel membership removed.
        poll_v6_membership(LOOPBACK_IF, &underlay, false);

        // Mock xde state (static) still has the M2P entry, simulating
        // xde kernel state surviving a sled-agent restart.
        {
            let hdl = Handle::new().unwrap();
            let dump = hdl.dump_m2p().unwrap();
            assert!(
                !dump.ip4.is_empty() || !dump.ip6.is_empty(),
                "Mock xde should still hold the M2P mapping after drop"
            );
        }

        // Second phase: new PortManager rehydrates from surviving xde state.
        let mgr2 =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &nics);
        {
            let sockets =
                mgr2.inner.mcast_underlay_sockets.sockets.lock().unwrap();
            assert!(
                sockets.contains_key(&underlay),
                "Rehydration should reopen socket for surviving M2P"
            );
        }
        poll_v6_membership(LOOPBACK_IF, &underlay, true);

        // Cleanup and clear the M2P.
        let clear_req =
            sled_agent_types::multicast::ClearMcast2Phys { group, underlay };
        mgr2.clear_mcast_m2p(&clear_req).unwrap();
        poll_v6_membership(LOOPBACK_IF, &underlay, false);

        logctx.cleanup_successful();
    }

    /// Verify that no sockets are created when no underlay NICs are
    /// configured (test/sim mode).
    #[test]
    fn underlay_multicast_mac_filter_no_nics() {
        let logctx = test_setup_log("underlay_multicast_mac_filter_no_nics");
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let underlay: Ipv6Addr = "ff04::2".parse().unwrap();
        let group: IpAddr = "239.10.10.2".parse().unwrap();

        let req =
            sled_agent_types::multicast::Mcast2PhysMapping { group, underlay };

        manager.set_mcast_m2p(&req).unwrap();
        {
            let sockets =
                manager.inner.mcast_underlay_sockets.sockets.lock().unwrap();
            assert!(
                sockets.is_empty(),
                "No sockets should be created without underlay NICs"
            );
        }

        // Cleanup so other tests sharing the static OPTE_STATE start clean.
        // A `PortManager` constructed in a later test re-joins any mapping
        // still present in the shared map on its own underlay NICs.
        manager
            .clear_mcast_m2p(&sled_agent_types::multicast::ClearMcast2Phys {
                group,
                underlay,
            })
            .unwrap();

        logctx.cleanup_successful();
    }

    /// Verify that setting the same group again with a different underlay
    /// replaces the prior mapping rather than accumulating entries.
    ///
    /// This pins the upsert-by-group semantics of the mock's `m2p` map,
    /// matching xde's `Mcast2Phys` shape. A regression here would surface
    /// as duplicate entries in `list_mcast_m2p` and prevent the reconciler
    /// from converging.
    #[test]
    fn multicast_m2p_set_replaces_underlay_for_same_group() {
        let logctx = test_setup_log(
            "multicast_m2p_set_replaces_underlay_for_same_group",
        );
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let group: IpAddr = "239.10.10.41".parse().unwrap();
        let underlay_x: Ipv6Addr = "ff04::a1".parse().unwrap();
        let underlay_y: Ipv6Addr = "ff04::a2".parse().unwrap();

        manager
            .set_mcast_m2p(&sled_agent_types::multicast::Mcast2PhysMapping {
                group,
                underlay: underlay_x,
            })
            .unwrap();
        manager
            .set_mcast_m2p(&sled_agent_types::multicast::Mcast2PhysMapping {
                group,
                underlay: underlay_y,
            })
            .unwrap();

        let mappings: Vec<_> = manager
            .list_mcast_m2p()
            .unwrap()
            .into_iter()
            .filter(|m| m.group == group)
            .collect();
        assert_eq!(
            mappings.len(),
            1,
            "expected a single mapping for group after setting it again, got {mappings:?}",
        );
        assert_eq!(mappings[0].underlay, underlay_y);

        // Cleanup so other tests sharing the static OPTE_STATE start clean.
        manager
            .clear_mcast_m2p(&sled_agent_types::multicast::ClearMcast2Phys {
                group,
                underlay: underlay_y,
            })
            .unwrap();

        logctx.cleanup_successful();
    }

    /// Verify that `clear_mcast_m2p` with a mismatched underlay is a
    /// noop, preserving the prior mapping. This pins the requirement
    /// that `clear_mcast_m2p` only removes the entry when both `group`
    /// and `underlay` match.
    #[test]
    fn multicast_m2p_clear_mismatched_underlay_is_noop() {
        let logctx =
            test_setup_log("multicast_m2p_clear_mismatched_underlay_is_noop");
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let group: IpAddr = "239.10.10.42".parse().unwrap();
        let underlay_x: Ipv6Addr = "ff04::b1".parse().unwrap();
        let underlay_z: Ipv6Addr = "ff04::b2".parse().unwrap();

        manager
            .set_mcast_m2p(&sled_agent_types::multicast::Mcast2PhysMapping {
                group,
                underlay: underlay_x,
            })
            .unwrap();

        // Clear with a different underlay than was set (not removing the
        // original mapping).
        manager
            .clear_mcast_m2p(&sled_agent_types::multicast::ClearMcast2Phys {
                group,
                underlay: underlay_z,
            })
            .unwrap();

        let mappings: Vec<_> = manager
            .list_mcast_m2p()
            .unwrap()
            .into_iter()
            .filter(|m| m.group == group)
            .collect();
        assert_eq!(
            mappings.len(),
            1,
            "mismatched clear should be a noop, expected mapping to remain",
        );
        assert_eq!(mappings[0].underlay, underlay_x);

        // Cleanup for shared static OPTE_STATE.
        manager
            .clear_mcast_m2p(&sled_agent_types::multicast::ClearMcast2Phys {
                group,
                underlay: underlay_x,
            })
            .unwrap();

        logctx.cleanup_successful();
    }

    /// Verify that `set_mcast_m2p` and `clear_mcast_m2p` reject an underlay
    /// address outside the admin-local multicast subnet (ff04::/16) with
    /// the proper error.
    #[test]
    fn multicast_m2p_rejects_non_admin_local_underlay() {
        let logctx =
            test_setup_log("multicast_m2p_rejects_non_admin_local_underlay");
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &[]);

        let group: IpAddr = "239.10.10.51".parse().unwrap();
        let bad_underlay: Ipv6Addr = "fd00::1".parse().unwrap();

        let err = manager
            .set_mcast_m2p(&sled_agent_types::multicast::Mcast2PhysMapping {
                group,
                underlay: bad_underlay,
            })
            .expect_err("set with non-admin-local underlay must be rejected");
        assert!(
            matches!(err, Error::InvalidMcastUnderlay(addr) if addr == bad_underlay),
            "expected InvalidMcastUnderlay({bad_underlay}), got {err:?}",
        );

        let err = manager
            .clear_mcast_m2p(&sled_agent_types::multicast::ClearMcast2Phys {
                group,
                underlay: bad_underlay,
            })
            .expect_err("clear with non-admin-local underlay must be rejected");
        assert!(
            matches!(err, Error::InvalidMcastUnderlay(addr) if addr == bad_underlay),
            "expected InvalidMcastUnderlay({bad_underlay}), got {err:?}",
        );

        logctx.cleanup_successful();
    }

    /// Verify that when any NIC multicast MAC filter join fails,
    /// `set_mcast_m2p` rolls back the xde M2P entry and returns the proper
    /// error, even when another NIC joined successfully.
    ///
    /// Without rollback the xde entry would stay present and the
    /// `list_mcast_m2p`-checked convergence loop would treat the mapping
    /// as already applied, silently dropping traffic for the group.
    ///
    /// We force a partial failure with one real interface and one
    /// nonexistent interface.
    #[cfg(target_os = "illumos")]
    #[test]
    fn multicast_m2p_set_rolls_back_on_partial_nic_join_failure() {
        let logctx = test_setup_log(
            "multicast_m2p_set_rolls_back_on_partial_nic_join_failure",
        );
        // `AddrObject` only validates that the name contains no slashes, so
        // the fake interface constructs successfully.
        let nics = vec![
            AddrObject::new_control(LOOPBACK_IF).unwrap(),
            AddrObject::new_control("nonexistent_xyz_nic_for_test").unwrap(),
        ];
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        let manager =
            PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST, &nics);

        let group: IpAddr = "239.10.10.61".parse().unwrap();
        let underlay: Ipv6Addr = "ff04::c1".parse().unwrap();
        let req =
            sled_agent_types::multicast::Mcast2PhysMapping { group, underlay };

        let err = manager
            .set_mcast_m2p(&req)
            .expect_err("set_mcast_m2p must fail when any NIC join fails");
        assert!(
            matches!(err, Error::UnderlayMcastJoinFailed(addr) if addr == underlay),
            "expected UnderlayMcastJoinFailed({underlay}), got {err:?}",
        );

        // The xde M2P entry must have been rolled back so the
        // convergence loop sees the gap and retries on the next pass.
        let listed =
            manager.list_mcast_m2p().expect("list_mcast_m2p succeeds in mock");
        assert!(
            !listed.iter().any(|m| m.group == group),
            "xde M2P entry must be rolled back after join failure, found {listed:?}",
        );

        {
            let sockets =
                manager.inner.mcast_underlay_sockets.sockets.lock().unwrap();
            assert!(
                !sockets.contains_key(&underlay),
                "no filter socket should exist after join failure",
            );
        }

        logctx.cleanup_successful();
    }

    /// Verify that a mapping stranded by a failed join rollback is healed
    /// by a later `set_mcast_m2p` call, mirroring the convergence loop
    /// re-running the set for an active group.
    ///
    /// The stranded scenario occurs in this way: a NIC join fails and the
    /// rollback `clear_m2p` also fails, cascadingly, leaving the xde entry
    /// present with no NIC MAC filter. A convergence check based only on
    /// `list_mcast_m2p` would treat the group as applied indefinitely.
    /// So, the re-run must retry the join rather than short-circuiting on the
    /// listed entry.
    #[cfg(target_os = "illumos")]
    #[test]
    fn multicast_m2p_set_retries_join_after_failed_rollback() {
        let logctx = test_setup_log(
            "multicast_m2p_set_retries_join_after_failed_rollback",
        );
        let handle = Handle::new().unwrap();
        let _state = ensure_xde_underlay(&handle);

        // Manager whose NIC join always fails: a nonexistent interface.
        // Constructed before the fault is injected, as is the healing
        // manager below, because `PortManager::new` re-joins mappings
        // listed in the shared state and would otherwise heal the
        // stranded entry before `set_mcast_m2p` gets the chance.
        let bad_nics = vec![
            AddrObject::new_control("nonexistent_xyz_nic_for_test").unwrap(),
        ];
        let bad_manager = PortManager::new(
            logctx.log.clone(),
            Ipv6Addr::LOCALHOST,
            &bad_nics,
        );

        // Manager with a joinable interface, standing in for the retry
        // path after the fault clears.
        let good_nics = vec![AddrObject::new_control(LOOPBACK_IF).unwrap()];
        let good_manager = PortManager::new(
            logctx.log.clone(),
            Ipv6Addr::LOCALHOST,
            &good_nics,
        );

        let group: IpAddr = "239.10.10.62".parse().unwrap();
        let underlay: Ipv6Addr = "ff04::c2".parse().unwrap();
        let req =
            sled_agent_types::multicast::Mcast2PhysMapping { group, underlay };

        // Strand the mapping: the NIC join fails and the rollback clear
        // fails too, so the xde entry survives.
        handle.state().lock().unwrap().fail_next_clear_m2p = true;
        let err = bad_manager
            .set_mcast_m2p(&req)
            .expect_err("set_mcast_m2p must fail when the NIC join fails");
        assert!(
            matches!(err, Error::UnderlayMcastJoinFailed(addr) if addr == underlay),
            "expected UnderlayMcastJoinFailed({underlay}), got {err:?}",
        );

        // The stranded state, i.e., mapping listed, no NIC membership.
        let listed = bad_manager.list_mcast_m2p().unwrap();
        assert!(
            listed.iter().any(|m| m.group == group && m.underlay == underlay),
            "xde entry must survive the failed rollback, found {listed:?}",
        );
        assert!(
            !netstat_v6_has_membership(LOOPBACK_IF, &underlay),
            "unexpected NIC membership {underlay} while stranded",
        );

        // A later pass re-runs the set even though the mapping is listed
        // and must complete the NIC join.
        good_manager
            .set_mcast_m2p(&req)
            .expect("retried set_mcast_m2p must heal the stranded mapping");
        poll_v6_membership(LOOPBACK_IF, &underlay, true);

        // Cleanup for shared static OPTE_STATE.
        good_manager
            .clear_mcast_m2p(&sled_agent_types::multicast::ClearMcast2Phys {
                group,
                underlay,
            })
            .unwrap();
        poll_v6_membership(LOOPBACK_IF, &underlay, false);

        logctx.cleanup_successful();
    }
}
