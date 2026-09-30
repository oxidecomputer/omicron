// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A single port on the OPTE virtual switch.

use crate::destructor::Deletable;
use crate::destructor::Destructor;
use crate::opte::Gateway;
use crate::opte::Handle;
use crate::opte::Vni;
use anyhow::Context as _;
use macaddr::MacAddr6;
use omicron_common::api::external;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_common::api::internal::shared::RouterId;
use omicron_common::api::internal::shared::RouterKind;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use sled_agent_types::inventory::NetworkInterfaceKind;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub struct PortData {
    /// Name of the port as identified by OPTE
    name: String,
    /// The VPC-private IP configuration for the port.
    ip: PrivateIpConfig,
    /// VPC-private MAC address
    mac: MacAddr6,
    /// Emulated PCI slot for the guest NIC, passed to Propolis
    slot: u8,
    /// Geneve VNI for the VPC
    vni: Vni,
    /// Information about the virtual gateway, aka OPTE
    gateway: Gateway,
}

struct PortInner {
    data: PortData,
    destructor: Destructor<PortName>,
}

impl Drop for PortInner {
    fn drop(&mut self) {
        self.destructor.enqueue_destroy(PortName(self.data.name.clone()));
    }
}

impl core::ops::Deref for PortInner {
    type Target = PortData;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

/// A port on the OPTE virtual switch, providing the virtual networking
/// abstractions for guest instances.
///
/// Note that the type is clonable and refers to the same underlying port on the
/// system.
#[derive(Clone)]
pub struct Port {
    inner: Arc<PortInner>,
}

impl std::fmt::Debug for Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Port")
            .field("name", &self.inner.name)
            .field("ip", &self.inner.ip)
            .field("mac", &self.inner.mac)
            .field("slot", &self.inner.slot)
            .field("vni", &self.inner.vni)
            .field("gateway", &self.inner.gateway)
            .finish()
    }
}

impl Port {
    pub(super) fn new(
        name: String,
        ip: PrivateIpConfig,
        mac: MacAddr6,
        slot: u8,
        vni: Vni,
        destructor: Destructor<PortName>,
    ) -> Self {
        let gateway = Gateway::from_ip_config(&ip);
        let data = PortData { name, ip, mac, slot, vni, gateway };
        Self { inner: Arc::new(PortInner { data, destructor }) }
    }

    /// Return the VPC-private IPv4 address, if it exists.
    pub fn ipv4_addr(&self) -> Option<&Ipv4Addr> {
        self.inner.ip.ipv4_addr()
    }

    /// Return the VPC-private IPv6 address, if it exists.
    pub fn ipv6_addr(&self) -> Option<&Ipv6Addr> {
        self.inner.ip.ipv6_addr()
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Return the OPTE gateway IPv4 address and the private IPv4 address.
    ///
    /// If the port is not configured for IPv4, None is returned.
    // TODO-remove: <https://github.com/oxidecomputer/omicron/issues/2931>
    pub fn gateway_and_private_ipv4(&self) -> Option<(&Ipv4Addr, &Ipv4Addr)> {
        match (self.inner.gateway.ipv4_addr(), self.ipv4_addr()) {
            (None, None) => None,
            (None, Some(_)) => unreachable!(),
            (Some(_), None) => unreachable!(),
            (Some(gateway_ip), Some(private_ip)) => {
                Some((gateway_ip, private_ip))
            }
        }
    }

    #[allow(dead_code)]
    pub fn mac(&self) -> &MacAddr6 {
        &self.inner.mac
    }

    pub fn vni(&self) -> &Vni {
        &self.inner.vni
    }

    /// Return the VPC-private IPv4 subnet, if it exists.
    pub fn ipv4_subnet(&self) -> Option<&Ipv4Net> {
        self.inner.ip.ipv4_subnet()
    }

    /// Return the VPC-private IPv6 subnet, if it exists.
    pub fn ipv6_subnet(&self) -> Option<&Ipv6Net> {
        self.inner.ip.ipv6_subnet()
    }

    pub fn slot(&self) -> u8 {
        self.inner.slot
    }

    pub fn system_router_key(&self) -> RouterId {
        // Unwrap safety: both of these VNI types represent validated u24s.
        let vni = external::Vni::try_from(self.vni().as_u32()).unwrap();
        RouterId { vni, kind: RouterKind::System }
    }

    pub fn custom_ipv4_router_key(&self) -> Option<RouterId> {
        self.ipv4_subnet().copied().map(|subnet| RouterId {
            kind: RouterKind::Custom(subnet.into()),
            ..self.system_router_key()
        })
    }

    pub fn custom_ipv6_router_key(&self) -> Option<RouterId> {
        self.ipv6_subnet().copied().map(|subnet| RouterId {
            kind: RouterKind::Custom(subnet.into()),
            ..self.system_router_key()
        })
    }
}

#[cfg(test)]
static TEST_DELETE_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
const DELETE_ATTEMPTS_IN_TESTS: u64 = 3;

pub(super) struct PortName(String);

impl PortName {
    #[cfg(test)]
    fn maybe_fail_delete_in_tests(&self) -> Result<(), anyhow::Error> {
        let count = TEST_DELETE_COUNT
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count < DELETE_ATTEMPTS_IN_TESTS {
            anyhow::bail!("pretending to fail deletion in tests, call {count}");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Deletable for PortName {
    async fn delete(&self) -> Result<(), anyhow::Error> {
        #[cfg(test)]
        self.maybe_fail_delete_in_tests()?;

        let hdl = Handle::new().context("creating handle to OPTE driver")?;
        hdl.delete_xde(&self.0).context("deleting XDE device").map(|_| ())
    }
}

/// An OPTE port, along with its control plane metadata.
pub struct PortInfo {
    pub port: Port,
    pub nic_id: Uuid,
    pub nic_kind: NetworkInterfaceKind,
}

#[cfg(test)]
mod tests {
    use crate::opte::PortCreateParams;
    use crate::opte::PortManager;
    use crate::opte::port::DELETE_ATTEMPTS_IN_TESTS;
    use crate::opte::port::TEST_DELETE_COUNT;
    use macaddr::MacAddr6;
    use omicron_common::api::external::MacAddr;
    use omicron_common::api::external::Vni;
    use omicron_common::api::internal::shared::PrivateIpConfig;
    use omicron_common::api::internal::shared::PrivateIpv4Config;
    use omicron_test_utils::dev;
    use oxide_vpc::api::DhcpCfg;
    use oxnet::Ipv4Net;
    use sled_agent_types::instance::ExternalIpConfig;
    use sled_agent_types::inventory::NetworkInterface;
    use sled_agent_types::inventory::NetworkInterfaceKind;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_drop_enqueues_destroy() {
        crate::opte::Handle::new()
            .unwrap()
            .set_xde_underlay("foo0", "foo1")
            .unwrap();
        let logctx = dev::test_setup_log("test_drop_enqueues_destroy");
        let manager = PortManager::new(logctx.log.clone(), Ipv6Addr::LOCALHOST);
        let id = Uuid::new_v4();
        let kind = NetworkInterfaceKind::Instance { id: Uuid::new_v4() };
        let key = (id, kind);
        let (port, ticket) = manager
            .create_port(PortCreateParams {
                nic: &NetworkInterface {
                    id,
                    kind,
                    name: "net0".parse().unwrap(),
                    ip_config: PrivateIpConfig::V4(
                        PrivateIpv4Config::new(
                            Ipv4Addr::new(10, 0, 0, 5),
                            Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 0), 24)
                                .unwrap(),
                        )
                        .unwrap(),
                    ),
                    mac: MacAddr(MacAddr6::new(
                        0xa8, 0x40, 0x25, 0x01, 0x01, 0x01,
                    )),
                    vni: Vni::try_from(7).unwrap(),
                    primary: true,
                    slot: 0,
                },
                external_ips: &ExternalIpConfig { v4: None, v6: None },
                firewall_rules: &[],
                dhcp_config: DhcpCfg {
                    hostname: None,
                    host_domain: None,
                    domain_search_list: vec![],
                    dns4_servers: vec![],
                    dns6_servers: vec![],
                },
                attached_subnets: vec![],
                mtu: None,
            })
            .unwrap();

        // We should have no destructor calls
        assert_eq!(TEST_DELETE_COUNT.load(Ordering::Relaxed), 0);

        // Dropping the ticket should remove the port from the map, but nothing
        // else.
        drop(ticket);
        assert!(!manager.contains(&key));
        assert_eq!(TEST_DELETE_COUNT.load(Ordering::Relaxed), 0);

        // Dropping the port should eventually actually delete the thing.
        drop(port);

        dev::poll::wait_for_condition(
            || async {
                if TEST_DELETE_COUNT.load(Ordering::Relaxed)
                    == DELETE_ATTEMPTS_IN_TESTS
                {
                    Ok(())
                } else {
                    Err(dev::poll::CondCheckError::<()>::NotYet {
                        status: None,
                    })
                }
            },
            &Duration::from_millis(100),
            &Duration::from_secs(10),
        )
        .await
        .expect("Should have deleted the port eventually");

        // We should have attempted to delete the port as many times as it
        // takes.
        assert_eq!(
            TEST_DELETE_COUNT.load(Ordering::Relaxed),
            DELETE_ATTEMPTS_IN_TESTS
        );
    }
}
