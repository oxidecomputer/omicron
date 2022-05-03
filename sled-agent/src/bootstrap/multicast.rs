// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ipv6 Multicast utilities used for Sled discovery.

use std::io;
use std::net::{Ipv6Addr, SocketAddrV6};
use tokio::net::UdpSocket;

/// Scope of an IPv6 Multicast address.
///
/// Attempts to align with the unstable [`std::net::Ipv6MulticastScope`] enum.
pub enum Ipv6MulticastScope {
    #[allow(dead_code)]
    InterfaceLocal,
    LinkLocal,
    #[allow(dead_code)]
    RealmLocal,
    #[allow(dead_code)]
    AdminLocal,
    #[allow(dead_code)]
    SiteLocal,
    #[allow(dead_code)]
    OrganizationLocal,
    #[allow(dead_code)]
    GlobalScope,
}

impl Ipv6MulticastScope {
    /// Returns the first hextet of an Ipv6 multicast IP address.
    pub fn first_hextet(&self) -> u16 {
        // Reference: https://datatracker.ietf.org/doc/html/rfc4291#section-2.7
        //
        // This implementation currently sets all multicast flags to zero;
        // this could easily change if needed.
        let flags = 0;
        let flags_shifted = flags << 4;
        match self {
            Ipv6MulticastScope::InterfaceLocal => 0xFF01 | flags_shifted,
            Ipv6MulticastScope::LinkLocal => 0xFF02 | flags_shifted,
            Ipv6MulticastScope::RealmLocal => 0xFF03 | flags_shifted,
            Ipv6MulticastScope::AdminLocal => 0xFF04 | flags_shifted,
            Ipv6MulticastScope::SiteLocal => 0xFF05 | flags_shifted,
            Ipv6MulticastScope::OrganizationLocal => 0xFF08 | flags_shifted,
            Ipv6MulticastScope::GlobalScope => 0xFF0E | flags_shifted,
        }
    }
}

fn new_ipv6_udp_socket() -> io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV6,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_only_v6(true)?;
    // From
    // https://docs.rs/tokio/1.14.0/tokio/net/struct.UdpSocket.html#method.from_std
    //
    // "It is left up to the user to set it in non-blocking mode".
    //
    // We (the user) do that here.
    socket.set_nonblocking(true)?;
    Ok(socket)
}

/// Create a new listening socket, capable of receiving IPv6 multicast traffic.
fn new_ipv6_udp_listener(
    addr: &SocketAddrV6,
    interface: u32,
) -> io::Result<UdpSocket> {
    let socket = new_ipv6_udp_socket()?;

    // From http://www.kohala.com/start/mcast.api.txt
    //
    // "More than one process may bind to the same SOCK_DGRAM UDP port [if
    // SO_REUSEADDR is used]. In this case, every incoming multicast or
    // broadcast UDP datagram destined to the shared port is delivered to all
    // sockets bound to the port."
    socket.set_reuse_address(true)?;

    socket.join_multicast_v6(addr.ip(), interface)?;
    let bind_address = SocketAddrV6::new(*addr.ip(), addr.port(), 0, 0);
    socket.bind(&(bind_address).into())?;

    // Convert from: socket2 -> std -> tokio
    UdpSocket::from_std(std::net::UdpSocket::from(socket))
}

/// Create a new sending socket, capable of sending IPv6 multicast traffic.
fn new_ipv6_udp_sender(
    addr: &Ipv6Addr,
    loopback: bool,
    interface: u32,
) -> io::Result<UdpSocket> {
    let socket = new_ipv6_udp_socket()?;
    socket.set_multicast_loop_v6(loopback)?;
    socket.set_multicast_if_v6(interface)?;
    let address = SocketAddrV6::new(*addr, 0, 0, 0);
    socket.bind(&address.into())?;

    UdpSocket::from_std(std::net::UdpSocket::from(socket))
}

pub fn multicast_address() -> SocketAddrV6 {
    let scope = Ipv6MulticastScope::LinkLocal.first_hextet();
    SocketAddrV6::new(Ipv6Addr::new(scope, 0, 0, 0, 0, 0, 0, 0x1), 7645, 0, 0)
}

/// Returns the (sender, receiver) sockets of an IPv6 UDP multicast group.
///
/// * `address`: The address to use for sending.
/// * `loopback`: If true, the receiver packet will see multicast packets sent
/// on our sender, in addition to those sent by everyone else in the multicast
/// group.
/// * `interface`: The index of the interface to join (zero indicates "any
/// interface").
pub fn new_ipv6_udp_pair(
    address: &Ipv6Addr,
    loopback: bool,
    interface: u32,
) -> io::Result<(UdpSocket, UdpSocket)> {
    let sender = new_ipv6_udp_sender(&address, loopback, interface)?;
    let listener = new_ipv6_udp_listener(&multicast_address(), interface)?;

    Ok((sender, listener))
}

// Refer to sled-agent/tests/integration_tests/multicast.rs for tests.
