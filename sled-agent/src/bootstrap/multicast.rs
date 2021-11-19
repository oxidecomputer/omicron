//! Ipv6 Multicast utilities used for Sled discovery.

use std::io;
use std::net::{Ipv6Addr, SocketAddrV6};
use tokio::net::UdpSocket;

// NOTE: Use zones? Exclusive netstacks?
//  ipdadm can drop packets?
//  pstop can stop packets
// NOTE: start w/link local, get address (TCP!) from 'hello' message.
// NOTE: Want to to set up pairs for each link. We'll have
// one for each switch.
// NOTE: "LIF" ioctls to identify devices?
// NOTE: Maghemite uses link-level multicast to discovery neighbors.

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
        // From
        // https://docs.rs/tokio/1.14.0/tokio/net/struct.UdpSocket.html#method.from_std
        //
        // "It is left up to the user to set it in non-blocking mode".
        //
        // We (the user) do that here.
        socket2::Type::DGRAM.nonblocking(),
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_only_v6(true)?;
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

    // TODO: I tried binding on the input value of "addr.ip()", but doing so
    // returns errno 22 ("Invalid Input").
    //
    // This may be binding to a larger address range than we want.
    let bind_address =
        SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, addr.port(), 0, 0);
    socket.bind(&(bind_address).into())?;

    // Convert from: socket2 -> std -> tokio
    UdpSocket::from_std(std::net::UdpSocket::from(socket))
}

/// Create a new sending socket, capable of sending IPv6 multicast traffic.
fn new_ipv6_udp_sender(
    loopback: bool,
    interface: u32,
) -> io::Result<UdpSocket> {
    let socket = new_ipv6_udp_socket()?;
    socket.set_multicast_loop_v6(loopback)?;
    socket.set_multicast_if_v6(interface)?;
    let address = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
    socket.bind(&address.into())?;

    UdpSocket::from_std(std::net::UdpSocket::from(socket))
}

/// Returns the (sender, receiver) sockets of an IPv6 UDP multicast group.
///
/// * `address`: The address to use. Consider a value from:
/// <https://www.iana.org/assignments/ipv6-multicast-addresses/ipv6-multicast-addresses.xhtml>,
/// and the [`Ipv6MulticastScope`] helper to provide the first hextet.
/// * `loopback`: If true, the receiver packet will see multicast packets sent
/// on our sender, in addition to those sent by everyone else in the multicast
/// group.
/// * `interface`: The index of the interface to join (zero indicates "any
/// interface").
pub fn new_ipv6_udp_pair(
    address: &SocketAddrV6,
    loopback: bool,
    interface: u32,
) -> io::Result<(UdpSocket, UdpSocket)> {
    let sender = new_ipv6_udp_sender(loopback, interface)?;
    let listener = new_ipv6_udp_listener(address, interface)?;

    Ok((sender, listener))
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_multicast_ipv6() {
        let message = b"Hello World!";
        let scope = Ipv6MulticastScope::LinkLocal.first_hextet();
        let address = SocketAddrV6::new(
            Ipv6Addr::new(scope, 0, 0, 0, 0, 0, 0, 0x1),
            7645,
            0,
            0,
        );

        // For this test, we want to see our own transmission.
        // Unlike most usage in the Sled Agent, this means we want
        // loopback to be enabled.
        let loopback = true;
        let interface = 0;
        let (sender, listener) =
            new_ipv6_udp_pair(&address, loopback, interface).unwrap();

        // Create a receiver task which reads for messages that have
        // been broadcast, verifies the message, and returns the
        // calling address.
        let receiver_task_handle = tokio::task::spawn(async move {
            let mut buf = vec![0u8; 32];
            let (len, addr) = listener.recv_from(&mut buf).await?;
            assert_eq!(message.len(), len);
            assert_eq!(message, &buf[..message.len()]);
            Ok::<_, io::Error>(addr)
        });

        // Send a message repeatedly, and exit successfully if we
        // manage to receive the response.
        tokio::pin!(receiver_task_handle);
        let mut send_count = 0;
        loop {
            tokio::select! {
                result = sender.send_to(message, address) => {
                    assert_eq!(message.len(), result.unwrap());
                    send_count += 1;
                    if send_count > 10 {
                        panic!("10 multicast UDP messages sent with no response");
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
                result = &mut receiver_task_handle => {
                    let addr = result.unwrap().unwrap();
                    eprintln!("Receiver received message: {:#?}", addr);
                    break;
                }
            }
        }
    }
}
