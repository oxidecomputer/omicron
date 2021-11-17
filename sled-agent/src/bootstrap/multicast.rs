use std::io;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use tokio::net::UdpSocket;

// TODO: 0x
//     pub static ref IPV6: IpAddr = Ipv6Addr::new(0xFF04, 0, 0, 0, 0, 0, 0, 0x0123).into();

/// Scope of an IPv6 Multicast address.
///
/// Attempts to align with the unstable [`std::net::Ipv6MulticastScope`] enum.
pub enum Ipv6MulticastScope {
    InterfaceLocal,
    LinkLocal,
    RealmLocal,
    AdminLocal,
    SiteLocal,
    OrganizationLocal,
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

fn new_ipv6_udp_socket(addr: &SocketAddrV6) -> io::Result<socket2::Socket> {
    eprintln!("Creating new socket");
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
    eprintln!("Creating new socket - setting v6 only");
    socket.set_only_v6(true)?;
    eprintln!("Creating new socket - OK");
    Ok(socket)
}

/// Create a new listening socket, capable of receiving IPv6 multicast traffic.
pub fn new_ipv6_multicast_udp_listener(addr: &SocketAddrV6) -> io::Result<UdpSocket> {
    eprintln!("Creating listener");
    let socket = new_ipv6_udp_socket(&addr)?;

    // From http://www.kohala.com/start/mcast.api.txt
    //
    // "More than one process may bind to the same SOCK_DGRAM UDP port [if
    // SO_REUSEADDR is used]. In this case, every incoming multicast or
    // broadcast UDP datagram destined to the shared port is delivered to all
    // sockets bound to the port."
    eprintln!("Creating listener - setting re-use");
    socket.set_reuse_address(true)?;
    // TODO: We can specify a more specific interface here. Should we?
    eprintln!("Creating listener - joining multi-cast");
    socket.join_multicast_v6(addr.ip(), 0)?;
    eprintln!("Creating listener - binding");
    socket.bind(&(*addr).into())?;
    eprintln!("Creating listener - OK");

    // Convert from: socket2 -> std -> tokio
    UdpSocket::from_std(std::net::UdpSocket::from(socket))
}

/// Create a new sending socket, capable of sending IPv6 multicast traffic.
pub fn new_ipv6_multicast_udp_sender(addr: &SocketAddrV6) -> io::Result<UdpSocket> {
    eprintln!("Creating sender");
    let socket = new_ipv6_udp_socket(&addr)?;
    // Avoid seeing our own transmissions.
    eprintln!("Creating sender - setting multicast loop to false");
    socket.set_multicast_loop_v6(false)?;

    // TODO: Should we pick a specific interface?
    // socket.set_multicast_if_v6(...)?;
    let any_interface_address = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0), 0, 0, 0);
    eprintln!("Creating sender - binding");
    socket.bind(&any_interface_address.into())?;
    eprintln!("Creating sender - OK");

    // Convert from: socket2 -> std -> tokio
    UdpSocket::from_std(std::net::UdpSocket::from(socket))
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_multicast_v6() {
        let message = b"Hello World!";
        let scope = Ipv6MulticastScope::LinkLocal.first_hextet();
        let address = SocketAddrV6::new(
            Ipv6Addr::new(scope, 0, 0, 0, 0, 0, 0, 0x0123),
            7645, 0, 0
        );

        eprintln!("Address: {}", address);
        let listener = new_ipv6_multicast_udp_listener(&address).unwrap();
        let sender = new_ipv6_multicast_udp_sender(&address).unwrap();


    }
}
