// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! One-shot ICMP echo probes for IPv4 and IPv6.
//!
//! Opens a raw ICMP socket, sends a single echo request, and waits up to a
//! timeout for a matching reply. Requires the process to have permission to
//! open raw sockets (available in the NTP zone).

use internet_checksum::Checksum;
use socket2::Domain;
use socket2::Protocol;
use socket2::SockAddr;
use socket2::Socket;
use socket2::Type;
use std::io;
use std::mem::MaybeUninit;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

// TODO-K: Test properly and probably move this submodule somehwere else
// where it can be reused?

// ICMPv4 message types (RFC 792).
const ICMPV4_ECHO_REQUEST: u8 = 8;
const ICMPV4_ECHO_REPLY: u8 = 0;

// ICMPv6 message types (RFC 4443).
const ICMPV6_ECHO_REQUEST: u8 = 128;
const ICMPV6_ECHO_REPLY: u8 = 129;

// TODO-K: Do i need this struct???
/// The result of a successful ICMP echo exchange.
#[derive(Debug)]
pub struct PingResult {
    /// Round-trip time from send to matching reply.
    pub rtt: Duration,
}

// TODO-K: Do I need both async and sync?

// TODO-K: Should I also support DNS resolution?

/// Send a single ICMP echo request to `target` and wait up to `timeout` for
/// a matching reply. Returns the round-trip time on success.
pub async fn ping_once(
    target: IpAddr,
    timeout: Duration,
) -> io::Result<PingResult> {
    let identifier: u16 = rand::random();
    tokio::task::spawn_blocking(move || {
        ping_once_blocking(target, identifier, timeout)
    })
    .await?
}

fn ping_once_blocking(
    target: IpAddr,
    identifier: u16,
    timeout: Duration,
) -> io::Result<PingResult> {
    let sequence: u16 = 1;
    let (socket, request, expected_reply_type) = match target {
        IpAddr::V4(_) => {
            let sock =
                Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::ICMPV4))?;
            (sock, build_icmpv4_echo(identifier, sequence), ICMPV4_ECHO_REPLY)
        }
        IpAddr::V6(_) => {
            let sock =
                Socket::new(Domain::IPV6, Type::RAW, Some(Protocol::ICMPV6))?;
            (sock, build_icmpv6_echo(identifier, sequence), ICMPV6_ECHO_REPLY)
        }
    };

    let target_sa: SockAddr = SocketAddr::new(target, 0).into();

    let sent_at = Instant::now();
    socket.send_to(&request, &target_sa)?;

    let deadline = sent_at + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "ping timed out waiting for reply",
            ));
        }
        socket.set_read_timeout(Some(remaining))?;

        let mut buf = [MaybeUninit::<u8>::uninit(); 1500];
        let (n, from) = match socket.recv_from(&mut buf) {
            Ok(pair) => pair,
            Err(err)
                if err.kind() == io::ErrorKind::TimedOut
                    || err.kind() == io::ErrorKind::WouldBlock =>
            {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "ping timed out waiting for reply",
                ));
            }
            Err(err) => return Err(err),
        };
        let received_at = Instant::now();

        // SAFETY: `recv_from` reports how many bytes were actually written.
        let bytes: &[u8] =
            unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, n) };

        // On an IPv4 raw ICMP socket the kernel hands us the full IPv4
        // packet (variable-length header + ICMP payload). On an IPv6 raw
        // ICMPv6 socket we only get the ICMPv6 message itself.
        let icmp = match target {
            IpAddr::V4(_) => {
                if bytes.len() < 20 {
                    continue;
                }
                let ihl = (bytes[0] & 0x0f) as usize * 4;
                if bytes.len() < ihl + 8 {
                    continue;
                }
                &bytes[ihl..]
            }
            IpAddr::V6(_) => {
                if bytes.len() < 8 {
                    continue;
                }
                bytes
            }
        };

        // Filter to our reply: right type, right identifier, and (when the
        // kernel gives us the source address) from the expected peer.
        if icmp[0] != expected_reply_type {
            continue;
        }
        let reply_id = u16::from_be_bytes([icmp[4], icmp[5]]);
        if reply_id != identifier {
            continue;
        }
        if let Some(from_ip) = from.as_socket().map(|sa| sa.ip()) {
            if from_ip != target {
                continue;
            }
        }

        return Ok(PingResult {
            rtt: received_at.saturating_duration_since(sent_at),
        });
    }
}

/// Build an 8-byte ICMPv4 echo request with the checksum computed over the
/// header.
fn build_icmpv4_echo(identifier: u16, sequence: u16) -> [u8; 8] {
    let mut pkt = [0u8; 8];
    pkt[0] = ICMPV4_ECHO_REQUEST;
    // pkt[1] = code = 0
    // pkt[2..4] = checksum, filled in below
    pkt[4..6].copy_from_slice(&identifier.to_be_bytes());
    pkt[6..8].copy_from_slice(&sequence.to_be_bytes());

    let mut c = Checksum::new();
    c.add_bytes(&pkt);
    let checksum = c.checksum();
    pkt[2..4].copy_from_slice(&checksum);
    pkt
}

/// Build an 8-byte ICMPv6 echo request. The kernel fills in the ICMPv6
/// checksum for `IPPROTO_ICMPV6` raw sockets (RFC 3542 §3.1), so we leave
/// those bytes zero.
fn build_icmpv6_echo(identifier: u16, sequence: u16) -> [u8; 8] {
    let mut pkt = [0u8; 8];
    pkt[0] = ICMPV6_ECHO_REQUEST;
    // pkt[1] = code = 0
    // pkt[2..4] = checksum, kernel fills in
    pkt[4..6].copy_from_slice(&identifier.to_be_bytes());
    pkt[6..8].copy_from_slice(&sequence.to_be_bytes());
    pkt
}
