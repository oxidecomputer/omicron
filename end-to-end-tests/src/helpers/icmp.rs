// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use colored::*;
use internet_checksum::Checksum;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::mem::MaybeUninit;
use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6, UdpSocket,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};

const HIDE_CURSOR: &str = "\x1b[?25l";
const SHOW_CURSOR: &str = "\x1b[?25h";
const MOVE_CURSOR_UP: &str = "\x1b[A";

const ICMP_ECHO_TYPE: u8 = 8;
const ICMP_ECHO_CODE: u8 = 0;
const ICMP_ECHO_REPLY_TYPE: u8 = 0;

// ICMPv6 message types (RFC 4443). Echo request/reply use distinct type values
// from ICMPv4, and the code is zero for both.
const ICMPV6_ECHO_TYPE: u8 = 128;
const ICMPV6_ECHO_CODE: u8 = 0;
const ICMPV6_ECHO_REPLY_TYPE: u8 = 129;

// IPv4 raw sockets deliver the leading IP header to userspace, so received
// datagrams are parsed past a fixed 20-byte IPv4 header. IPv6 raw sockets never
// include the IPv6 header (RFC 3542), so v6 datagrams are parsed from offset 0.
const IPV4_HEADER_LEN: usize = 20;

#[derive(Debug, Serialize, Deserialize)]
struct EchoRequest {
    typ: u8,
    code: u8,
    checksum: u16,
    identifier: u16,
    sequence_number: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub v4: Vec<Ping4State>,
}

/// Run a ping test against the provided destination addreses, with the
/// specified time-to-live (ttl) at a given rate in packets per second
/// (pps) for the specified duration.
pub fn ping4_test_run(
    dst: &[Ipv4Addr],
    ttl: u32,
    pps: usize,
    duration: Duration,
) -> Report {
    let p = Pinger4::new(ttl);
    for dst in dst {
        // use a random number for the ICMP identifier
        p.add_target(rand::random(), *dst, pps, duration);
    }
    // Use an ASCII code to hide the blinking cursor as it makes the output hard
    // to read.
    print!("{HIDE_CURSOR}");
    p.clone().show();
    // wait for the test to conclude plus a bit of buffer time for packets in
    // flight.
    sleep(duration + Duration::from_millis(250));
    for _ in 0..p.targets.lock().unwrap().len() {
        println!();
    }
    // turn the blinky cursor back on
    print!("{SHOW_CURSOR}");

    // return a report to the caller
    let v4 = p.targets.lock().unwrap().values().copied().collect();
    Report { v4 }
}

struct Pinger4 {
    sock: Socket,
    targets: Mutex<BTreeMap<u16, Ping4State>>,
}

/// Running results for a single ping target, generic over the address family.
///
/// Aliased as [`Ping4State`] and [`Ping6State`] for the two concrete families.
/// The accounting fields are identical, only the address type differs.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PingState<T> {
    /// Destination address of the ping test.
    pub dest: T,
    /// Low water mark for ping round trip times.
    pub low: Duration,
    /// High water mark for ping round trip times.
    pub high: Duration,
    /// Summation of ping round trip times.
    pub sum: Duration,
    /// The last recorded ping round trip time.
    pub current: Option<Duration>,
    /// The number of ICMP packets considered lost. Does not start ticking
    /// until at least one reply has been received.
    pub lost: usize,
    /// The number of packets sent. Widened past the 16-bit ICMP sequence number
    /// so a long or high-rate run (more than 65535 packets) cannot wrap the
    /// counter and corrupt the loss math.
    pub tx_count: u32,
    /// The number of packets received.
    pub rx_count: u32,
    /// The last time a packet was sent.
    #[serde(skip)]
    pub sent: Option<Instant>,
    /// The transmit counter value when we received the first reply.
    #[serde(skip)]
    pub first: u32,
}

/// Per-target ping accounting for an IPv4 destination.
pub type Ping4State = PingState<Ipv4Addr>;
/// Per-target ping accounting for an IPv6 destination.
pub type Ping6State = PingState<Ipv6Addr>;

impl<T> PingState<T> {
    fn new(addr: T) -> Self {
        Self {
            dest: addr,
            low: Duration::default(),
            high: Duration::default(),
            sum: Duration::default(),
            current: None,
            lost: 0,
            tx_count: 0,
            rx_count: 0,
            sent: None,
            first: 0,
        }
    }

    /// Record a sent request at `at`, advancing the transmit count to
    /// `tx_total`.
    fn record_sent(&mut self, at: Instant, tx_total: u32) {
        self.sent = Some(at);
        self.tx_count = tx_total;
    }

    /// Record a matched reply received at `at`: bump the receive count, latch
    /// the transmit baseline on the first reply, and fold the round-trip time
    /// into the running statistics. The round-trip update is skipped when no
    /// send has been recorded yet, which cannot happen once traffic is flowing.
    fn record_reply(&mut self, at: Instant) {
        self.rx_count += 1;
        if self.first == 0 {
            self.first = self.tx_count;
        }
        if let Some(sent) = self.sent {
            let dt = at - sent;
            self.current = Some(dt);
            if self.low == Duration::ZERO || dt < self.low {
                self.low = dt;
            }
            if dt > self.high {
                self.high = dt;
            }
            self.sum += dt;
        }
    }
}

impl Pinger4 {
    fn new(ttl: u32) -> Arc<Self> {
        let sock = Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::ICMPV4))
            .unwrap();
        sock.set_ttl(ttl).unwrap();
        let s = Arc::new(Self { sock, targets: Mutex::new(BTreeMap::new()) });
        s.clone().rx();
        s.clone().count_lost();
        s
    }

    fn show(self: Arc<Self>) {
        print_ping_header();
        // run the reporting on a background thread
        spawn(move || {
            loop {
                // print a status line for each target
                for (_id, t) in self.targets.lock().unwrap().iter() {
                    print_ping_row(t);
                }
                // move the cursor back to the top for another round of reporting
                for _ in 0..self.targets.lock().unwrap().len() {
                    print!("{MOVE_CURSOR_UP}");
                }
                print!("\r");

                sleep(Duration::from_millis(100));
            }
        });
    }

    fn add_target(
        self: &Arc<Self>,
        id: u16,
        addr: Ipv4Addr,
        pps: usize,
        duration: Duration,
    ) {
        self.targets.lock().unwrap().insert(id, Ping4State::new(addr));
        let interval = Duration::from_secs_f64(1.0 / pps as f64);
        self.clone().tx(id, addr, interval, duration);
    }

    fn tx(
        self: Arc<Self>,
        id: u16,
        dst: Ipv4Addr,
        interval: Duration,
        duration: Duration,
    ) {
        // `seq` is the 16-bit ICMP sequence number and is allowed to wrap.
        // `tx_total` is the true count of packets sent and must not, so it is a
        // separate wide counter.
        let mut seq = 0u16;
        let mut tx_total: u32 = 0;
        let stop = Instant::now() + duration;
        // send ICMP test packets on a background thread
        spawn(move || {
            loop {
                if Instant::now() >= stop {
                    break;
                }
                let mut c = Checksum::new();
                c.add_bytes(&[ICMP_ECHO_TYPE, ICMP_ECHO_CODE]);
                c.add_bytes(&id.to_be_bytes());
                c.add_bytes(&seq.to_be_bytes());
                let pkt = EchoRequest {
                    typ: ICMP_ECHO_TYPE,
                    code: ICMP_ECHO_CODE,
                    checksum: u16::from_be_bytes(c.checksum()),
                    identifier: id,
                    sequence_number: seq,
                };
                let msg = ispf::to_bytes_be(&pkt).unwrap();

                match self.targets.lock().unwrap().get_mut(&id) {
                    Some(ref mut tgt) => {
                        tx_total += 1;
                        tgt.record_sent(Instant::now(), tx_total);
                        let sa: SockAddr = SocketAddrV4::new(dst, 0).into();
                        self.sock.send_to(&msg, &sa).unwrap();
                    }
                    None => continue,
                }

                seq = seq.wrapping_add(1);
                sleep(interval);
            }
        });
    }

    // At the end of the day this is not strictly necessary for the final
    // report. But it's really nice for interactive use to have a live
    // ticker for lost packet count.
    fn count_lost(self: Arc<Self>) {
        spawn(move || {
            loop {
                for (_, tgt) in self.targets.lock().unwrap().iter_mut() {
                    update_lost_packet_count(tgt);
                }
                sleep(Duration::from_millis(10));
            }
        });
    }

    fn rx(self: Arc<Self>) {
        // Spawn a background thread to receive ICMP replies and do the
        // necessary accounting.
        spawn(move || {
            loop {
                let mut ubuf = [MaybeUninit::new(0); 10240];
                if let Ok((sz, _)) = self.sock.recv_from(&mut ubuf) {
                    let buf = unsafe { ubuf[..sz].assume_init_ref() };
                    let msg: EchoRequest =
                        match ispf::from_bytes_be(&buf[IPV4_HEADER_LEN..sz]) {
                            Ok(msg) => msg,
                            Err(_) => {
                                continue;
                            }
                        };
                    // correlate the ICMP identifier with a target
                    match self.targets.lock().unwrap().get_mut(&msg.identifier)
                    {
                        Some(ref mut target) => {
                            target.record_reply(Instant::now());
                        }
                        None => {
                            println!("no target {}", msg.identifier);
                        }
                    }
                }
            }
        });
    }
}

/// Print the column header shared by the unicast and multicast tickers.
fn print_ping_header() {
    println!(
        "{:15} {:7} {:7} {:7} {:7} {:7} {:9} {}",
        "addr".dimmed(),
        "low".dimmed(),
        "avg".dimmed(),
        "high".dimmed(),
        "last".dimmed(),
        "sent".dimmed(),
        "received".dimmed(),
        "lost".dimmed()
    );
}

/// Print a single per-target status line shared by both tickers.
fn print_ping_row<T: Display>(t: &PingState<T>) {
    println!(
        "{:15} {:7} {:7} {:7} {:7} {:7} {:9} {:<7}",
        t.dest.to_string().cyan(),
        format!("{:.3}", (t.low.as_micros() as f32 / 1000.0)),
        if t.rx_count == 0 {
            format!("{:.3}", 0.0)
        } else {
            format!(
                "{:.3}",
                (t.sum.as_micros() as f32 / 1000.0 / t.rx_count as f32)
            )
        },
        format!("{:.3}", (t.high.as_micros() as f32 / 1000.0)),
        match t.current {
            Some(dt) => format!("{:.3}", (dt.as_micros() as f32 / 1000.0)),
            None => format!("{:.3}", 0.0),
        },
        t.tx_count.to_string(),
        t.rx_count.to_string(),
        if t.lost == 0 {
            t.lost.to_string().green()
        } else {
            t.lost.to_string().red()
        },
    );
}

/// Recompute a target's lost-packet count.
///
/// Loss is only considered after the first reply arrives, giving the remote
/// endpoint time to come online without charging initial packets as lost.
fn update_lost_packet_count<T>(t: &mut PingState<T>) {
    if t.first != 0 {
        t.lost = t.tx_count.saturating_sub(t.first).saturating_sub(t.rx_count)
            as usize;
    }
}

/// Per-member reply accounting for a multicast dataplane test.
///
/// This is distinct from [`PingState`], the unicast per-destination equivalent.
/// A multicast stream is sent once to the group address and the rack replicates
/// it to every joined member, so `member` is the source address a reply came
/// back from (the responder), not a destination the sender chose. The timing
/// and counter fields carry the same meaning as in [`PingState`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct McastMember<T> {
    /// Address of the member that replied, taken from the reply's source.
    pub member: T,
    /// Low water mark for round trip times.
    pub low: Duration,
    /// High water mark for round trip times.
    pub high: Duration,
    /// Summation of round trip times.
    pub sum: Duration,
    /// The last recorded round trip time.
    pub current: Option<Duration>,
    /// The number of packets considered lost.
    pub lost: usize,
    /// The number of packets sent to the group.
    pub tx_count: u32,
    /// The number of replies received from this member.
    pub rx_count: u32,
}

impl<T> From<PingState<T>> for McastMember<T> {
    fn from(state: PingState<T>) -> Self {
        Self {
            member: state.dest,
            low: state.low,
            high: state.high,
            sum: state.sum,
            current: state.current,
            lost: state.lost,
            tx_count: state.tx_count,
            rx_count: state.rx_count,
        }
    }
}

/// Running results for a multicast dataplane test, generic over the group's
/// address family.
#[derive(Debug, Serialize, Deserialize)]
pub struct McastReport<T> {
    /// The multicast group address that was pinged.
    pub group: T,
    /// The source address the request stream egressed from. A single stream is
    /// sent from this one sender to the group. The rack replicates it to every
    /// member, so each member in `members` replies to this sender.
    pub sender: Option<T>,
    /// Per-member reply accounting, one entry per responder. The count of
    /// entries is the replication fan-out (one sender to many members).
    pub members: Vec<McastMember<T>>,
    /// Copies of the sender's own requests that arrived back at the sender
    /// from the wire. The pinger socket joins the group as a listener and
    /// disables multicast loopback, so any echo request observed with the
    /// stream's identifier and the sender's own source address was delivered
    /// by the network, not looped locally. A correct dataplane never
    /// replicates a sender's own group traffic back to it, so this must be
    /// zero.
    pub sender_self_rx: u32,
}

/// Outcome of `drain_until_quiescent`.
///
/// The two exits are not equivalent: [`DrainResult::Quiesced`] means every
/// replicated reply was counted, while [`DrainResult::TimedOut`] means the
/// receive count never settled, so the tally may be incomplete or duplicate
/// delivery is still ongoing. Callers should treat a timeout as a test failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainResult {
    /// Receive count held steady for the quiesce window.
    Quiesced,
    /// The timeout elapsed before the receive count settled.
    TimedOut,
}

/// Block until multicast replication has drained.
///
/// When the send loop ends, replicated copies of the final requests are still
/// in flight. Rather than clip the tail at a fixed buffer, poll the aggregate
/// receive count and return `DrainResult::Quiesced` once it has stopped
/// advancing for `quiesce`, so every replicated reply is counted. `timeout`
/// bounds the wait so a member that never quiesces (for example a steady
/// duplicate storm) cannot hang the test; that exit returns
/// `DrainResult::TimedOut`.
fn drain_until_quiescent<K, T>(
    targets: &Mutex<BTreeMap<K, PingState<T>>>,
    quiesce: Duration,
    timeout: Duration,
) -> DrainResult {
    let total_rx = |t: &BTreeMap<K, PingState<T>>| -> u64 {
        t.values().map(|state| u64::from(state.rx_count)).sum()
    };
    let deadline = Instant::now() + timeout;
    let mut last = total_rx(&targets.lock().unwrap());
    let mut stable_since = Instant::now();
    loop {
        sleep(Duration::from_millis(100));

        if Instant::now() >= deadline {
            return DrainResult::TimedOut;
        }
        let now = total_rx(&targets.lock().unwrap());
        if now != last {
            last = now;
            stable_since = Instant::now();
        } else if stable_since.elapsed() >= quiesce {
            return DrainResult::Quiesced;
        }
    }
}

/// A multicast dataplane pinger, abstracted over the group's address family so
/// `mcast_ping_test_run` can drive the IPv4 and IPv6 paths with one body.
///
/// Implementors own a raw socket and the per-member accounting map. The family
/// specific pieces (socket setup, on-wire framing, source extraction) live in
/// `new`, `tx`, and `egress_source`. The live ticker and loss bookkeeping are
/// shared as default methods over `targets`.
trait McastPinger: Send + Sync + 'static {
    /// The address family of the group and its members.
    type Addr: Copy + Ord + Display;

    /// Build a pinger for `group`, stamping every request with `id`, and spawn
    /// its receive and loss-counting workers. `ttl` raises the multicast egress
    /// limit (IPv4 `IP_MULTICAST_TTL` or IPv6 `IPV6_MULTICAST_HOPS`) so requests
    /// traverse the rack underlay to remote members.
    fn new(ttl: u32, group: Self::Addr, id: u16) -> Arc<Self>;

    /// Per-member reply accounting, keyed by responder source address.
    fn targets(&self) -> &Mutex<BTreeMap<Self::Addr, PingState<Self::Addr>>>;

    /// Spawn the transmit worker: send one echo stream to the group at `pps`
    /// for `duration`, recording each send instant against every member.
    fn tx(self: Arc<Self>, pps: usize, duration: Duration);

    /// Determine the source address the raw stream egresses from, or `None` on
    /// route-lookup failure.
    fn egress_source(group: Self::Addr) -> Option<Self::Addr>;

    /// Wire copies of our own requests received back so far; see
    /// [`McastReport::sender_self_rx`].
    fn self_rx(&self) -> u32;

    /// Spawn the live ticker that repaints a status row per member.
    fn show(self: Arc<Self>) {
        print_ping_header();
        spawn(move || {
            loop {
                for (_src, t) in self.targets().lock().unwrap().iter() {
                    print_ping_row(t);
                }
                for _ in 0..self.targets().lock().unwrap().len() {
                    print!("{MOVE_CURSOR_UP}");
                }
                print!("\r");
                sleep(Duration::from_millis(100));
            }
        });
    }

    /// Spawn the worker that recomputes per-member loss from the running tx/rx
    /// counts.
    fn count_lost(self: Arc<Self>) {
        spawn(move || {
            loop {
                for (_, tgt) in self.targets().lock().unwrap().iter_mut() {
                    update_lost_packet_count(tgt);
                }
                sleep(Duration::from_millis(10));
            }
        });
    }
}

/// Drive a multicast dataplane ping test against `group`, generic over the
/// pinger's address family.
///
/// A single ICMP echo stream is stamped with one random identifier and sent to
/// the group at `pps` for `duration`. The rack replicates each request to the
/// joined members, whose kernels reply unicast. `expected_members` seeds the
/// per-member report so a member that never replies surfaces with
/// `rx_count == 0`. Unexpected responders are added as they appear. After the
/// send window the count is drained to quiescence so late replicated copies are
/// tallied before the report is built.
fn mcast_ping_test_run<P: McastPinger>(
    group: P::Addr,
    expected_members: &[P::Addr],
    ttl: u32,
    pps: usize,
    duration: Duration,
) -> McastReport<P::Addr> {
    let pinger = P::new(ttl, group, rand::random());
    {
        let mut targets = pinger.targets().lock().unwrap();
        for member in expected_members {
            targets.insert(*member, PingState::new(*member));
        }
    }
    // Hide the blinking cursor while the live ticker runs.
    print!("{HIDE_CURSOR}");
    pinger.clone().show();
    pinger.clone().tx(pps, duration);
    // Let the send window elapse, then keep counting until replication drains.
    // The final requests' replicated copies are still in flight at the deadline,
    // so we finish only once the receive count stops advancing rather than
    // clipping the tail at a fixed buffer.
    sleep(duration);
    assert_eq!(
        drain_until_quiescent(
            pinger.targets(),
            Duration::from_millis(500),
            Duration::from_secs(5),
        ),
        DrainResult::Quiesced,
        "multicast replies for group {group} did not quiesce within the \
         drain timeout; the report may be missing late replies or duplicate \
         delivery is ongoing",
    );
    for _ in 0..pinger.targets().lock().unwrap().len() {
        println!();
    }
    print!("{SHOW_CURSOR}");

    let members = pinger
        .targets()
        .lock()
        .unwrap()
        .values()
        .copied()
        .map(McastMember::from)
        .collect();
    McastReport {
        group,
        sender: P::egress_source(group),
        members,
        sender_self_rx: pinger.self_rx(),
    }
}

/// Run a multicast dataplane ping test against `group`.
///
/// A single ICMP echo stream is sent to the multicast group address at `pps`
/// for `duration`. The rack replicates each request to the joined members,
/// and replies are tallied by the responder source address rather than ICMP
/// identifer, as every member answers with the same ID the request carried, so
/// the source address is the only discriminator. `expected_members` seeds the
/// per-member report so a member that never replies surfaces with
/// `rx_count == 0`.  Unexpected responders are added as they appear.
///
/// The reply is generated by each member's kernel ICMP responder, not by any
/// userspace listener: membership (held open by the in-zone joiner) only makes
/// the kernel accept the group's packets, after which the kernel echoes. This
/// relies on illumos responding to multicast echo by default
/// (`ip_respond_to_echo_multicast=1`). Linux takes the opposite default
/// (`net.ipv4.icmp_echo_ignore_broadcasts=1`), so a Linux member would join the
/// group yet never reply, surfacing here as a silent `rx_count == 0`.
///
/// The reply is unicast back to the sender, so a counted `rx` witnesses the
/// full round trip: multicast egress to the member and the unicast return. A
/// clean run confirms both directions. A member that stops replying does not
/// localize the fault to egress, since a broken return path looks identical.
/// Pair this with a switch-side replication counter to attribute a failure to
/// egress alone.
pub fn mcast_ping4_test_run(
    group: Ipv4Addr,
    expected_members: &[Ipv4Addr],
    ttl: u32,
    pps: usize,
    duration: Duration,
) -> McastReport<Ipv4Addr> {
    mcast_ping_test_run::<McastPinger4>(
        group,
        expected_members,
        ttl,
        pps,
        duration,
    )
}

/// Determine the IPv4 source address the raw multicast stream egresses from.
///
/// The raw ICMP socket lets the kernel pick the source via a route lookup on the
/// group. A throwaway UDP `connect` to the group runs the same lookup and binds
/// the source that `local_addr` reports. No datagram is sent.
///
/// Returns `None` on lookup failure, surfaced as a null sender rather than
/// aborting the test.
fn egress_source_v4(group: Ipv4Addr) -> Option<Ipv4Addr> {
    let sock = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    sock.connect((group, 9)).ok()?;
    match sock.local_addr().ok()?.ip() {
        IpAddr::V4(addr) => Some(addr),
        IpAddr::V6(_) => None,
    }
}

/// Set `IP_MULTICAST_TTL` on a raw IPv4 socket.
///
/// illumos describes this option as a single byte (`uchar_t`) in `ip(4P)`,
/// and its raw ICMP option table uses `sizeof (uchar_t)` for
/// `IP_MULTICAST_TTL`. Fixed-length option validation requires the supplied
/// length to match that table entry exactly.
///
/// socket2's `set_multicast_ttl_v4` passes a four-byte `c_int`, which
/// the illumos kernel rejects with `EINVAL`. There the option is set directly as
/// a `u8`. Other platforms accept the `c_int` form, so the socket2 path is used
/// unchanged.
#[cfg(target_os = "illumos")]
fn set_multicast_ttl_v4(sock: &Socket, ttl: u32) {
    use std::os::fd::AsRawFd;
    let ttl = ttl as u8;
    let ret = unsafe {
        libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_MULTICAST_TTL,
            std::ptr::from_ref(&ttl).cast(),
            std::mem::size_of::<u8>() as libc::socklen_t,
        )
    };
    if ret != 0 {
        panic!("set IP_MULTICAST_TTL: {}", std::io::Error::last_os_error());
    }
}

#[cfg(not(target_os = "illumos"))]
fn set_multicast_ttl_v4(sock: &Socket, ttl: u32) {
    sock.set_multicast_ttl_v4(ttl).unwrap();
}

/// Set `IP_MULTICAST_LOOP` on a raw IPv4 socket.
///
/// Like `IP_MULTICAST_TTL` above, illumos types this option as a single byte
/// (`uchar_t`) and rejects socket2's four-byte `c_int` form with `EINVAL`, so
/// it is set directly as a `u8` there. Other platforms take the socket2 path
/// unchanged.
#[cfg(target_os = "illumos")]
fn set_multicast_loop_v4(sock: &Socket, on: bool) {
    use std::os::fd::AsRawFd;
    let val = u8::from(on);
    let ret = unsafe {
        libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_MULTICAST_LOOP,
            std::ptr::from_ref(&val).cast(),
            std::mem::size_of::<u8>() as libc::socklen_t,
        )
    };
    if ret != 0 {
        panic!("set IP_MULTICAST_LOOP: {}", std::io::Error::last_os_error());
    }
}

#[cfg(not(target_os = "illumos"))]
fn set_multicast_loop_v4(sock: &Socket, on: bool) {
    sock.set_multicast_loop_v4(on).unwrap();
}

struct McastPinger4 {
    sock: Socket,
    /// ICMP identifier stamped on every request and matched on replies.
    id: u16,
    /// Multicast group address requests are sent to.
    group: Ipv4Addr,
    /// Per-member reply accounting, keyed by responder source address.
    targets: Mutex<BTreeMap<Ipv4Addr, Ping4State>>,
    /// Wire copies of our own requests received back; see
    /// [`McastReport::sender_self_rx`].
    self_rx: AtomicU32,
}

impl McastPinger4 {
    fn rx(self: Arc<Self>) {
        spawn(move || {
            let egress = Self::egress_source(self.group);
            loop {
                let mut ubuf = [MaybeUninit::new(0); 10240];
                if let Ok((sz, from)) = self.sock.recv_from(&mut ubuf) {
                    let buf = unsafe { ubuf[..sz].assume_init_ref() };
                    let msg: EchoRequest =
                        match ispf::from_bytes_be(&buf[IPV4_HEADER_LEN..sz]) {
                            Ok(msg) => msg,
                            Err(_) => continue,
                        };
                    let src = from
                        .as_socket_ipv4()
                        .map(|socket_addr| *socket_addr.ip());
                    // With multicast loopback disabled and the group joined as
                    // a listener, an echo request carrying our identifier can
                    // only have arrived from the wire: the dataplane delivered
                    // the sender's own stream back to it. Requiring the source
                    // to be our own egress address discards identifier
                    // collisions with another host's request stream.
                    if msg.typ == ICMP_ECHO_TYPE
                        && msg.identifier == self.id
                        && egress.map_or(true, |e| src == Some(e))
                    {
                        self.self_rx.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let Some(src) = classify_mcast_reply(
                        msg.typ,
                        ICMP_ECHO_REPLY_TYPE,
                        msg.identifier,
                        self.id,
                        src,
                    ) else {
                        continue;
                    };

                    self.targets
                        .lock()
                        .unwrap()
                        .entry(src)
                        .or_insert_with(|| Ping4State::new(src))
                        .record_reply(Instant::now());
                }
            }
        });
    }
}

impl McastPinger for McastPinger4 {
    type Addr = Ipv4Addr;

    fn new(ttl: u32, group: Ipv4Addr, id: u16) -> Arc<Self> {
        let sock = Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::ICMPV4))
            .unwrap();
        sock.set_ttl(ttl).unwrap();
        // Multicast egress is governed by IP_MULTICAST_TTL (default 1). Raise
        // it so requests traverse the rack underlay to remote members.
        set_multicast_ttl_v4(&sock, ttl);
        // Join the group as a local listener so the NIC and IP stack accept
        // group-destined frames, giving the sender a vantage on its own
        // stream coming back from the wire. This is a kernel-level join on
        // the egress interface, not a rack membership. Loopback is disabled
        // so the only self-copies that can arrive are wire deliveries, which
        // `rx` counts as `self_rx` violations.
        sock.join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED).unwrap();
        set_multicast_loop_v4(&sock, false);
        let s = Arc::new(Self {
            sock,
            id,
            group,
            targets: Mutex::new(BTreeMap::new()),
            self_rx: AtomicU32::new(0),
        });
        s.clone().rx();
        s.clone().count_lost();
        s
    }

    fn targets(&self) -> &Mutex<BTreeMap<Ipv4Addr, Ping4State>> {
        &self.targets
    }

    fn tx(self: Arc<Self>, pps: usize, duration: Duration) {
        let interval = Duration::from_secs_f64(1.0 / pps as f64);
        // `seq` is the 16-bit ICMP sequence number and is allowed to wrap.
        // `tx_total` is the true count of packets sent and must not, so it is a
        // separate wide counter.
        let mut seq = 0u16;
        let mut tx_total: u32 = 0;
        let stop = Instant::now() + duration;
        let dst: SockAddr = SocketAddrV4::new(self.group, 0).into();
        spawn(move || {
            loop {
                if Instant::now() >= stop {
                    break;
                }
                let mut c = Checksum::new();
                c.add_bytes(&[ICMP_ECHO_TYPE, ICMP_ECHO_CODE]);
                c.add_bytes(&self.id.to_be_bytes());
                c.add_bytes(&seq.to_be_bytes());
                let pkt = EchoRequest {
                    typ: ICMP_ECHO_TYPE,
                    code: ICMP_ECHO_CODE,
                    checksum: u16::from_be_bytes(c.checksum()),
                    identifier: self.id,
                    sequence_number: seq,
                };
                let msg = ispf::to_bytes_be(&pkt).unwrap();

                // The transmit clock is shared across members: one request is
                // replicated to all, so each member's round trip is measured
                // from the same send instant.
                tx_total += 1;
                {
                    let now = Instant::now();
                    let mut targets = self.targets.lock().unwrap();
                    for tgt in targets.values_mut() {
                        tgt.record_sent(now, tx_total);
                    }
                }
                self.sock.send_to(&msg, &dst).unwrap();

                seq = seq.wrapping_add(1);
                sleep(interval);
            }
        });
    }

    fn egress_source(group: Ipv4Addr) -> Option<Ipv4Addr> {
        egress_source_v4(group)
    }

    fn self_rx(&self) -> u32 {
        self.self_rx.load(Ordering::Relaxed)
    }
}

/// Run a multicast dataplane ping test against an IPv6 `group`.
///
/// The IPv6 sibling of `mcast_ping4_test_run`. A single ICMPv6 echo stream is
/// sent to the multicast group at `pps` for `duration`. The rack replicates
/// each request to the joined members and replies are tallied by responder
/// source address. `expected_members` seeds the per-member report so a member
/// that never replies surfaces with `rx_count == 0`. Unexpected responders are
/// added as they appear.
///
/// As with the IPv4 path, the reply comes from each member's kernel ICMPv6
/// responder, not the in-zone joiner that's applied.
///
/// Membership only makes the kernel accept the group's packets. illumos echoes
/// multicast pings by default, so a member on a stack that suppresses multicast
/// echo replies would join yet still stay silent.
///
/// The reply is unicast back to the sender, so a counted `rx` witnesses the full
/// round trip: multicast egress to the member and the unicast return. A clean run
/// confirms both directions. A member that stops replying does not localize the
/// fault to egress, since a broken return path looks identical. Pair this with a
/// switch-side replication counter to attribute a failure to egress alone.
pub fn mcast_ping6_test_run(
    group: Ipv6Addr,
    expected_members: &[Ipv6Addr],
    hops: u32,
    pps: usize,
    duration: Duration,
) -> McastReport<Ipv6Addr> {
    mcast_ping_test_run::<McastPinger6>(
        group,
        expected_members,
        hops,
        pps,
        duration,
    )
}

/// Determine the IPv6 source address the raw multicast stream egresses from.
///
/// The IPv6 sibling of `egress_source_v4`: a throwaway UDP `connect` to the
/// group runs the same kernel route lookup the raw socket uses and binds the
/// source that `local_addr` reports. No datagram is sent.
///
/// Returns `None` on lookup failure, surfaced as a null sender rather than
/// aborting the test.
fn egress_source_v6(group: Ipv6Addr) -> Option<Ipv6Addr> {
    let sock = UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0)).ok()?;
    sock.connect((group, 9)).ok()?;
    match sock.local_addr().ok()?.ip() {
        IpAddr::V6(addr) => Some(addr),
        IpAddr::V4(_) => None,
    }
}

struct McastPinger6 {
    sock: Socket,
    /// ICMP identifier stamped on every request and matched on replies.
    id: u16,
    /// Multicast group address requests are sent to.
    group: Ipv6Addr,
    /// Per-member reply accounting, keyed by responder source address.
    targets: Mutex<BTreeMap<Ipv6Addr, Ping6State>>,
    /// Wire copies of our own requests received back; see
    /// [`McastReport::sender_self_rx`].
    self_rx: AtomicU32,
}

impl McastPinger6 {
    fn rx(self: Arc<Self>) {
        spawn(move || {
            let egress = Self::egress_source(self.group);
            loop {
                let mut ubuf = [MaybeUninit::new(0); 10240];
                if let Ok((sz, from)) = self.sock.recv_from(&mut ubuf) {
                    let buf = unsafe { ubuf[..sz].assume_init_ref() };
                    // IPv6 raw sockets never deliver the IPv6 header, so the
                    // ICMPv6 message starts at offset 0 (unlike the IPv4 path,
                    // which skips the leading IPv4 header).
                    let msg: EchoRequest = match ispf::from_bytes_be(&buf[..sz])
                    {
                        Ok(msg) => msg,
                        Err(_) => continue,
                    };
                    let src = from
                        .as_socket_ipv6()
                        .map(|socket_addr| *socket_addr.ip());
                    // With multicast loopback disabled and the group joined as
                    // a listener, an echo request carrying our identifier can
                    // only have arrived from the wire: the dataplane delivered
                    // the sender's own stream back to it. Requiring the source
                    // to be our own egress address discards identifier
                    // collisions with another host's request stream.
                    if msg.typ == ICMPV6_ECHO_TYPE
                        && msg.identifier == self.id
                        && egress.map_or(true, |e| src == Some(e))
                    {
                        self.self_rx.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let Some(src) = classify_mcast_reply(
                        msg.typ,
                        ICMPV6_ECHO_REPLY_TYPE,
                        msg.identifier,
                        self.id,
                        src,
                    ) else {
                        continue;
                    };

                    self.targets
                        .lock()
                        .unwrap()
                        .entry(src)
                        .or_insert_with(|| Ping6State::new(src))
                        .record_reply(Instant::now());
                }
            }
        });
    }
}

impl McastPinger for McastPinger6 {
    type Addr = Ipv6Addr;

    fn new(hops: u32, group: Ipv6Addr, id: u16) -> Arc<Self> {
        let sock = Socket::new(Domain::IPV6, Type::RAW, Some(Protocol::ICMPV6))
            .unwrap();
        // Multicast egress is governed by IPV6_MULTICAST_HOPS (default 1). Raise
        // it so requests traverse the rack underlay to remote members. Unlike
        // the IPv4 TTL this is the standard four-byte `c_int` form on every
        // platform, so socket2's helper works on illumos without a libc shim.
        sock.set_multicast_hops_v6(hops).unwrap();
        // Join the group as a local listener and disable loopback, mirroring
        // the IPv4 path: any echo request of ours that arrives came from the
        // wire, and `rx` counts it as a `self_rx` violation. Interface index
        // zero defers interface selection to the kernel's route lookup, the
        // same one the raw stream egresses by. Both options take the standard
        // forms on every platform, so no illumos shim is needed here.
        sock.join_multicast_v6(&group, 0).unwrap();
        sock.set_multicast_loop_v6(false).unwrap();
        let s = Arc::new(Self {
            sock,
            id,
            group,
            targets: Mutex::new(BTreeMap::new()),
            self_rx: AtomicU32::new(0),
        });
        s.clone().rx();
        s.clone().count_lost();
        s
    }

    fn targets(&self) -> &Mutex<BTreeMap<Ipv6Addr, Ping6State>> {
        &self.targets
    }

    fn tx(self: Arc<Self>, pps: usize, duration: Duration) {
        let interval = Duration::from_secs_f64(1.0 / pps as f64);
        // `seq` is the 16-bit ICMP sequence number and is allowed to wrap.
        // `tx_total` is the true count of packets sent and must not, so it is a
        // separate wide counter.
        let mut seq = 0u16;
        let mut tx_total: u32 = 0;
        let stop = Instant::now() + duration;
        let dst: SockAddr = SocketAddrV6::new(self.group, 0, 0, 0).into();
        spawn(move || {
            loop {
                if Instant::now() >= stop {
                    break;
                }
                // The ICMPv6 checksum covers an IPv6 pseudo-header the sender
                // does not assemble. Per RFC 3542 the kernel computes and
                // inserts it for IPPROTO_ICMPV6 raw sockets, so the field is
                // left zero here.
                let pkt = EchoRequest {
                    typ: ICMPV6_ECHO_TYPE,
                    code: ICMPV6_ECHO_CODE,
                    checksum: 0,
                    identifier: self.id,
                    sequence_number: seq,
                };
                let msg = ispf::to_bytes_be(&pkt).unwrap();

                // The transmit clock is shared across members: one request is
                // replicated to all, so each member's round trip is measured
                // from the same send instant.
                tx_total += 1;
                {
                    let now = Instant::now();
                    let mut targets = self.targets.lock().unwrap();
                    for tgt in targets.values_mut() {
                        tgt.record_sent(now, tx_total);
                    }
                }
                self.sock.send_to(&msg, &dst).unwrap();

                seq = seq.wrapping_add(1);
                sleep(interval);
            }
        });
    }

    fn egress_source(group: Ipv6Addr) -> Option<Ipv6Addr> {
        egress_source_v6(group)
    }

    fn self_rx(&self) -> u32 {
        self.self_rx.load(Ordering::Relaxed)
    }
}

/// A source address that may legitimately appear on an echo reply.
///
/// An Echo Reply to a multicast-destined Echo Request must be sourced from a
/// unicast address of the responding interface, never the group address. For
/// ICMPv6 this is mandated by RFC 4443 section 4.2. For ICMPv4, RFC 1122
/// section 3.2.2.6 allows any host to answer a broadcast or multicast Echo
/// Request, and the reply carries that host's own unicast address. A
/// noncompliant responder that instead sources the reply from the group address
/// would otherwise be tallied against the group rather than against a member,
/// corrupting per-member accounting. Enforcing unicast sources also discards a
/// looped-back copy of our own multicast-destined request.
trait ReplySource {
    fn is_valid_reply_source(&self) -> bool;
}

impl ReplySource for Ipv4Addr {
    fn is_valid_reply_source(&self) -> bool {
        !self.is_multicast() && !self.is_unspecified()
    }
}

impl ReplySource for Ipv6Addr {
    fn is_valid_reply_source(&self) -> bool {
        !self.is_multicast() && !self.is_unspecified()
    }
}

/// Decide whether a received ICMP datagram is a member reply for our echo
/// stream, returning the responder source on acceptance.
///
/// A multicast sender may observe copies of its own outgoing requests (for
/// example when `IP_MULTICAST_LOOP` is enabled). Echo requests share the echo
/// header layout, so acceptance is gated on the message being an echo reply
/// (`expected_reply_type`, which differs between ICMPv4 and ICMPv6) that carries
/// our stream `identifier`. A reply without a resolvable source, or one whose
/// source is not a unicast address (per RFC 1122 section 3.2.2.6 for ICMPv4 and
/// RFC 4443 section 4.2 for ICMPv6), is dropped. The caller supplies the
/// family-specific reply type so the same logic serves both IPv4 and IPv6
/// streams.
fn classify_mcast_reply<T: ReplySource>(
    typ: u8,
    expected_reply_type: u8,
    identifier: u16,
    expected_id: u16,
    src: Option<T>,
) -> Option<T> {
    if typ != expected_reply_type {
        return None;
    }
    if identifier != expected_id {
        return None;
    }
    src.filter(ReplySource::is_valid_reply_source)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: u16 = 0x1234;
    const SRC: Ipv4Addr = Ipv4Addr::new(192, 168, 1, 10);
    const SRC6: Ipv6Addr = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0x10);

    #[test]
    fn echo_reply_for_stream_is_accepted() {
        assert_eq!(
            classify_mcast_reply(
                ICMP_ECHO_REPLY_TYPE,
                ICMP_ECHO_REPLY_TYPE,
                ID,
                ID,
                Some(SRC)
            ),
            Some(SRC),
        );
    }

    #[test]
    fn echo_request_is_rejected() {
        // A looped-back copy of our own request carries our identifier but is
        // type 8 (echo request), so it must not be tallied as a member reply.
        assert_eq!(
            classify_mcast_reply(
                ICMP_ECHO_TYPE,
                ICMP_ECHO_REPLY_TYPE,
                ID,
                ID,
                Some(SRC)
            ),
            None,
        );
    }

    #[test]
    fn reply_from_other_stream_is_rejected() {
        assert_eq!(
            classify_mcast_reply(
                ICMP_ECHO_REPLY_TYPE,
                ICMP_ECHO_REPLY_TYPE,
                ID ^ 0xffff,
                ID,
                Some(SRC)
            ),
            None,
        );
    }

    #[test]
    fn reply_without_ipv4_source_is_rejected() {
        assert_eq!(
            classify_mcast_reply::<Ipv4Addr>(
                ICMP_ECHO_REPLY_TYPE,
                ICMP_ECHO_REPLY_TYPE,
                ID,
                ID,
                None
            ),
            None,
        );
    }

    #[test]
    fn reply_from_multicast_source_is_rejected() {
        // An echo reply's source must be unicast (RFC 1122 section 3.2.2.6 for
        // ICMPv4, RFC 4443 section 4.2 for ICMPv6). A group-sourced reply is
        // noncompliant and must not be tallied against a member.
        assert_eq!(
            classify_mcast_reply(
                ICMP_ECHO_REPLY_TYPE,
                ICMP_ECHO_REPLY_TYPE,
                ID,
                ID,
                Some(Ipv4Addr::new(239, 100, 0, 1)),
            ),
            None,
        );
        assert_eq!(
            classify_mcast_reply(
                ICMPV6_ECHO_REPLY_TYPE,
                ICMPV6_ECHO_REPLY_TYPE,
                ID,
                ID,
                Some(Ipv6Addr::new(0xff0e, 0, 0, 0, 0, 0, 0, 1)),
            ),
            None,
        );
    }

    #[test]
    fn v6_echo_reply_for_stream_is_accepted() {
        assert_eq!(
            classify_mcast_reply(
                ICMPV6_ECHO_REPLY_TYPE,
                ICMPV6_ECHO_REPLY_TYPE,
                ID,
                ID,
                Some(SRC6)
            ),
            Some(SRC6),
        );
    }

    #[test]
    fn v6_echo_request_is_rejected() {
        // The looped-back v6 request is type 128 (echo request), not the 129
        // reply type, so it must not be tallied as a member reply.
        assert_eq!(
            classify_mcast_reply(
                ICMPV6_ECHO_TYPE,
                ICMPV6_ECHO_REPLY_TYPE,
                ID,
                ID,
                Some(SRC6)
            ),
            None,
        );
    }
}
