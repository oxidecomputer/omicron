// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use colored::*;
use internet_checksum::Checksum;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::collections::BTreeMap;
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};

const HIDE_CURSOR: &str = "\x1b[?25l";
const SHOW_CURSOR: &str = "\x1b[?25h";
const MOVE_CURSOR_UP: &str = "\x1b[A";

const ICMP_ECHO_TYPE: u8 = 8;
const ICMP_ECHO_CODE: u8 = 0;
const ICMP_ECHO_REPLY_TYPE: u8 = 0;

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
        // use a random number for the ICMP id
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

/// Running results for an IPv4 ping test.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Ping4State {
    /// Destination address of the ping test.
    pub dest: Ipv4Addr,
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
    /// The number of packets sent.
    pub tx_count: u16,
    /// The number of packets received.
    pub rx_count: u16,
    /// The last time a packet was sent.
    #[serde(skip)]
    pub sent: Option<Instant>,
    /// The transmit counter value when we received the first reply.
    #[serde(skip)]
    pub first: u16,
}

impl Ping4State {
    fn new(addr: Ipv4Addr) -> Self {
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
        let mut seq = 0u16;
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
                        tgt.sent = Some(Instant::now());
                        tgt.tx_count = seq;
                        let sa: SockAddr = SocketAddrV4::new(dst, 0).into();
                        self.sock.send_to(&msg, &sa).unwrap();
                    }
                    None => continue,
                }

                seq += 1;
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
                    update_lost(tgt);
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
                    let buf = unsafe { &slice_assume_init_ref(&ubuf[..sz]) };
                    let msg: EchoRequest =
                        match ispf::from_bytes_be(&buf[20..sz]) {
                            Ok(msg) => msg,
                            Err(_) => {
                                continue;
                            }
                        };
                    // correlate the ICMP id with a target
                    match self.targets.lock().unwrap().get_mut(&msg.identifier)
                    {
                        Some(ref mut target) => match target.sent {
                            Some(ref mut sent) => {
                                let t1 = Instant::now();
                                let dt = t1 - *sent;
                                target.current = Some(dt);
                                if target.low == Duration::ZERO
                                    || dt < target.low
                                {
                                    target.low = dt;
                                }
                                if dt > target.high {
                                    target.high = dt;
                                }
                                target.sum += dt;
                                target.current = Some(dt);
                                target.rx_count += 1;
                                if target.first == 0 {
                                    target.first = target.tx_count;
                                }
                            }
                            None => {
                                println!("no sent");
                            }
                        },
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
fn print_ping_row(t: &Ping4State) {
    println!(
        "{:15} {:7} {:7} {:7} {:7} {:7} {:9} {:<7}",
        t.dest.to_string().cyan(),
        format!("{:.3}", (t.low.as_micros() as f32 / 1000.0)),
        if t.rx_count == 0 {
            format!("{:.3}", 0.0)
        } else {
            format!(
                "{:.3}",
                (t.sum.as_micros() as f32 / 1000.0 / f32::from(t.rx_count))
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
fn update_lost(t: &mut Ping4State) {
    if t.first != 0 {
        t.lost = t.tx_count.saturating_sub(t.first).saturating_sub(t.rx_count)
            as usize;
    }
}

/// Running results for an IPv4 multicast data-plane test.
#[derive(Debug, Serialize, Deserialize)]
pub struct McastReport {
    /// The multicast group address that was pinged.
    pub group: Ipv4Addr,
    /// Per-member reply accounting, keyed by responder source address.
    pub members: Vec<Ping4State>,
}

/// Run a multicast data-plane ping test against `group`.
///
/// A single ICMP echo stream is sent to the multicast group address at `pps`
/// for `duration`. The rack replicates each request to the joined members,
/// and replies are tallied by responder source address rather than ICMP id:
/// every member answers with the same id the request carried, so the source
/// address is the only discriminator. `expected_members` seeds the per-member
/// report so a member that never replies surfaces with `rx_count == 0`.
/// Unexpected responders are added as they appear.
pub fn mcast_ping4_test_run(
    group: Ipv4Addr,
    expected_members: &[Ipv4Addr],
    ttl: u32,
    pps: usize,
    duration: Duration,
) -> McastReport {
    // A single ICMP id identifies this echo stream (members echo it verbatim).
    let p = McastPinger4::new(ttl, group, rand::random());
    {
        let mut targets = p.targets.lock().unwrap();
        for member in expected_members {
            targets.insert(*member, Ping4State::new(*member));
        }
    }
    // Hide the blinking cursor while the live ticker runs.
    print!("{HIDE_CURSOR}");
    p.clone().show();
    p.clone().tx(pps, duration);
    // Wait for the test to conclude plus a bit of buffer for in-flight packets.
    sleep(duration + Duration::from_millis(250));
    for _ in 0..p.targets.lock().unwrap().len() {
        println!();
    }
    print!("{SHOW_CURSOR}");

    let members = p.targets.lock().unwrap().values().copied().collect();
    McastReport { group, members }
}

struct McastPinger4 {
    sock: Socket,
    /// ICMP id stamped on every request and matched on replies.
    id: u16,
    /// Multicast group address requests are sent to.
    group: Ipv4Addr,
    /// Per-member reply accounting, keyed by responder source address.
    targets: Mutex<BTreeMap<Ipv4Addr, Ping4State>>,
}

impl McastPinger4 {
    fn new(ttl: u32, group: Ipv4Addr, id: u16) -> Arc<Self> {
        let sock = Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::ICMPV4))
            .unwrap();
        sock.set_ttl(ttl).unwrap();
        // Multicast egress is governed by IP_MULTICAST_TTL (default 1). Raise
        // it so requests traverse the rack underlay to remote members.
        sock.set_multicast_ttl_v4(ttl).unwrap();
        // IP_MULTICAST_LOOP is left at its default. As with a real multicast
        // sender, correctness comes from [`classify_mcast_reply`] discarding
        // anything that is not an echo reply for our stream, not from
        // suppressing loopback.
        let s = Arc::new(Self {
            sock,
            id,
            group,
            targets: Mutex::new(BTreeMap::new()),
        });
        s.clone().rx();
        s.clone().count_lost();
        s
    }

    fn show(self: Arc<Self>) {
        print_ping_header();
        spawn(move || {
            loop {
                for (_src, t) in self.targets.lock().unwrap().iter() {
                    print_ping_row(t);
                }
                for _ in 0..self.targets.lock().unwrap().len() {
                    print!("{MOVE_CURSOR_UP}");
                }
                print!("\r");
                sleep(Duration::from_millis(100));
            }
        });
    }

    fn count_lost(self: Arc<Self>) {
        spawn(move || {
            loop {
                for (_, tgt) in self.targets.lock().unwrap().iter_mut() {
                    update_lost(tgt);
                }
                sleep(Duration::from_millis(10));
            }
        });
    }

    fn tx(self: Arc<Self>, pps: usize, duration: Duration) {
        let interval = Duration::from_secs_f64(1.0 / pps as f64);
        let mut seq = 0u16;
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
                {
                    let now = Instant::now();
                    let mut targets = self.targets.lock().unwrap();
                    for tgt in targets.values_mut() {
                        tgt.sent = Some(now);
                        tgt.tx_count = seq;
                    }
                }
                self.sock.send_to(&msg, &dst).unwrap();

                seq += 1;
                sleep(interval);
            }
        });
    }

    fn rx(self: Arc<Self>) {
        spawn(move || {
            loop {
                let mut ubuf = [MaybeUninit::new(0); 10240];
                if let Ok((sz, from)) = self.sock.recv_from(&mut ubuf) {
                    let buf = unsafe { slice_assume_init_ref(&ubuf[..sz]) };
                    let msg: EchoRequest =
                        match ispf::from_bytes_be(&buf[20..sz]) {
                            Ok(msg) => msg,
                            Err(_) => continue,
                        };
                    let src = from.as_socket_ipv4().map(|sa| *sa.ip());
                    let Some(src) = classify_mcast_reply(
                        msg.typ,
                        msg.identifier,
                        self.id,
                        src,
                    ) else {
                        continue;
                    };

                    let mut targets = self.targets.lock().unwrap();
                    let target = targets
                        .entry(src)
                        .or_insert_with(|| Ping4State::new(src));
                    target.rx_count += 1;
                    if target.first == 0 {
                        target.first = target.tx_count;
                    }
                    if let Some(sent) = target.sent {
                        let dt = Instant::now() - sent;
                        target.current = Some(dt);
                        if target.low == Duration::ZERO || dt < target.low {
                            target.low = dt;
                        }
                        if dt > target.high {
                            target.high = dt;
                        }
                        target.sum += dt;
                    }
                }
            }
        });
    }
}

/// Decide whether a received ICMP datagram is a member reply for our echo
/// stream, returning the responder source on acceptance.
///
/// A multicast sender may observe copies of its own outgoing requests (for
/// example when `IP_MULTICAST_LOOP` is enabled). Echo requests share the echo
/// header layout, so acceptance is gated on the message being an echo reply
/// (type [`ICMP_ECHO_REPLY_TYPE`]) that carries our stream `identifier`. A
/// reply without a resolvable IPv4 source is dropped.
fn classify_mcast_reply(
    typ: u8,
    identifier: u16,
    expected_id: u16,
    src: Option<Ipv4Addr>,
) -> Option<Ipv4Addr> {
    if typ != ICMP_ECHO_REPLY_TYPE {
        return None;
    }
    if identifier != expected_id {
        return None;
    }
    src
}

// TODO: Use `MaybeUninit::slice_assume_init_ref` once it has stabilized
unsafe fn slice_assume_init_ref<T>(slice: &[MaybeUninit<T>]) -> &[T] {
    unsafe { &*(slice as *const [MaybeUninit<T>] as *const [T]) }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: u16 = 0x1234;
    const SRC: Ipv4Addr = Ipv4Addr::new(192, 168, 1, 10);

    #[test]
    fn echo_reply_for_our_stream_is_accepted() {
        assert_eq!(
            classify_mcast_reply(ICMP_ECHO_REPLY_TYPE, ID, ID, Some(SRC)),
            Some(SRC),
        );
    }

    #[test]
    fn own_echo_request_is_rejected() {
        // A looped-back copy of our own request carries our identifier but is
        // type 8 (echo request), so it must not be tallied as a member reply.
        assert_eq!(
            classify_mcast_reply(ICMP_ECHO_TYPE, ID, ID, Some(SRC)),
            None,
        );
    }

    #[test]
    fn reply_from_other_stream_is_rejected() {
        assert_eq!(
            classify_mcast_reply(
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
            classify_mcast_reply(ICMP_ECHO_REPLY_TYPE, ID, ID, None),
            None,
        );
    }
}
