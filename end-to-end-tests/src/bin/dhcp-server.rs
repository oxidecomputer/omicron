//! This is a dirt simple DHCP server for handing out addresses in a given
//! range. Leases do not expire. If the server runs out of addresses, it
//! panics. This is a stopgap program to hand out addresses to VMs in CI. It's
//! in no way meant to be a generic DHCP server solution.

use anyhow::Result;
use clap::Parser;
use dhcproto::{
    v4::{
        self, Decodable, Decoder, DhcpOptions, Encodable, Message, Opcode,
        OptionCode,
    },
    Encoder,
};
use end_to_end_tests::helpers::cli::oxide_cli_style;
use macaddr::MacAddr6;
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4, UdpSocket},
};

#[derive(Parser, Debug)]
#[clap(version, about, long_about = None, styles = oxide_cli_style())]
struct Cli {
    /// First address in DHCP range.
    begin: Ipv4Addr,
    /// Last address in DHCP range.
    end: Ipv4Addr,
    /// Default router to advertise.
    router: Ipv4Addr,
    /// Server address to advertise.
    server: Ipv4Addr,
}

pub fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut current = cli.begin;
    let mut assignments = HashMap::<MacAddr6, Ipv4Addr>::new();

    let sock = UdpSocket::bind("0.0.0.0:67")?;
    loop {
        let mut buf = [0; 8192];
        let (n, src) = sock.recv_from(&mut buf)?;

        let mut msg = match Message::decode(&mut Decoder::new(&buf[..n])) {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("message decode error {e}");
                continue;
            }
        };

        println!("request: {msg:#?}");

        if msg.opcode() != Opcode::BootRequest {
            continue;
        }

        let mac: [u8; 6] = msg.chaddr()[0..6].try_into().unwrap();
        let mac = MacAddr6::from(mac);

        let ip = match assignments.get(&mac) {
            Some(ip) => *ip,
            None => {
                assignments.insert(mac, current);
                let ip = current;
                current = Ipv4Addr::from(u32::from(current) + 1);
                if u32::from(current) > u32::from(cli.end) {
                    panic!("address exhaustion");
                }
                ip
            }
        };

        let mut opts = DhcpOptions::new();
        match msg.opts().get(OptionCode::MessageType) {
            Some(v4::DhcpOption::MessageType(v4::MessageType::Discover)) => {
                opts.insert(v4::DhcpOption::MessageType(
                    v4::MessageType::Offer,
                ));
            }
            Some(v4::DhcpOption::MessageType(v4::MessageType::Request)) => {
                opts.insert(v4::DhcpOption::MessageType(v4::MessageType::Ack));
            }
            Some(mtype) => eprintln!("unexpected message type {mtype:?}"),
            None => {
                eprintln!("no message type");
            }
        };
        // hardcode to /24
        opts.insert(v4::DhcpOption::SubnetMask(Ipv4Addr::new(
            255, 255, 255, 0,
        )));
        // hardcode to something stable
        opts.insert(v4::DhcpOption::DomainNameServer(vec![Ipv4Addr::new(
            1, 1, 1, 1,
        )]));
        opts.insert(v4::DhcpOption::ServerIdentifier(cli.server));
        // just something big enough to last CI runs
        opts.insert(v4::DhcpOption::AddressLeaseTime(60 * 60 * 24 * 30));
        opts.insert(v4::DhcpOption::Router(vec![cli.router]));
        if let Some(opt) = msg.opts().get(OptionCode::ClientIdentifier) {
            opts.insert(opt.clone());
        }

        msg.set_opcode(Opcode::BootReply);
        msg.set_siaddr(cli.server);
        msg.set_yiaddr(ip);
        msg.set_opts(opts);

        let mut buf = Vec::new();
        let mut e = Encoder::new(&mut buf);
        if let Err(e) = msg.encode(&mut e) {
            eprintln!("encode reply error: {e}");
            continue;
        }

        // always blast replys bcast
        let dst =
            SocketAddrV4::new(Ipv4Addr::new(255, 255, 255, 255), src.port());
        if let Err(e) = sock.send_to(&buf, dst) {
            eprintln!("send reply error: {e}");
        }
    }
}
