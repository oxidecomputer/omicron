// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use gateway_messages::version;
use gateway_messages::Request;
use gateway_messages::RequestKind;
use gateway_messages::ResponseError;
use gateway_messages::ResponseKind;
use gateway_messages::SerializedSize;
use gateway_messages::SpMessage;
use gateway_messages::SpMessageKind;
use slog::debug;
use slog::o;
use slog::trace;
use slog::warn;
use slog::Drain;
use slog::Level;
use slog::Logger;
use std::net::SocketAddrV6;
use std::net::UdpSocket;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Command line program that can send MGS messages to a single SP.
#[derive(Parser, Debug)]
struct Args {
    #[clap(
        short,
        long,
        default_value = "info",
        value_parser = level_from_str,
        help = "Log level for MGS client",
    )]
    log_level: Level,

    #[clap(long)]
    sp: SocketAddrV6,

    #[clap(long, short, default_value = "3000")]
    timeout_millis: u64,

    #[clap(subcommand)]
    command: Commands,
}

fn level_from_str(s: &str) -> Result<Level> {
    if let Ok(level) = s.parse() {
        Ok(level)
    } else {
        bail!(format!("Invalid log level: {}", s))
    }
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Ask SP on which port it receives messages from us.
    Discover,

    /// Ask SP for its current state.
    State,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .build()
        .filter_level(args.log_level)
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!("component" => "faux-mgs"));

    let socket = UdpSocket::bind("[::]:0")
        .with_context(|| "failed to bind UDP socket")?;
    socket
        .set_read_timeout(Some(Duration::from_millis(args.timeout_millis)))
        .with_context(|| "failed to set read timeout on UDP socket")?;

    let request_kind = match args.command {
        Commands::Discover => RequestKind::Discover,
        Commands::State => RequestKind::SpState,
    };

    let response = request_response(&log, &socket, args.sp, request_kind)?;
    println!("{response:?}");

    Ok(())
}

fn request_response(
    log: &Logger,
    socket: &UdpSocket,
    addr: SocketAddrV6,
    kind: RequestKind,
) -> Result<Result<ResponseKind, ResponseError>> {
    let request_id = send_request(log, socket, addr, kind)?;
    loop {
        let message = recv_sp_message(log, socket)?;
        match message.kind {
            SpMessageKind::Response { request_id: response_id, result } => {
                if response_id != request_id {
                    warn!(
                        log, "ignoring unexpected response id";
                        "response_id" => response_id,
                    );
                    continue;
                }
                return Ok(result);
            }
            SpMessageKind::SerialConsole(_) => {
                debug!(log, "ignoring serial console packet from SP");
                continue;
            }
        }
    }
}

// On success, returns the request ID we sent.
fn send_request(
    log: &Logger,
    socket: &UdpSocket,
    addr: SocketAddrV6,
    kind: RequestKind,
) -> Result<u32> {
    static REQUEST_ID: AtomicU32 = AtomicU32::new(1);

    let version = version::V1;
    let request_id = REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let request = Request { version, request_id, kind };

    let mut buf = [0; Request::MAX_SIZE];
    debug!(
        log, "sending request to SP";
        "request" => ?request,
        "sp" => %addr,
    );
    let n = gateway_messages::serialize(&mut buf[..], &request).unwrap();
    socket
        .send_to(&buf[..n], addr)
        .with_context(|| format!("failed to send to {addr}"))?;

    Ok(request_id)
}

fn recv_sp_message(log: &Logger, socket: &UdpSocket) -> Result<SpMessage> {
    let mut resp = [0; SpMessage::MAX_SIZE];

    let (n, peer) = socket
        .recv_from(&mut resp[..])
        .with_context(|| format!("failed to receive response"))?;
    let resp = &resp[..n];
    trace!(log, "received packet"; "data" => ?resp);

    let (message, _leftover) = gateway_messages::deserialize::<SpMessage>(resp)
        .with_context(|| {
            format!("failed to deserialize response from {peer}")
        })?;
    debug!(log, "received response"; "response" => ?message);

    if message.version != version::V1 {
        bail!(
            "incorrect message versiom from {peer}: {} (expected {})",
            message.version,
            version::V1
        );
    }

    Ok(message)
}
