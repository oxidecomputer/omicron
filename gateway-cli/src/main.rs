// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use futures::future::Fuse;
use futures::future::FusedFuture;
use futures::prelude::*;
use gateway_client::types::SpType;
use gateway_client::types::UpdateBody;
use slog::o;
use slog::Drain;
use slog::Level;
use slog::Logger;
use std::borrow::Cow;
use std::fs;
use std::net::IpAddr;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::select;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;

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

    #[clap(
        short,
        long,
        value_parser = resolve_host,
        help = "IP address of MGS server",
    )]
    server: IpAddr,

    #[clap(short, long, help = "Port of MGS server", action)]
    port: u16,

    #[clap(long, help = "Target sled number", action)]
    sled: u32,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[clap(about = "Attach to serial console")]
    Attach {
        #[clap(long, help = "Put terminal in raw mode", action)]
        raw: bool,
    },
    #[clap(about = "Detach any existing serial console connection")]
    Detach,
    #[clap(about = "Update the SP or one of its componenets")]
    Update {
        #[clap(help = "Component name")]
        component: String,
        #[clap(help = "Update slot of the component")]
        slot: u16,
        #[clap(help = "Path to the new image")]
        path: PathBuf,
    },
    #[clap(about = "Reset the SP")]
    Reset,
}

fn level_from_str(s: &str) -> Result<Level> {
    if let Ok(level) = s.parse() {
        Ok(level)
    } else {
        bail!(format!("Invalid log level: {}", s))
    }
}

/// Given a string representing an host, attempts to resolve it to a specific IP
/// address
fn resolve_host(server: &str) -> Result<IpAddr> {
    (server, 0)
        .to_socket_addrs()?
        .map(|sock_addr| sock_addr.ip())
        .next()
        .ok_or_else(|| {
            anyhow!("failed to resolve server argument '{}'", server)
        })
}

struct Client {
    inner: gateway_client::Client,
    server: IpAddr,
    port: u16,
    sp_type: SpType,
    component: &'static str,
    sled: u32,
}

impl Client {
    fn new(server: IpAddr, port: u16, sled: u32, log: Logger) -> Self {
        // We currently hardcode `sled` and `sp3`, as that is the only
        // sptype+component pair that has a serial port.
        Self {
            inner: gateway_client::Client::new(
                &format!("http://{}:{}", server, port),
                log,
            ),
            server,
            port,
            sled,
            sp_type: SpType::Sled,
            component: "sp3",
        }
    }

    async fn reset(&self) -> Result<()> {
        self.inner.sp_reset(self.sp_type, self.sled).await?;
        Ok(())
    }

    async fn update(
        &self,
        component: &str,
        slot: u16,
        path: &Path,
    ) -> Result<()> {
        let image = fs::read(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        self.inner
            .sp_component_update(
                self.sp_type,
                self.sled,
                component,
                &UpdateBody { image, slot },
            )
            .await?;
        Ok(())
    }

    async fn detach(&self) -> Result<()> {
        self.inner
            .sp_component_serial_console_detach(
                self.sp_type,
                self.sled,
                self.component,
            )
            .await?;
        Ok(())
    }

    async fn attach(
        &self,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        // TODO should we be able to attach through the openapi client?
        let path = format!(
            "ws://{}:{}/sp/sled/{}/component/sp3/serial-console/attach",
            self.server, self.port, self.sled
        );
        let (ws, _response) = tokio_tungstenite::connect_async(path)
            .await
            .with_context(|| "failed to create serial websocket stream")?;
        Ok(ws)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .build()
        .filter_level(args.log_level)
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!("component" => "gateway-client"));

    let client = Client::new(args.server, args.port, args.sled, log);

    let term_raw = match args.command {
        Command::Attach { raw } => raw, // continue; primary use case
        Command::Detach => {
            return client.detach().await;
        }
        Command::Update { component, slot, path } => {
            return client.update(&component, slot, &path).await;
        }
        Command::Reset => {
            return client.reset().await;
        }
    };

    // TODO Much of the code from this point on is derived from propolis's CLI,
    // but isn't identical. Can/should we share it, or is this small enough that
    // having it duplicated is fine?

    let _raw_guard = if term_raw {
        Some(
            RawTermiosGuard::stdio_guard()
                .with_context(|| "failed to set raw mode")?,
        )
    } else {
        None
    };

    // https://docs.rs/tokio/latest/tokio/io/trait.AsyncReadExt.html#method.read_exact
    // is not cancel safe! Meaning reads from tokio::io::stdin are not cancel
    // safe. Spawn a separate task to read and put bytes onto this channel.
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);

    tokio::spawn(async move {
        let mut stdin = tokio::io::stdin();

        // next_raw must live outside loop, because Ctrl-A should work across
        // multiple inbuf reads.
        let mut next_raw = false;
        let mut inbuf = [0u8; 1024];

        // We're connected to a websocket which itself is forwarding data we
        // send it over UDP. If our terminal is in raw mode, our `stdin.read()`
        // below will happily return a single byte at a time, which would result
        // in a UDP packet for every byte read. Instead, we'll start a timer any
        // time we receive a byte and only flush it when that timer fires. This
        // introduces a human-noticeable delay, but results in overall better
        // performance. This mechanism is unused in non-raw mode where our
        // `stdin.read()` blocks until we get a newline.
        let flush_timeout = Fuse::terminated();
        futures::pin_mut!(flush_timeout);

        let mut exit = false;
        let mut outbuf = Vec::new();

        loop {
            let n = select! {
                read_result = stdin.read(&mut inbuf) => match read_result {
                    Err(_) | Ok(0) => if outbuf.is_empty() {
                        break;
                    } else {
                        exit = true;
                        continue;
                    }
                    Ok(n) => n,
                },

                () = &mut flush_timeout => {
                    tx.send(outbuf).await.unwrap();
                    if exit {
                        break;
                    } else {
                        outbuf = Vec::new();
                        continue;
                    }
                }
            };

            if term_raw {
                // Put bytes from inbuf to outbuf, but don't send Ctrl-A unless
                // next_raw is true.
                for i in 0..n {
                    let c = inbuf[i];
                    match c {
                        // Ctrl-A means send next one raw
                        b'\x01' => {
                            if next_raw {
                                // Ctrl-A Ctrl-A should be sent as Ctrl-A
                                outbuf.push(c);
                                next_raw = false;
                            } else {
                                next_raw = true;
                            }
                        }
                        b'\x03' => {
                            if !next_raw {
                                // Exit on non-raw Ctrl-C
                                exit = true;
                                break;
                            } else {
                                // Otherwise send Ctrl-C
                                outbuf.push(c);
                                next_raw = false;
                            }
                        }
                        _ => {
                            outbuf.push(c);
                            next_raw = false;
                        }
                    }
                }

                // start a timer to flush what we have and send a UDP packet,
                // unless we're already waiting to flush
                if flush_timeout.is_terminated() {
                    flush_timeout.set(
                        tokio::time::sleep(Duration::from_millis(500)).fuse(),
                    );
                }
            } else {
                outbuf.extend_from_slice(&inbuf[..n]);
                flush_timeout
                    .set(tokio::time::sleep(Duration::default()).fuse());
            }
        }
    });

    let mut stdout = tokio::io::stdout();
    let mut ws = client.attach().await?;
    loop {
        tokio::select! {
            c = rx.recv() => {
                match c {
                    None => {
                        // channel is closed
                        _ = ws.close(Some(CloseFrame {
                            code: CloseCode::Normal,
                            reason: Cow::Borrowed("client closed stdin"),
                        })).await;
                        break;
                    }
                    Some(c) => {
                        ws.send(Message::Binary(c)).await?;
                    },
                }
            }
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Binary(input))) => {
                        stdout.write_all(&input).await?;
                        stdout.flush().await?;
                    }
                    Some(Ok(Message::Close(..))) | None => break,
                    _ => continue,
                }
            }
        }
    }

    Ok(())
}

/// Guard object that will set the terminal to raw mode and restore it
/// to its previous state when it's dropped
struct RawTermiosGuard(libc::c_int, libc::termios);

impl RawTermiosGuard {
    fn stdio_guard() -> Result<RawTermiosGuard, std::io::Error> {
        use std::os::unix::prelude::AsRawFd;
        let fd = std::io::stdout().as_raw_fd();
        let termios = unsafe {
            let mut curr_termios = std::mem::zeroed();
            let r = libc::tcgetattr(fd, &mut curr_termios);
            if r == -1 {
                return Err(std::io::Error::last_os_error());
            }
            curr_termios
        };
        let guard = RawTermiosGuard(fd, termios);
        unsafe {
            let mut raw_termios = termios;
            libc::cfmakeraw(&mut raw_termios);
            let r = libc::tcsetattr(fd, libc::TCSAFLUSH, &raw_termios);
            if r == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(guard)
    }
}

impl Drop for RawTermiosGuard {
    fn drop(&mut self) {
        let r = unsafe { libc::tcsetattr(self.0, libc::TCSADRAIN, &self.1) };
        if r == -1 {
            Err::<(), _>(std::io::Error::last_os_error()).unwrap();
        }
    }
}
