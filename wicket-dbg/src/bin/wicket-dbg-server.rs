// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result, bail};
use bytes::BytesMut;
use camino::Utf8PathBuf;
use slog::Drain;
use slog::info;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wicket_dbg::{Cmd, Runner, RunnerHandle};

fn main() -> Result<()> {
    oxide_tokio_rt::run(main_impl())
}

async fn main_impl() -> Result<()> {
    let log = setup_log()?;
    let (mut runner, handle) = Runner::new(log.clone());

    tokio::spawn(async move {
        runner.run().await;
    });

    // TODO: Allow port configuration
    let listener = TcpListener::bind("::1:9010").await?;

    // Accept connections serially. We only want one client at a time.
    loop {
        let (sock, _) = listener.accept().await?;

        if let Err(e) = process(sock, &handle).await {
            info!(log, "Error processing request: {e:#?}");
        }
    }
}

async fn process(mut sock: TcpStream, handle: &RunnerHandle) -> Result<()> {
    // Reuse allocations across commands
    let mut buf = BytesMut::with_capacity(4096);
    let mut out = Vec::with_capacity(4096);
    loop {
        // Read at least the 4-byte size header
        while buf.len() < 4 {
            if 0 == sock.read_buf(&mut buf).await? {
                // EOF - The client disconnected. Just return.
                return Ok(());
            };
        }
        // We at least have the 4-byte size header
        let frame_size =
            u32::from_le_bytes(<[u8; 4]>::try_from(&buf[..4]).unwrap())
                as usize;
        let total_size = frame_size + 4;

        // Read the rest of the frame
        while buf.len() < total_size {
            if 0 == sock.read_buf(&mut buf).await? {
                // EOF - The client disconnected. Just return.
                return Ok(());
            };
        }

        if buf.len() > total_size {
            bail!("Data size exceeded frame + header size");
        }

        // We've read a complete frame
        let cmd: Cmd = ciborium::de::from_reader(&buf[4..])?;
        let rpy = handle.send(cmd).await;

        // Serialize the reply
        ciborium::ser::into_writer(&rpy, &mut out)?;

        // Write a 4-byte frame header along with the reply
        let frame_size = u32::try_from(out.len()).unwrap();
        sock.write_all(&frame_size.to_le_bytes()[..]).await?;
        sock.write_all(&out[..]).await?;

        // Clear the buffers so we can re-use them
        buf.clear();
        out.clear();
    }
}

fn setup_log() -> anyhow::Result<slog::Logger> {
    let path = log_path()?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .with_context(|| format!("error opening log file {path}"))?;

    let decorator = slog_term::PlainDecorator::new(file);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let stderr_drain = stderr_env_drain("RUST_LOG");
    let drain = slog::Duplicate::new(drain, stderr_drain).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    Ok(slog::Logger::root(drain, slog::o!("component" => "wicket-dbg-server")))
}

fn log_path() -> Result<Utf8PathBuf> {
    match std::env::var("WICKET_DBG_SERVER_LOG_PATH") {
        Ok(path) => Ok(path.into()),
        Err(std::env::VarError::NotPresent) => {
            Ok("/tmp/wicket_dbg_server.log".into())
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("WICKET_DBG_SERVER_LOG_PATH is not valid unicode");
        }
    }
}

fn stderr_env_drain(
    env_var: &str,
) -> impl Drain<Ok = (), Err = slog::Never> + use<> {
    let stderr_decorator = slog_term::TermDecorator::new().build();
    let stderr_drain =
        slog_term::FullFormat::new(stderr_decorator).build().fuse();
    let mut builder = slog_envlogger::LogBuilder::new(stderr_drain);
    if let Ok(s) = std::env::var(env_var) {
        builder = builder.parse(&s);
    } else {
        // Log at the info level by default.
        builder = builder.filter(None, slog::FilterLevel::Info);
    }
    builder.build()
}
