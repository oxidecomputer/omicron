// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A simple TCP proxy that provides a stable port for test services so that
//! their backends can be restarted without races.
//!
//! Some of our tests do the following set of operations:
//!
//! 1. Start a service on an ephemeral port (i.e., bind to port 0).
//! 2. Kill the service.
//! 3. Restart the service on the same port.
//!
//! The problem is that between steps 2 and 3, it is possible for the port to be
//! reused by a different test running concurrently, causing interference
//! between the tests.
//!
//! This module provides a stable port for that scenario. Clients connect to the
//! TCP proxy; the proxy is notified of whether the backend is up, and if so,
//! which port it is using.
//!
//! Adapted from `wicketd/src/nexus_proxy.rs`. We keep them separate because the
//! proxies are likely to diverge in functionality in the future (e.g., this
//! proxy might gain more fault injection capabilities).

use std::io;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use std::time::Duration;

use slog::{Logger, debug, info, o, warn};
use slog_error_chain::InlineErrorChain;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, oneshot};

/// The target for a [`RetargetableTcpProxy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProxyTarget {
    /// Forward the connection to the specified backend address.
    Forward(SocketAddr),
    /// Close the connection immediately, mimicking a dead backend.
    Refuse,
}

/// A simple TCP proxy that forwards connections to a backend which can be
/// changed out from underneath.
#[derive(Debug)]
pub struct RetargetableTcpProxy {
    local_addr: SocketAddrV6,
    target: Arc<RwLock<ProxyTarget>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
    log: Logger,
}

impl RetargetableTcpProxy {
    /// Start a new [`RetargetableTcpProxy`] that forwards connections to the
    /// specified target.
    pub async fn bind(target: ProxyTarget, log: &Logger) -> io::Result<Self> {
        let listener =
            TcpListener::bind(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0))
                .await?;
        let local_addr = match listener.local_addr()? {
            SocketAddr::V6(addr) => addr,
            SocketAddr::V4(addr) => {
                unreachable!(
                    "bound an IPv6 listener but the kernel returned \
                     an IPv4 local address: {addr}"
                );
            }
        };
        let log = log.new(o!(
            "component" => "RetargetableTcpProxy",
            "proxy_addr" => local_addr.to_string(),
        ));
        info!(log, "TCP proxy listening"; "target" => ?target);

        let target = Arc::new(RwLock::new(target));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(run_proxy(
            listener,
            Arc::clone(&target),
            shutdown_rx,
            log.clone(),
        ));

        Ok(Self {
            local_addr,
            target,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
            log,
        })
    }

    /// Return the local address of the proxy.
    pub fn local_addr(&self) -> SocketAddrV6 {
        self.local_addr
    }

    /// Return the TCP port of the proxy.
    pub fn port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Set a new target for the proxy.
    ///
    /// Existing connections that have already been set up will keep using the
    /// old targets. Once this returns, however, no new connections can reach
    /// the old target.
    ///
    /// This is cancel-safe: if the future returned by this method is dropped,
    /// the target will not be replaced.
    ///
    /// Returns the previous target.
    pub async fn set_target(&self, target: ProxyTarget) -> ProxyTarget {
        let old_target = {
            let mut guard = self.target.write().await;
            // Note that we don't hold the guard past an await point.
            std::mem::replace(&mut *guard, target)
        };
        info!(
            self.log, "retargeted TCP proxy";
            "old_target" => ?old_target,
            "new_target" => ?target,
        );
        old_target
    }

    /// Stops the accept loop and releases the port, blocking until the port no
    /// longer accepts new connections.
    ///
    /// Established connections will run to completion, but new connections will
    /// not be accepted.
    ///
    /// This is not cancel-safe in the sense that the shutdown message is sent
    /// immediately, and that the only thing cancelling the future will do is
    /// not wait for the port to fully stop accepting new connections.
    pub async fn shutdown(mut self) {
        self.send_shutdown();
        if let Some(task) = self.task.take() {
            task.await.expect("TCP proxy task exited cleanly");
        }
    }

    fn send_shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            info!(self.log, "shutting down TCP proxy");
            // A send failure means the task already exited.
            if tx.send(()).is_err() {
                warn!(
                    self.log,
                    "failed to send shutdown signal (task already exited)"
                );
            }
        }
    }
}

impl Drop for RetargetableTcpProxy {
    fn drop(&mut self) {
        self.send_shutdown();
    }
}

async fn run_proxy(
    listener: TcpListener,
    target: Arc<RwLock<ProxyTarget>>,
    mut shutdown_rx: oneshot::Receiver<()>,
    log: Logger,
) {
    loop {
        tokio::select! {
            // TcpListener::accept() is cancel-safe, per Tokio documentation.
            result = listener.accept() => {
                match result {
                    Ok((client_stream, peer)) => {
                        let log = log.new(o!("peer" => peer));
                        debug!(log, "accepted connection");
                        tokio::spawn(run_connection(
                            client_stream,
                            Arc::clone(&target),
                            log,
                        ));
                    }
                    Err(error) => {
                        // accept() erroring out might be something like EMFILE
                        // (too many files open). We shouldn't exit the proxy on
                        // something like that -- it's likely transient, and we
                        // don't have a great reporting channel anyway.
                        const ACCEPT_ERROR_SLEEP_DURATION: Duration =
                            Duration::from_millis(100);

                        warn!(
                            log, "accept() failed (continuing after sleep)";
                            InlineErrorChain::new(&error),
                            "sleep_duration" => ?ACCEPT_ERROR_SLEEP_DURATION,
                        );
                        tokio::time::sleep(ACCEPT_ERROR_SLEEP_DURATION).await;
                    }
                }
            }

            // This is cancel-safe because it awaits a &mut Future. It is not
            // susceptible to futurelock because run_proxy runs on a different
            // task from the one that sends the shutdown message.
            _ = &mut shutdown_rx => {
                info!(log, "TCP proxy exiting");
                return;
            }
        }
    }
}

/// Causes closing the stream to produce a connection reset, simulating
/// something closer to (but not quite) a dead server.
fn reset_on_drop(stream: &TcpStream, log: &Logger) {
    if let Err(error) = stream.set_zero_linger() {
        debug!(
            log, "set_zero_linger failed (connection already dead?)";
            InlineErrorChain::new(&error),
        );
    }
}

async fn run_connection(
    mut client_stream: TcpStream,
    target: Arc<RwLock<ProxyTarget>>,
    log: Logger,
) {
    let mut backend_stream = {
        // Here, the read guard _is_ held across the connect await point so that
        // it is synchronized with set_target (which acquires a write lock). But
        // it is _not_ held across copy_bidirectional below -- we do not want
        // deadlocks involving `set_target` and open connections!
        let guard = target.read().await;
        match *guard {
            ProxyTarget::Forward(backend_addr) => {
                match TcpStream::connect(backend_addr).await {
                    Ok(stream) => {
                        debug!(
                            log, "connected to backend";
                            "backend_addr" => backend_addr,
                        );
                        stream
                    }
                    Err(error) => {
                        warn!(
                            log,
                            "failed to connect to backend; \
                             resetting client connection";
                            "backend_addr" => backend_addr,
                            InlineErrorChain::new(&error),
                        );
                        reset_on_drop(&client_stream, &log);
                        return;
                    }
                }
            }
            ProxyTarget::Refuse => {
                debug!(
                    log,
                    "refusing accepted connection; resetting it immediately"
                );
                reset_on_drop(&client_stream, &log);
                return;
            }
        }
    };

    // Setting `TCP_NODELAY` avoids Nagle-induced latency. Failure implies that
    // the connection is already dead, which will be reported in the
    // copy_bidirectional call's result below.
    if let Err(error) = client_stream.set_nodelay(true) {
        debug!(
            log, "set_nodelay failed on client stream";
            InlineErrorChain::new(&error),
        );
    }
    if let Err(error) = backend_stream.set_nodelay(true) {
        debug!(
            log, "set_nodelay failed on backend stream";
            InlineErrorChain::new(&error),
        );
    }

    match tokio::io::copy_bidirectional(&mut client_stream, &mut backend_stream)
        .await
    {
        Ok((client_to_backend, backend_to_client)) => {
            debug!(
                log, "proxied connection closed";
                "bytes_client_to_backend" => client_to_backend,
                "bytes_backend_to_client" => backend_to_client,
            );
        }
        Err(error) => {
            // This is expected when the backend is SIGKILLed mid-connection.
            // Log it as a warning anyway to flag cases where the process
            // aborted or similar.
            warn!(
                log, "proxied connection ended with error";
                InlineErrorChain::new(&error),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Persistence {
        Persistent,
        OneShot,
    }

    /// A simple TCP backend that echoes what was provided to it.
    #[derive(Debug)]
    struct TestBackend {
        addr: SocketAddr,
        accepts: Arc<AtomicUsize>,
    }

    impl TestBackend {
        async fn spawn(
            reply_prefix: &'static [u8],
            persistence: Persistence,
        ) -> Self {
            let listener = TcpListener::bind(SocketAddrV6::new(
                Ipv6Addr::LOCALHOST,
                0,
                0,
                0,
            ))
            .await
            .expect("bound backend listener");
            let addr = listener.local_addr().expect("read backend local addr");
            let accepts = Arc::new(AtomicUsize::new(0));
            let accepts_bg = Arc::clone(&accepts);
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    accepts_bg.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(async move {
                        let mut buf = [0u8; 5];
                        loop {
                            if stream.read_exact(&mut buf).await.is_err() {
                                return;
                            }
                            if !reply_prefix.is_empty()
                                && stream.write_all(reply_prefix).await.is_err()
                            {
                                return;
                            }
                            if stream.write_all(&buf).await.is_err() {
                                return;
                            }
                            match persistence {
                                Persistence::OneShot => return,
                                Persistence::Persistent => {
                                    // Keep looping to read more data.
                                }
                            }
                        }
                    });
                }
            });
            Self { addr, accepts }
        }

        fn addr(&self) -> SocketAddr {
            self.addr
        }

        fn accepted(&self) -> usize {
            self.accepts.load(Ordering::Relaxed)
        }
    }

    async fn roundtrip(proxy_addr: SocketAddr, request: &[u8; 5]) -> Vec<u8> {
        let mut conn =
            TcpStream::connect(proxy_addr).await.expect("connected to proxy");
        conn.write_all(request).await.expect("wrote request");
        let mut response = Vec::new();
        conn.read_to_end(&mut response).await.expect("read response");
        response
    }

    #[tokio::test]
    async fn proxy_forwards_and_retargets() {
        let logctx = crate::dev::test_setup_log("proxy_forwards_and_retargets");
        let backend_a = TestBackend::spawn(b"A:", Persistence::OneShot).await;
        let backend_b = TestBackend::spawn(b"B:", Persistence::OneShot).await;

        let proxy = RetargetableTcpProxy::bind(
            ProxyTarget::Forward(backend_a.addr()),
            &logctx.log,
        )
        .await
        .expect("bound proxy");
        let proxy_addr = SocketAddr::V6(proxy.local_addr());
        assert_ne!(proxy.port(), 0);

        assert_eq!(roundtrip(proxy_addr, b"hello").await, b"A:hello");

        // Once Refuse is set, connections are accepted and closed immediately.
        proxy.set_target(ProxyTarget::Refuse).await;
        let mut conn =
            TcpStream::connect(proxy_addr).await.expect("connected to proxy");
        let mut response = Vec::new();
        // reset_on_drop sets SO_LINGER(0) which shows up as a ConnectionReset.
        let error = conn
            .read_to_end(&mut response)
            .await
            .expect_err("connection is reset in Refuse mode");
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);

        // Test the point of the TCP proxy. While the backend is down, the proxy
        // must keep the port bound so a concurrently running process (like a
        // test) can't grab it.
        TcpListener::bind(proxy_addr).await.expect_err(
            "proxy keeps the port bound while the backend is down, so a \
             concurrent binder cannot steal it",
        );

        proxy.set_target(ProxyTarget::Forward(backend_b.addr())).await;
        assert_eq!(roundtrip(proxy_addr, b"world").await, b"B:world");

        proxy.shutdown().await;
        // The port must be rebindable after shutdown.
        TcpListener::bind(proxy_addr)
            .await
            .expect("proxy port is released after shutdown");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn refuse_quiesces_old_backend() {
        let logctx = crate::dev::test_setup_log("refuse_quiesces_old_backend");
        let backend = TestBackend::spawn(b"A:", Persistence::OneShot).await;

        let proxy = RetargetableTcpProxy::bind(
            ProxyTarget::Forward(backend.addr()),
            &logctx.log,
        )
        .await
        .expect("bound proxy");
        let proxy_addr = SocketAddr::V6(proxy.local_addr());

        assert_eq!(roundtrip(proxy_addr, b"hello").await, b"A:hello");
        assert_eq!(backend.accepted(), 1);

        proxy.set_target(ProxyTarget::Refuse).await;

        // Attempt to hit the backend a bunch of times now. It should never be
        // successful.
        for _ in 0..16 {
            let mut conn = TcpStream::connect(proxy_addr)
                .await
                .expect("connected to proxy");
            let mut response = Vec::new();
            let error = conn
                .read_to_end(&mut response)
                .await
                .expect_err("connection is reset in Refuse mode");
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
        }
        assert_eq!(
            backend.accepted(),
            1,
            "no connection should reach the backend once the proxy refuses"
        );

        proxy.shutdown().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn set_target_does_not_deadlock_with_live_connection() {
        let logctx = crate::dev::test_setup_log(
            "set_target_does_not_deadlock_with_live_connection",
        );
        let backend = TestBackend::spawn(b"", Persistence::Persistent).await;

        let proxy = RetargetableTcpProxy::bind(
            ProxyTarget::Forward(backend.addr()),
            &logctx.log,
        )
        .await
        .expect("bound proxy");
        let proxy_addr = SocketAddr::V6(proxy.local_addr());

        // Exchange a message so this connection's copy_bidirectional is
        // actively running. (If the read guard weren't released before
        // copy_bidirectional, it would be held here.)
        let mut conn =
            TcpStream::connect(proxy_addr).await.expect("connected to proxy");
        conn.write_all(b"ping1").await.expect("wrote first message");
        let mut buf = [0u8; 5];
        conn.read_exact(&mut buf).await.expect("read first echo");
        assert_eq!(&buf, b"ping1");

        // Attempt to call set_target while the connection is open. If the
        // timeout is hit, then the read guard is held for some reason.
        const SET_TARGET_TIMEOUT: Duration = Duration::from_secs(10);
        tokio::time::timeout(
            SET_TARGET_TIMEOUT,
            proxy.set_target(ProxyTarget::Refuse),
        )
        .await
        .expect(
            "set_target should complete promptly while a connection is open",
        );

        // The established connection keeps using the old target.
        conn.write_all(b"ping2").await.expect("wrote second message");
        conn.read_exact(&mut buf).await.expect("read second echo");
        assert_eq!(&buf, b"ping2");

        // But new connections are refused.
        let mut fresh =
            TcpStream::connect(proxy_addr).await.expect("connected to proxy");
        let mut response = Vec::new();
        let error = fresh
            .read_to_end(&mut response)
            .await
            .expect_err("new connection is refused after retarget");
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);

        proxy.shutdown().await;
        logctx.cleanup_successful();
    }
}
