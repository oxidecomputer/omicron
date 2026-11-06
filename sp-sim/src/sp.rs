// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Board-independent parts of a simulated SP.

use crate::Responsiveness;
use crate::config::Ereport;
use crate::config::EreportRestart;
use crate::ereport;
use crate::ereport::EreportState;
use crate::server::Command;
use crate::server::SimSpHandler;
use crate::server::UdpServer;
use crate::server::UdpTask;
use gateway_ereport_messages::Ena;
use gateway_messages::SpPort;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::MutexGuard;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task;
use tokio::task::JoinHandle;

/// A handle to a running simulated SP (i.e. the spawned [`UdpTask`] and
/// its `H`-typed [`SimSpHandler`] implementation).
///
/// This type implements the board-independent parts of the
/// [`SimulatedSp`](crate::SimulatedSp) trait. Each board-specific simulated SP
/// holds an instance of this struct and delegates its implementation to it.
///
/// A simulated SP without a network config can be constructed with
/// [`Handle::rot_only`], and as the name implies, will only simulate the root
/// of trust. It has no `SimSpHandler` implementation and doesn't bind any UDP
/// sockets, so any commands that require these will fail.
///
/// Dropping this handle aborts the UDP task.
pub(crate) struct Handle<H> {
    local_addrs: Option<[SocketAddrV6; 2]>,
    ereport_addrs: Option<[SocketAddrV6; 2]>,
    handler: Option<Arc<TokioMutex<H>>>,
    commands: mpsc::UnboundedSender<Command>,
    udp_task: Option<JoinHandle<()>>,
    responses_sent_count: Option<watch::Receiver<usize>>,
    power_state_changes: Arc<AtomicUsize>,
}

impl<H> Handle<H>
where
    H: SimSpHandler<VLanId = SpPort> + Send + 'static,
{
    /// Spawns a [`UdpTask`] serving `handler` on the given sockets.
    pub(crate) fn spawn(
        servers: [UdpServer; 2],
        ereport_servers: Option<[UdpServer; 2]>,
        ereport_state: EreportState,
        handler: H,
    ) -> Self {
        let local_addrs = servers.each_ref().map(UdpServer::local_addr);
        let ereport_addrs = ereport_servers
            .as_ref()
            .map(|servers| servers.each_ref().map(UdpServer::local_addr));
        let power_state_changes = handler.power_state_changes().clone();
        let handler = Arc::new(TokioMutex::new(handler));
        let (commands, commands_rx) = mpsc::unbounded_channel();
        let (udp_task, responses_sent_count) = UdpTask::new(
            servers,
            ereport_servers,
            ereport_state,
            handler.clone(),
            commands_rx,
        );
        let udp_task =
            task::spawn(async move { udp_task.run().await.unwrap() });
        Self {
            local_addrs: Some(local_addrs),
            ereport_addrs,
            handler: Some(handler),
            commands,
            udp_task: Some(udp_task),
            responses_sent_count: Some(responses_sent_count),
            power_state_changes,
        }
    }
}

impl<H> Handle<H> {
    /// Returns a handle for a simulated SP configured without networking,
    /// which only simulates an RoT.
    pub(crate) fn rot_only() -> Self {
        // Dropping the receiver makes every command send fail, as it would if
        // the UDP task had exited.
        let (commands, _) = mpsc::unbounded_channel();
        Self {
            local_addrs: None,
            ereport_addrs: None,
            handler: None,
            commands,
            udp_task: None,
            responses_sent_count: None,
            power_state_changes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Locks and returns the SP's handler, or `None` if this handle is
    /// [`rot_only`](Handle::rot_only).
    ///
    /// The UDP task holds this lock while handling each request, so method
    /// waits for any in-flight request to complete.
    pub(crate) async fn handler(&self) -> Option<MutexGuard<'_, H>> {
        Some(self.handler.as_ref()?.lock().await)
    }

    pub(crate) fn local_addr(&self, port: SpPort) -> Option<SocketAddrV6> {
        self.local_addrs.map(|addrs| addrs[port_index(port)])
    }

    pub(crate) fn local_ereport_addr(
        &self,
        port: SpPort,
    ) -> Option<SocketAddrV6> {
        self.ereport_addrs.map(|addrs| addrs[port_index(port)])
    }

    /// # Panics
    ///
    /// Panics if the UDP task is not running.
    pub(crate) async fn set_responsiveness(&self, r: Responsiveness) {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::SetResponsiveness(r, tx))
            .expect("simulated SP's UDP task is not running");
        rx.await.unwrap();
    }

    pub(crate) fn power_state_changes(&self) -> usize {
        self.power_state_changes.load(Ordering::Relaxed)
    }

    pub(crate) fn responses_sent_count(
        &self,
    ) -> Option<watch::Receiver<usize>> {
        self.responses_sent_count.clone()
    }

    pub(crate) async fn install_udp_accept_semaphore(
        &self,
    ) -> mpsc::UnboundedSender<usize> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (resp_tx, resp_rx) = oneshot::channel();
        if let Ok(()) =
            self.commands.send(Command::SetThrottler(Some(rx), resp_tx))
        {
            resp_rx.await.unwrap();
        }
        tx
    }

    pub(crate) async fn ereport_restart(&self, restart: EreportRestart) {
        let (tx, rx) = oneshot::channel();
        if self
            .commands
            .send(Command::Ereport(ereport::Command::Restart(restart, tx)))
            .is_ok()
        {
            rx.await.unwrap();
        }
    }

    /// Appends `ereport` to the simulated SP's ereport queue, returning the
    /// [`Ena`] assigned to that ereport.
    ///
    /// # Panics
    ///
    /// Panics if the UDP task is not running.
    pub(crate) async fn ereport_append(&self, ereport: Ereport) -> Ena {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Ereport(ereport::Command::Append(ereport, tx)))
            .expect("simulated SP's UDP task is not running");
        rx.await.unwrap()
    }
}

impl<H> Drop for Handle<H> {
    fn drop(&mut self) {
        if let Some(udp_task) = self.udp_task.as_ref() {
            // default join handle drop behavior is to detach; we want to abort
            udp_task.abort();
        }
    }
}

fn port_index(port: SpPort) -> usize {
    match port {
        SpPort::One => 0,
        SpPort::Two => 1,
    }
}
