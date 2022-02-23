// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO: This may need to move in part or in its entirety to a separate crate
// for reuse by RSS, once that exists.

//! Inteface for communicating with SPs over UDP on the management network.

use crate::config::KnownSps;
use dropshot::HttpError;
use gateway_messages::{
    version, IgnitionState, Request, RequestKind, Response, ResponseKind,
    SerializedSize,
};
use slog::{debug, error, info, o, Logger};
use std::{
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr},
    sync::{atomic::AtomicU32, Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::oneshot::{self, error::RecvError, Receiver, Sender},
    task::JoinHandle,
};

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("error binding to UDP address {addr}: {err}")]
    UdpBind { addr: SocketAddr, err: io::Error },
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("error sending to UDP address {addr}: {err}")]
    UdpSend { addr: SocketAddr, err: io::Error },
    #[error("timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
}

impl From<Error> for HttpError {
    fn from(err: Error) -> Self {
        // none of `Error`'s cases are caused by the client; they're all
        // internal to gateway <-> SP failures
        HttpError::for_internal_error(err.to_string())
    }
}

#[derive(Debug)]
pub struct SpCommunicator {
    log: Logger,
    socket: Arc<UdpSocket>,
    known_sps: KnownSps,
    outstanding_requests: Arc<OutstandingRequests>,
    request_id: AtomicU32,
    recv_task: JoinHandle<()>,
}

impl Drop for SpCommunicator {
    fn drop(&mut self) {
        // default `JoinHandle` drop behavior is to detach; we want to kill our
        // recv task if we're dropped.
        self.recv_task.abort();
    }
}

impl SpCommunicator {
    pub async fn new(
        bind_addr: SocketAddr,
        known_sps: KnownSps,
        log: &Logger,
    ) -> Result<Self, StartupError> {
        let socket =
            Arc::new(UdpSocket::bind(bind_addr).await.map_err(|err| {
                StartupError::UdpBind { addr: bind_addr, err }
            })?);
        let log = log.new(o!(
            "componennt" => "SpCommunicator",
            "local_addr" => bind_addr,
        ));
        let outstanding_requests =
            Arc::new(OutstandingRequests::new(&known_sps));
        let recv_task = RecvTask::new(
            Arc::clone(&socket),
            Arc::clone(&outstanding_requests),
            log.clone(),
        );
        let recv_task = tokio::spawn(recv_task.run());
        info!(&log, "started sp-server");
        Ok(Self {
            log,
            socket,
            known_sps,
            outstanding_requests,
            request_id: AtomicU32::new(0),
            recv_task,
        })
    }

    pub fn placeholder_known_sps(&self) -> &KnownSps {
        &self.known_sps
    }

    // How do we want to describe ignition targets? Currently we want to
    // send a u8 in the UDP message, so just take that for now.
    pub async fn ignition_get(
        &self,
        target: u8,
        timeout: Duration,
    ) -> Result<IgnitionState, Error> {
        tokio::time::timeout(timeout, self.ignition_get_impl(target)).await?
    }

    // TODO As we add additional methods, it's likely this should be cleaned up
    // to extract common logic, as most methods will only vary by the type of
    // request/response they're sending. For now we only have this one method.
    async fn ignition_get_impl(
        &self,
        target: u8,
    ) -> Result<IgnitionState, Error> {
        // XXX We currently assume we're know which ignition controller is our
        // local one, and only use it for ignition interactions.
        let controller = self.known_sps.ignition_controller;

        // request IDs will eventually roll over; since we enforce timeouts
        // this should be a non-issue in practice. does this need testing?
        let request_id =
            self.request_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // tell our background receiver to expect a response to this request
        let response =
            self.outstanding_requests.insert(controller.ip(), request_id);

        // Serialize and send our request. We know `buf` is large enough for any
        // `Request`, so unwrapping here is fine.
        let request = Request {
            version: version::V1,
            request_id,
            kind: RequestKind::IgnitionState { target },
        };
        let mut buf = [0; Request::MAX_SIZE];
        let n = gateway_messages::serialize(&mut buf, &request).unwrap();

        let serialized_request = &buf[..n];
        debug!(
            &self.log,
            "sending {:?} to igntition controller {}", request, controller
        );
        self.socket
            .send_to(serialized_request, controller)
            .await
            .map_err(|err| Error::UdpSend { addr: controller, err })?;

        // recv() can only fail if the sender is dropped, but we're holding it
        // in `self.outstanding_requests`; unwrap() is fine.
        let response_kind = response.recv().await.unwrap();

        match response_kind {
            ResponseKind::IgnitionState(state) => Ok(state),
            other => panic!("bogus response kind {:?}", other),
        }
    }
}

/// Handle for the background tokio task responsible for receiving incoming UDP
/// messages.
///
/// We currently assume that we know (before this task is spawned) the IP
/// address of all SPs with which we want to communicate, and that those IP
/// addresses will not change while we're running. Either or both of those may
/// end up being wrong.
///
/// This task is spawned when `SpCommunicator` is created, and runs until it is
/// dropped. When the communicator wants to send a request on behalf of an HTTP
/// request:
///
/// 1. `SpCommunicator` creates a tokio oneshot channel for this task to use to
///    send the response.
/// 2. `SpCommunicator` inserts the sending half of that channel into
///    `outstanding_requests`, which is keyed by both the SP IP address and the
///    u32 ID attached to the request.
/// 3. `SpCommunicator` sends the UDP packet containing the request to the
///    target SP, and waits for a response on the channel it created in 1.
/// 4. When we receive a packet, we check:
///    a. Did it come from an IP address in our list of SP IPs?
///    b. Does it parse as a `Response`?
///    c. Is there a corresponding entry in `outstanding_requests` for this IP +
///    request ID?
///    If so, we send the response on the channel, which unblocks
///    `SpCommunicator`, who can now return the response to its caller.
///
/// If a timeout or other error occurs between step 2 and the end of step 4, the
/// `ResponseReceiver` wrapper below is responsible for cleaning up the entry in
/// `outstanding_requests` (via its `Drop` impl).
struct RecvTask {
    socket: Arc<UdpSocket>,
    outstanding_requests: Arc<OutstandingRequests>,
    log: Logger,
}

impl RecvTask {
    fn new(
        socket: Arc<UdpSocket>,
        outstanding_requests: Arc<OutstandingRequests>,
        log: Logger,
    ) -> Self {
        Self { socket, outstanding_requests, log }
    }

    async fn run(self) {
        let mut resp_buf = [0; Response::MAX_SIZE];
        loop {
            // raw recv
            let (n, addr) = match self.socket.recv_from(&mut resp_buf).await {
                Ok((n, addr)) => (n, addr),
                Err(err) => {
                    error!(&self.log, "recv_from() failed: {}", err);
                    continue;
                }
            };
            debug!(&self.log, "received {} bytes from {}", n, addr);

            // parse into a `Response`
            let resp =
                match gateway_messages::deserialize::<Response>(&resp_buf[..n])
                {
                    Ok((resp, _extra)) => {
                        // TODO should we check that `extra` is empty? if the
                        // response is maximal size any extra data is silently
                        // discarded anyway, so probably not?
                        resp
                    }
                    Err(err) => {
                        error!(
                            &self.log,
                            "discarding malformed response ({})", err
                        );
                        continue;
                    }
                };
            debug!(&self.log, "received {:?} from {}", resp, addr);

            // `version` is intentionally the first 4 bytes of the packet; we
            // could check it before trying to deserialize?
            if resp.version != version::V1 {
                error!(
                    &self.log,
                    "discarding message with unsupported version {}",
                    resp.version
                );
                continue;
            }

            // see if we know who to send the response to
            let tx = match self
                .outstanding_requests
                .remove(addr.ip(), resp.request_id)
            {
                Some(tx) => tx,
                None => {
                    error!(&self.log,
                        "discarding unexpected response {} from {} (possibly past timeout?)",
                        resp.request_id,
                        addr,
                    );
                    continue;
                }
            };

            // actually send it
            if tx.send(resp.kind).is_err() {
                // This can only fail if the receiving half has been dropped.
                // That's held in the relevant `SpCommunicator` method above
                // that initiated this request; they should only have dropped
                // the rx half if they've been dropped (in which case we've been
                // aborted and can't get here) or if we landed in a race where
                // the `SpCommunicator` task was cancelled (presumably by
                // timeout) in between us pulling `tx` out of
                // `outstanding_requests` and actually sending the response on
                // it. But that window does exist, so log when we fail to send.
                // I believe these should be interpreted as timeout failures;
                // most of the time failing to get a `tx` at all (above) is also
                // caused by a timeout, but that path is also invoked if we get
                // a garbage response somehow.
                error!(
                    &self.log,
                    "discarding unexpected response {} from {} (receiver gone)",
                    resp.request_id,
                    addr,
                );
            }
        }
    }
}

#[derive(Debug)]
struct OutstandingRequests {
    // map of SP -> request ID -> receiving oneshot channel
    requests_by_sp: HashMap<IpAddr, Mutex<HashMap<u32, Sender<ResponseKind>>>>,
}

impl OutstandingRequests {
    fn new(known_sps: &KnownSps) -> Self {
        let mut requests_by_sp = HashMap::new();
        for sp_list in [
            &known_sps.switches,
            &known_sps.sleds,
            &known_sps.power_controllers,
        ] {
            for sp in sp_list {
                requests_by_sp.insert(sp.ip(), Mutex::default());
            }
        }
        Self { requests_by_sp }
    }

    fn insert(
        self: &Arc<Self>,
        sp: IpAddr,
        request_id: u32,
    ) -> ResponseReceiver {
        // caller should never try to send a request to an SP we don't know
        // about, since it created us with all SPs it knows.
        let requests = self.requests_by_sp.get(&sp).expect("nonexistent SP");

        let (tx, rx) = oneshot::channel();
        requests.lock().unwrap().insert(request_id, tx);

        ResponseReceiver {
            parent: Arc::clone(self),
            sp,
            request_id,
            rx,
            removed_from_parent: false,
        }
    }

    fn remove(
        &self,
        sp: IpAddr,
        request_id: u32,
    ) -> Option<Sender<ResponseKind>> {
        // caller should never try to send a request to an SP we don't know
        // about, since it created us with all SPs it knows.
        let requests = self.requests_by_sp.get(&sp).expect("nonexistent SP");
        requests.lock().unwrap().remove(&request_id)
    }
}

// Wrapper around a tokio oneshot receiver that removes itself from its parent
// `OutstandingRequests` either when the message is received (happy path) or
// we're dropped (due to either a timeout or some other kind of
// error/cancellation)
struct ResponseReceiver {
    parent: Arc<OutstandingRequests>,
    sp: IpAddr,
    request_id: u32,
    rx: Receiver<ResponseKind>,
    removed_from_parent: bool,
}

impl Drop for ResponseReceiver {
    fn drop(&mut self) {
        self.remove_from_parent();
    }
}

impl ResponseReceiver {
    async fn recv(mut self) -> Result<ResponseKind, RecvError> {
        let result = (&mut self.rx).await;
        self.remove_from_parent();
        result
    }

    fn remove_from_parent(&mut self) {
        // we're unconditionally called from our `Drop` impl, but if we already
        // successfully received a response we already removed ourselves
        if self.removed_from_parent {
            return;
        }

        // we should never have an SP IP address that doesn't exist in our
        // parent
        let map =
            self.parent.requests_by_sp.get(&self.sp).expect("nonexistent SP");

        map.lock().unwrap().remove(&self.request_id);
        self.removed_from_parent = true;
    }
}
