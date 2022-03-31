// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::error::BadResponseType;
use crate::error::Error;
use crate::error::SpCommunicationError;
use crate::error::StartupError;
use crate::management_switch::ManagementSwitch;
use crate::management_switch::ManagementSwitchDiscovery;
use crate::management_switch::SpSocket;
use crate::management_switch::SwitchPort;
use crate::recv_handler::RecvHandler;
use crate::KnownSps;
use crate::SpIdentifier;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::Stream;
use gateway_messages::version;
use gateway_messages::BulkIgnitionState;
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionState;
use gateway_messages::Request;
use gateway_messages::RequestKind;
use gateway_messages::ResponseError;
use gateway_messages::ResponseKind;
use gateway_messages::SerialConsole;
use gateway_messages::SerializedSize;
use gateway_messages::SpComponent;
use gateway_messages::SpState;
use hyper::header;
use hyper::upgrade;
use hyper::Body;
use omicron_common::backoff;
use omicron_common::backoff::Backoff;
use slog::debug;
use slog::info;
use slog::o;
use slog::Logger;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::handshake;

/// Helper trait that allows us to return an `impl FuturesUnordered<_>` where
/// the caller can call `.is_empty()` without knowing the type of the future
/// inside the collection.
pub trait FuturesUnorderedImpl: Stream + Unpin {
    fn is_empty(&self) -> bool;
}

impl<Fut> FuturesUnorderedImpl for FuturesUnordered<Fut>
where
    Fut: Future,
{
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

#[derive(Debug)]
pub struct Communicator {
    log: Logger,
    switch: ManagementSwitch,
    request_id: AtomicU32,
    recv_handler: Arc<RecvHandler>,
}

impl Communicator {
    pub async fn new(
        known_sps: KnownSps,
        log: &Logger,
    ) -> Result<Self, StartupError> {
        let log = log.new(o!("component" => "SpCommunicator"));
        let discovery = ManagementSwitchDiscovery::placeholder_start(
            known_sps,
            log.clone(),
        )
        .await?;

        let (switch, recv_handler) = RecvHandler::new(discovery, log.clone());

        info!(&log, "started SP communicator");
        Ok(Self { log, switch, request_id: AtomicU32::new(0), recv_handler })
    }

    // convert an identifier to a port number; this is fallible because
    // identifiers can be constructed arbiatrarily, in contrast to `port_to_id`
    // below.
    fn id_to_port(&self, sp: SpIdentifier) -> Result<SwitchPort, Error> {
        self.switch.switch_port(sp).ok_or(Error::SpDoesNotExist(sp))
    }

    // convert a port to an identifier; this is infallible because we construct
    // `SwitchPort`s and know they map to valid IDs
    fn port_to_id(&self, port: SwitchPort) -> SpIdentifier {
        self.switch.switch_port_to_id(port)
    }

    /// Ask the local ignition controller for the ignition state of a given SP.
    pub async fn get_ignition_state(
        &self,
        sp: SpIdentifier,
        timeout: Instant,
    ) -> Result<IgnitionState, Error> {
        let controller = self.switch.ignition_controller();
        let port = self.id_to_port(sp)?;
        let request =
            RequestKind::IgnitionState { target: port.as_ignition_target() };

        self.request_response(
            &controller,
            request,
            Some(timeout),
            ResponseKindExt::try_into_ignition_state,
        )
        .await
    }

    /// Ask the local ignition controller for the ignition state of all SPs.
    pub async fn get_ignition_state_all(
        &self,
        timeout: Instant,
    ) -> Result<Vec<(SpIdentifier, IgnitionState)>, Error> {
        let controller = self.switch.ignition_controller();
        let request = RequestKind::BulkIgnitionState;

        let bulk_state = self
            .request_response(
                &controller,
                request,
                Some(timeout),
                ResponseKindExt::try_into_bulk_ignition_state,
            )
            .await?;

        // deserializing checks that `num_targets` is reasonably sized, so we
        // don't need to guard that here
        let targets =
            &bulk_state.targets[..usize::from(bulk_state.num_targets)];

        // map ignition target indices back to `SpIdentifier`s for our caller
        targets
            .iter()
            .copied()
            .enumerate()
            .map(|(target, state)| {
                let port = self
                    .switch
                    .switch_port_from_ignition_target(target)
                    .ok_or(SpCommunicationError::BadIgnitionTarget(target))?;
                let id = self.port_to_id(port);
                Ok((id, state))
            })
            .collect()
    }

    /// Instruct the local ignition controller to perform the given `command` on
    /// `target_sp`.
    pub async fn send_ignition_command(
        &self,
        target_sp: SpIdentifier,
        command: IgnitionCommand,
        timeout: Instant,
    ) -> Result<(), Error> {
        let controller = self.switch.ignition_controller();
        let target = self.id_to_port(target_sp)?.as_ignition_target();
        let request = RequestKind::IgnitionCommand { target, command };

        self.request_response(
            &controller,
            request,
            Some(timeout),
            ResponseKindExt::try_into_ignition_command_ack,
        )
        .await
    }

    /// Set up a websocket connection that forwards data to and from the given
    /// SP component's serial console.
    // TODO: Much of the implementation of this function is shamelessly copied
    // from propolis. Should dropshot provide some of this? Is there another
    // common place it could live?
    //
    // NOTE / TODO: This currently does not actually contact the target SP; it
    // sets up the websocket connection in the current process which knows how
    // to relay any information sent or received on that connection to the SP
    // via UDP. SPs will continuously broadcast any serial console data, even if
    // there is no attached client. Maybe this is fine, since the serial console
    // shouldn't be noisy without a corresponding client driving it?
    pub async fn serial_console_attach(
        self: &Arc<Self>,
        request: &mut http::Request<Body>,
        sp: SpIdentifier,
        component: SpComponent,
        sp_ack_timeout: Duration,
    ) -> Result<http::Response<Body>, Error> {
        let port = self.id_to_port(sp)?;

        if !request
            .headers()
            .get(header::CONNECTION)
            .and_then(|hv| hv.to_str().ok())
            .map(|hv| {
                hv.split(|c| c == ',' || c == ' ')
                    .any(|vs| vs.eq_ignore_ascii_case("upgrade"))
            })
            .unwrap_or(false)
        {
            return Err(Error::BadWebsocketConnection(
                "expected connection upgrade",
            ));
        }
        if !request
            .headers()
            .get(header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .map(|v| {
                v.split(|c| c == ',' || c == ' ')
                    .any(|v| v.eq_ignore_ascii_case("websocket"))
            })
            .unwrap_or(false)
        {
            return Err(Error::BadWebsocketConnection(
                "unexpected protocol for upgrade",
            ));
        }
        if request
            .headers()
            .get(header::SEC_WEBSOCKET_VERSION)
            .map(|v| v.as_bytes())
            != Some(b"13")
        {
            return Err(Error::BadWebsocketConnection(
                "missing or invalid websocket version",
            ));
        }
        let accept_key = request
            .headers()
            .get(header::SEC_WEBSOCKET_KEY)
            .map(|hv| hv.as_bytes())
            .map(|key| handshake::derive_accept_key(key))
            .ok_or(Error::BadWebsocketConnection("missing websocket key"))?;

        self.recv_handler.serial_console_attach(
            Arc::clone(self),
            port,
            component,
            sp_ack_timeout,
            upgrade::on(request),
        )?;

        // `.body()` only fails if our headers are bad, which they aren't
        // (unless `hyper::handshake` gives us a bogus accept key?), so we're
        // safe to unwrap this
        Ok(http::Response::builder()
            .status(http::StatusCode::SWITCHING_PROTOCOLS)
            .header(header::CONNECTION, "Upgrade")
            .header(header::UPGRADE, "websocket")
            .header(header::SEC_WEBSOCKET_ACCEPT, accept_key)
            .body(Body::empty())
            .unwrap())
    }

    /// Detach any existing connection to the given SP component's serial
    /// console.
    ///
    /// If there is an existing websocket connection to this SP component, it
    /// will be closed. If there isn't, this method does nothing.
    pub async fn serial_console_detach(
        &self,
        sp: SpIdentifier,
        component: &SpComponent,
    ) -> Result<(), Error> {
        let port = self.id_to_port(sp)?;
        self.recv_handler.serial_console_detach(port, component)
    }

    /// Send `packet` to the given SP component's serial console.
    pub(crate) async fn serial_console_send_packet(
        &self,
        port: SwitchPort,
        packet: SerialConsole,
        timeout: Instant,
    ) -> Result<(), Error> {
        // We can only send to an SP's serial console if we've attached to it,
        // which means we know its address.
        //
        // TODO how do we handle SP "disconnects"? If `self.switch` keeps the
        // old addr around and we send data into the ether until a reconnection
        // is established this is fine, but if it detects them and clears out
        // addresses this could panic and needs better handling.
        let sp =
            self.switch.sp_socket(port).expect("lost address of attached SP");

        self.request_response(
            &sp,
            RequestKind::SerialConsoleWrite(packet),
            Some(timeout),
            ResponseKindExt::try_into_serial_console_write_ack,
        )
        .await
    }

    /// Get the state of a given SP.
    pub async fn get_state(
        &self,
        sp: SpIdentifier,
        timeout: Instant,
    ) -> Result<SpState, Error> {
        self.get_state_maybe_timeout(sp, Some(timeout)).await
    }

    /// Get the state of a given SP without a timeout; it is the caller's
    /// responsibility to ensure a reasonable timeout is applied higher up in
    /// the chain.
    // TODO we could have one method that takes `Option<Instant>` for a timeout,
    // and/or apply that to _all_ the methods in this class. I don't want to
    // make it easy to accidentally call a method without providing a timeout,
    // though, so went with the current design.
    pub async fn get_state_without_timeout(
        &self,
        sp: SpIdentifier,
    ) -> Result<SpState, Error> {
        self.get_state_maybe_timeout(sp, None).await
    }

    async fn get_state_maybe_timeout(
        &self,
        sp: SpIdentifier,
        timeout: Option<Instant>,
    ) -> Result<SpState, Error> {
        let port = self.id_to_port(sp)?;
        let sp =
            self.switch.sp_socket(port).ok_or(Error::SpAddressUnknown(sp))?;
        let request = RequestKind::SpState;

        self.request_response(
            &sp,
            request,
            timeout,
            ResponseKindExt::try_into_sp_state,
        )
        .await
    }

    /// Query all online SPs.
    ///
    /// `ignition_state` should be the state returned by a (recent) call to
    /// [`get_ignition_state_all()`].
    ///
    /// All SPs included in `ignition_state` will be yielded by the returned
    /// stream. The order in which they are yielded is undefined; the offline
    /// SPs are likely to be first, but even that is not guaranteed. The item
    /// yielded by offline SPs will be `None`; the item yielded by online SPs
    /// will be `Some(Ok(_))` if the future returned by `f` for that item
    /// completed before `timeout` or `Some(Err(_))` if not.
    ///
    /// Note that the timeout is be applied to each _element_ of the returned
    /// stream rather than the stream as a whole, allowing easy access to which
    /// SPs timed out based on the yielded value associated with those SPs.
    pub fn query_all_online_sps<F, T, Fut>(
        &self,
        ignition_state: &[(SpIdentifier, IgnitionState)],
        timeout: Instant,
        f: F,
    ) -> impl FuturesUnorderedImpl<
        Item = (
            SpIdentifier,
            IgnitionState,
            Option<Result<T, tokio::time::error::Elapsed>>,
        ),
    >
    where
        F: FnMut(SpIdentifier) -> Fut + Clone,
        Fut: Future<Output = T>,
    {
        ignition_state
            .iter()
            .copied()
            .map(move |(id, state)| {
                let mut f = f.clone();
                async move {
                    let val = if state.is_powered_on() {
                        Some(tokio::time::timeout_at(timeout, f(id)).await)
                    } else {
                        None
                    };
                    (id, state, val)
                }
            })
            .collect::<FuturesUnordered<_>>()
    }

    async fn request_response<F, T>(
        &self,
        sp: &SpSocket<'_>,
        mut kind: RequestKind,
        timeout: Option<Instant>,
        mut map_response_kind: F,
    ) -> Result<T, Error>
    where
        F: FnMut(ResponseKind) -> Result<T, BadResponseType>,
    {
        // helper to wrap a future in a timeout if we have one
        async fn maybe_with_timeout<F, U>(
            timeout: Option<Instant>,
            fut: F,
        ) -> Result<U, tokio::time::error::Elapsed>
        where
            F: Future<Output = U>,
        {
            match timeout {
                Some(t) => tokio::time::timeout_at(t, fut).await,
                None => Ok(fut.await),
            }
        }

        // We'll use exponential backoff if and only if the SP responds with
        // "busy"; any other error will cause the loop below to terminate.
        let mut backoff = backoff::internal_service_policy();

        loop {
            // It would be nicer to use `backoff::retry()` instead of manually
            // stepping the backoff policy, but the dance we do with `kind` to
            // avoid cloning it is hard to map into `retry()` in a way that
            // satisfies the borrow checker. ("The dance we do with `kind` to
            // avoid cloning it" being that we move it into `request` below, and
            // on a busy response from the SP we move it back out into the
            // `kind` local var.)
            let duration = backoff
                .next_backoff()
                .expect("internal backoff policy gave up");
            maybe_with_timeout(timeout, tokio::time::sleep(duration)).await?;

            // request IDs will eventually roll over; since we enforce timeouts
            // this should be a non-issue in practice. does this need testing?
            let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);

            // update our recv_handler to expect a response for this request ID
            let response_fut =
                self.recv_handler.register_request_id(sp.port(), request_id);

            // Serialize and send our request. We know `buf` is large enough for
            // any `Request`, so unwrapping here is fine.
            let request = Request { version: version::V1, request_id, kind };
            let mut buf = [0; Request::MAX_SIZE];
            let n = gateway_messages::serialize(&mut buf, &request).unwrap();
            let serialized_request = &buf[..n];

            // Actual communication, guarded by `timeout` if it's not `None`.
            let result = maybe_with_timeout(timeout, async {
                debug!(&self.log, "sending {:?} to SP {:?}", request, sp);
                sp.send(serialized_request).await.map_err(|err| {
                    SpCommunicationError::UdpSend { addr: sp.addr(), err }
                })?;

                Ok::<ResponseKind, SpCommunicationError>(response_fut.await?)
            })
            .await?;

            match result {
                Ok(response_kind) => {
                    return map_response_kind(response_kind)
                        .map_err(SpCommunicationError::from)
                        .map_err(Error::from)
                }
                Err(SpCommunicationError::SpError(ResponseError::Busy)) => {
                    debug!(
                        &self.log,
                        "SP busy; sleeping before retrying send";
                        "sp" => ?sp,
                    );

                    // move `kind` back into local var; required to satisfy
                    // borrow check of this loop
                    kind = request.kind;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }
}

// When we send a request we expect a specific kind of response; the boilerplate
// for confirming that is a little noisy, so it lives in this extension trait.
trait ResponseKindExt {
    fn name(&self) -> &'static str;

    fn try_into_ignition_state(self) -> Result<IgnitionState, BadResponseType>;

    fn try_into_bulk_ignition_state(
        self,
    ) -> Result<BulkIgnitionState, BadResponseType>;

    fn try_into_ignition_command_ack(self) -> Result<(), BadResponseType>;

    fn try_into_sp_state(self) -> Result<SpState, BadResponseType>;

    fn try_into_serial_console_write_ack(self) -> Result<(), BadResponseType>;
}

impl ResponseKindExt for ResponseKind {
    fn name(&self) -> &'static str {
        match self {
            ResponseKind::Pong => response_kind_names::PONG,
            ResponseKind::IgnitionState(_) => {
                response_kind_names::IGNITION_STATE
            }
            ResponseKind::BulkIgnitionState(_) => {
                response_kind_names::BULK_IGNITION_STATE
            }
            ResponseKind::IgnitionCommandAck => {
                response_kind_names::IGNITION_COMMAND_ACK
            }
            ResponseKind::SpState(_) => response_kind_names::SP_STATE,
            ResponseKind::SerialConsoleWriteAck => {
                response_kind_names::SERIAL_CONSOLE_WRITE_ACK
            }
        }
    }

    fn try_into_ignition_state(self) -> Result<IgnitionState, BadResponseType> {
        match self {
            ResponseKind::IgnitionState(state) => Ok(state),
            other => Err(BadResponseType {
                expected: response_kind_names::IGNITION_STATE,
                got: other.name(),
            }),
        }
    }

    fn try_into_bulk_ignition_state(
        self,
    ) -> Result<BulkIgnitionState, BadResponseType> {
        match self {
            ResponseKind::BulkIgnitionState(state) => Ok(state),
            other => Err(BadResponseType {
                expected: response_kind_names::BULK_IGNITION_STATE,
                got: other.name(),
            }),
        }
    }

    fn try_into_ignition_command_ack(self) -> Result<(), BadResponseType> {
        match self {
            ResponseKind::IgnitionCommandAck => Ok(()),
            other => Err(BadResponseType {
                expected: response_kind_names::IGNITION_COMMAND_ACK,
                got: other.name(),
            }),
        }
    }

    fn try_into_sp_state(self) -> Result<SpState, BadResponseType> {
        match self {
            ResponseKind::SpState(state) => Ok(state),
            other => Err(BadResponseType {
                expected: response_kind_names::SP_STATE,
                got: other.name(),
            }),
        }
    }

    fn try_into_serial_console_write_ack(self) -> Result<(), BadResponseType> {
        match self {
            ResponseKind::SerialConsoleWriteAck => Ok(()),
            other => Err(BadResponseType {
                expected: response_kind_names::SP_STATE,
                got: other.name(),
            }),
        }
    }
}

mod response_kind_names {
    pub(super) const PONG: &str = "pong";
    pub(super) const IGNITION_STATE: &str = "ignition_state";
    pub(super) const BULK_IGNITION_STATE: &str = "bulk_ignition_state";
    pub(super) const IGNITION_COMMAND_ACK: &str = "ignition_command_ack";
    pub(super) const SP_STATE: &str = "sp_state";
    pub(super) const SERIAL_CONSOLE_WRITE_ACK: &str =
        "serial_console_write_ack";
}
