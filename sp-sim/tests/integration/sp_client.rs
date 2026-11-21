// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_messages::Header;
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionState;
use gateway_messages::MAX_SERIALIZED_SIZE;
use gateway_messages::Message;
use gateway_messages::MessageKind;
use gateway_messages::MgsRequest;
use gateway_messages::SpPort;
use gateway_messages::SpResponse;
use gateway_messages::ignition::SystemPowerState;
use gateway_messages::ignition::SystemType;
use sp_sim::SimulatedSp;
use std::net::Ipv6Addr;
use std::time::Duration;
use tokio::net::UdpSocket;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

/// A small client used to interact with a simulated SP.
pub(crate) struct SpClient {
    port: SpPort,
    sock: UdpSocket,
    next_message_id: u32,
}

impl SpClient {
    pub(crate) async fn connect<S: SimulatedSp>(sp: &S, port: SpPort) -> Self {
        let addr = sp
            .local_addr(port)
            .unwrap_or_else(|| panic!("simulated SP listens on {port:?}"));
        let sock = UdpSocket::bind((Ipv6Addr::LOCALHOST, 0))
            .await
            .expect("bound UDP socket on localhost");
        sock.connect(addr)
            .await
            .expect("connected UDP socket to the simulated SP");
        Self { port, sock, next_message_id: 0 }
    }

    async fn send(&mut self, request: MgsRequest) -> u32 {
        let message_id = self.next_message_id;
        self.next_message_id += 1;
        let message = Message {
            header: Header {
                version: gateway_messages::version::CURRENT,
                message_id,
            },
            kind: MessageKind::MgsRequest(request),
        };
        let mut buf = [0u8; MAX_SERIALIZED_SIZE];
        let n = gateway_messages::serialize(&mut buf, &message)
            .expect("serialized MGS request");
        self.sock.send(&buf[..n]).await.expect("sent MGS request");
        message_id
    }

    async fn request(&mut self, request: MgsRequest) -> SpResponse {
        let message_id = self.send(request).await;
        let mut buf = [0u8; MAX_SERIALIZED_SIZE];
        let len =
            tokio::time::timeout(RESPONSE_TIMEOUT, self.sock.recv(&mut buf))
                .await
                .unwrap_or_else(|_elapsed| {
                    panic!(
                        "simulated SP did not answer {request:?} on {:?} \
                         within the {RESPONSE_TIMEOUT:?} response timeout",
                        self.port,
                    )
                })
                .expect("received SP response");
        let (message, _trailing) =
            gateway_messages::deserialize::<Message>(&buf[..len])
                .expect("deserialized SP response");
        assert_eq!(
            message.header.message_id, message_id,
            "response message id matches the id of {request:?}",
        );
        match message.kind {
            MessageKind::SpResponse(response) => response,
            MessageKind::MgsRequest(_)
            | MessageKind::MgsResponse(_)
            | MessageKind::SpRequest(_) => {
                panic!("expected an SP response, got {:?}", message.kind)
            }
        }
    }

    pub(crate) async fn ignition_command(
        &mut self,
        target: u8,
        command: IgnitionCommand,
    ) {
        let response =
            self.request(MgsRequest::IgnitionCommand { target, command }).await;
        let SpResponse::IgnitionCommandAck = response else {
            panic!("expected an ignition command ack, got {response:?}");
        };
    }

    pub(crate) async fn assert_power(
        &mut self,
        target: u8,
        system_type: SystemType,
        expected: SystemPowerState,
    ) {
        let response = self.request(MgsRequest::IgnitionState { target }).await;
        let SpResponse::IgnitionState(IgnitionState {
            target: Some(state),
            ..
        }) = response
        else {
            panic!(
                "expected ignition target {target} to be present, \
                 got {response:?}"
            );
        };
        assert_eq!(state.system_type, system_type, "type of target {target}");
        assert_eq!(state.power_state, expected, "power of target {target}");
    }
}
