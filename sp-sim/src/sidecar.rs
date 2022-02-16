// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{server::UdpServer, Config};
use anyhow::Result;
use gateway_messages::sp_impl::{SpHandler, SpServer};
use gateway_messages::{IgnitionFlags, IgnitionState, ResponseKind};
use tokio::{
    select,
    task::{self, JoinHandle},
};

pub struct Sidecar {
    inner_task: JoinHandle<()>,
}

impl Drop for Sidecar {
    fn drop(&mut self) {
        // default join handle drop behavior is to detach; we want to abort
        self.inner_task.abort();
    }
}

impl Sidecar {
    pub async fn spawn(config: &Config) -> Result<Self> {
        let server = UdpServer::new(config).await?;
        let inner = Inner::new(server);
        let inner_task = task::spawn(async move { inner.run().await.unwrap() });
        Ok(Self { inner_task })
    }
}

struct Handler;

impl SpHandler for Handler {
    fn ping(&mut self) -> ResponseKind {
        println!("received ping; sending pong");
        ResponseKind::Pong
    }

    fn ignition_state(&mut self, target: u8) -> ResponseKind {
        const SIDECAR_ID: u8 = 0b01_0010;

        let state = IgnitionState {
            id: SIDECAR_ID,
            flags: IgnitionFlags::POWER | IgnitionFlags::CTRL_DETECT_0,
        };

        println!(
            "received ignition state request for {}; sending {:?}",
            target, state
        );
        ResponseKind::IgnitionState(state)
    }

    fn ignition_command(
        &mut self,
        target: u8,
        command: gateway_messages::IgnitionCommand,
    ) -> ResponseKind {
        println!(
            "received ignition command {:?} for target {}; sending ack",
            command, target
        );
        ResponseKind::IgnitionCommandAck
    }

    fn sp_message_ack(&mut self, _msg_id: u32) {
        todo!()
    }
}

struct Inner {
    server: UdpServer,
    handler: SpServer<Handler>,
}

impl Inner {
    fn new(server: UdpServer) -> Self {
        Self { server, handler: SpServer::new(Handler) }
    }

    async fn run(mut self) -> Result<()> {
        loop {
            select! {
                recv = self.server.recv_from() => {
                    let (data, addr) = recv?;

                    let resp = match self.handler.dispatch(data) {
                        Ok(resp) => resp,
                        // TODO: should we send back an error here? may not be
                        // able to say anything meaningful, depending on `err`
                        Err(err) => {
                            println!("dispatching message failed: {:?}", err);
                            continue;
                        }
                    };

                    self.server.send_to(resp, addr).await?;
                }
            }
        }
    }
}
