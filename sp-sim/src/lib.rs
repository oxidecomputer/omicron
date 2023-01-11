// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod config;
mod gimlet;
mod rot;
mod server;
mod sidecar;

pub use anyhow::Result;
use async_trait::async_trait;
pub use config::Config;
use gateway_messages::SpPort;
pub use gimlet::Gimlet;
pub use server::logger;
pub use sidecar::Sidecar;
pub use slog::Logger;
pub use sprockets_rot::common::msgs::RotRequestV1;
pub use sprockets_rot::common::msgs::RotResponseV1;
use sprockets_rot::common::Ed25519PublicKey;
pub use sprockets_rot::RotSprocketError;
use std::net::SocketAddrV6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Responsiveness {
    Responsive,
    Unresponsive,
}

#[async_trait]
pub trait SimulatedSp {
    /// Hexlified serial number.
    fn serial_number(&self) -> String;

    /// Public key for the manufacturing cert used to sign this SP's RoT certs.
    fn manufacturing_public_key(&self) -> Ed25519PublicKey;

    /// Listening UDP address of the given port of this simulated SP, if it was
    /// configured to listen.
    fn local_addr(&self, port: SpPort) -> Option<SocketAddrV6>;

    /// Simulate the SP being unresponsive, in which it ignores all incoming
    /// messages.
    async fn set_responsiveness(&self, r: Responsiveness);

    /// Send a request to the (simulated) RoT.
    fn rot_request(
        &self,
        request: RotRequestV1,
    ) -> Result<RotResponseV1, RotSprocketError>;
}

pub struct SimRack {
    pub sidecars: Vec<Sidecar>,
    pub gimlets: Vec<Gimlet>,
}

impl SimRack {
    pub async fn start(config: &Config, log: &Logger) -> Result<Self> {
        let mut sidecars =
            Vec::with_capacity(config.simulated_sps.sidecar.len());
        for (i, sidecar) in config.simulated_sps.sidecar.iter().enumerate() {
            sidecars.push(
                Sidecar::spawn(
                    config,
                    sidecar,
                    log.new(slog::o!("slot" => format!("sidecar {}", i))),
                )
                .await?,
            );
        }

        let mut gimlets = Vec::with_capacity(config.simulated_sps.gimlet.len());
        for (i, gimlet) in config.simulated_sps.gimlet.iter().enumerate() {
            gimlets.push(
                Gimlet::spawn(
                    gimlet,
                    log.new(slog::o!("slot" => format!("gimlet {}", i))),
                )
                .await?,
            );
        }

        Ok(Self { sidecars, gimlets })
    }

    pub fn ignition_controller(&self) -> &Sidecar {
        // This simulator exists to test MGS, which only makes sense with a
        // sidecar in place. We'll assume we're always configured with at least
        // one, and panic if that's wrong.
        &self.sidecars[0]
    }
}
