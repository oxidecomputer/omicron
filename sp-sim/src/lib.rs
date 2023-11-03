// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod config;
mod gimlet;
mod helpers;
mod rot;
mod server;
mod sidecar;
mod update;

pub use anyhow::Result;
use async_trait::async_trait;
pub use config::Config;
use gateway_messages::SpPort;
pub use gimlet::Gimlet;
pub use gimlet::SIM_GIMLET_BOARD;
pub use server::logger;
pub use sidecar::Sidecar;
pub use sidecar::SIM_SIDECAR_BOARD;
pub use slog::Logger;
pub use sprockets_rot::common::msgs::RotRequestV1;
pub use sprockets_rot::common::msgs::RotResponseV1;
use sprockets_rot::common::Ed25519PublicKey;
pub use sprockets_rot::RotSprocketError;
use std::net::SocketAddrV6;
use tokio::sync::mpsc;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Responsiveness {
    Responsive,
    Unresponsive,
}

#[async_trait]
pub trait SimulatedSp {
    /// Serial number.
    async fn state(&self) -> omicron_gateway::http_entrypoints::SpState;

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

    /// Get the last completed update delivered to this simulated SP.
    ///
    /// Only returns data after a simulated reset.
    async fn last_update_data(&self) -> Option<Box<[u8]>>;

    /// Get the current update status, just as would be returned by an MGS
    /// request to get the update status.
    async fn current_update_status(&self) -> gateway_messages::UpdateStatus;

    /// Get a watch channel on which this simulated SP will publish a
    /// monotonically increasing count of how many responses it has successfully
    /// sent.
    ///
    /// Returns `None` if called before the SP has set up its sockets to handle
    /// requests.
    fn responses_sent_count(&self) -> Option<watch::Receiver<usize>>;

    /// Inject a UDP-level "throttler" on this simualted SP.
    ///
    /// If this method is not called, the SP will handle requests as they come
    /// in.
    ///
    /// When this method is called, it will set its "throttle count" to zero.
    /// When the trottle count is zero, the SP will not accept incoming UDP
    /// packets. When a value is sent on the channel returned by this method,
    /// that value will be added to the throttle count. When the SP successfully
    /// sends a response to a message, the throttle count will be decremented by
    /// one.
    ///
    /// Two example use cases for this method are that a caller could:
    ///
    /// * Artificially slow down the simulated SP (e.g., increment the throttle
    ///   count by N every M millis)
    /// * Force the simulated SP to single-step message (e.g., by incrementing
    ///   the throttle count by 1 and then waiting for a message to be received,
    ///   which can be observed via `responses_sent_count`)
    async fn install_udp_throttler(&self) -> mpsc::UnboundedSender<usize>;
}

// Helper function to pad a simulated serial number (stored as a `String`) to
// the appropriate size for returning in the SpState message.
fn serial_number_padded(serial_number: &str) -> [u8; 32] {
    let mut padded = [0; 32];
    padded
        .get_mut(0..serial_number.len())
        .expect("simulated serial number too long")
        .copy_from_slice(serial_number.as_bytes());
    padded
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
