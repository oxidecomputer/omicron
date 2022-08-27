// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A sprockets server for handling bootstrap related requests
//!
//! This is the server for trust-quorum rack unlock as well as
//! the server backing the 2PC implementation used for trust quorum initialization,
//! trust quorum reconfiguration, and NetworkConfiguration needed to configure NTP.

use slog::Drain;
use slog::Logger;
use std::io;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;

use super::db::Db;
use crate::sp::SimSpConfig;
use crate::sp::SpHandle;

/// The sprockets server for the bootstore
///
/// The Server is in charge of managing the SP, key shares, and early boot
/// network configuration.
pub struct Server {
    listener: TcpListener,
    bind_address: SocketAddrV6,
    sp: SpHandle,
    log: Logger,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot bind to {bind_address}: {err}")]
    Bind { bind_address: SocketAddrV6, err: io::Error },
}

impl Server {
    async fn start(
        log: Logger,
        bind_address: SocketAddrV6,
        sp: SpHandle,
    ) -> Result<JoinHandle<Result<(), Error>>, Error> {
        let listener = TcpListener::bind(bind_address)
            .await
            .map_err(|err| Error::Bind { bind_address, err })?;
        info!(log, "Started listening"; "local_addr" => %bind_address);
        let server = Server { listener, sp, bind_address, log };
        Ok(tokio::spawn(server.run()))
    }

    async fn run(self) -> Result<(), Error> {}
}
