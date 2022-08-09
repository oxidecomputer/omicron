// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::recv_sp_message_ignoring_serial_console;
use crate::request_response;
use crate::send_request;
use crate::RecvError;
use anyhow::bail;
use anyhow::Result;
use gateway_messages::RequestKind;
use gateway_messages::ResponseError;
use gateway_messages::ResponseKind;
use slog::info;
use slog::Logger;
use std::io;
use std::net::SocketAddrV6;
use std::net::UdpSocket;

pub(crate) fn run(
    log: Logger,
    socket: UdpSocket,
    sp: SocketAddrV6,
) -> Result<()> {
    // Tell SP to prepare to reset.
    let response =
        request_response(&log, &socket, sp, RequestKind::SysResetPrepare)??;
    match response {
        ResponseKind::SysResetPrepareAck => (),
        other => bail!("unexpected SP response: {other:?}"),
    }
    info!(log, "SP is prepared to reset");

    // We won't get an ack to a reset trigger; instead, keep sending trigger
    // resets until we get back the error we expect from sending a reset trigger
    // to a recently-rebooted SP.
    for retry in 1..=5 {
        info!(log, "sending reset trigger (attempt {retry})");
        send_request(&log, &socket, sp, RequestKind::SysResetTrigger)?;

        let result =
            match recv_sp_message_ignoring_serial_console(&log, &socket) {
                Ok((_request_id, result)) => result,
                Err(RecvError::Io(err))
                    if err.kind() == io::ErrorKind::WouldBlock =>
                {
                    continue;
                }
                Err(err) => return Err(err.into()),
            };

        match result {
            Err(ResponseError::SysResetTriggerWithoutPrepare) => {
                info!(log, "SP reset complete");
                return Ok(());
            }
            other => bail!("unexpected SP response: {other:?}"),
        };
    }

    bail!("failed to confirm reset")
}
