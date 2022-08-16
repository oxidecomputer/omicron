// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::recv_sp_message_ignoring_serial_console;
use crate::send_request;
use crate::RecvError;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use gateway_messages::RequestKind;
use gateway_messages::ResponseError;
use gateway_messages::ResponseKind;
use gateway_messages::UpdateChunk;
use gateway_messages::UpdateStart;
use slog::debug;
use slog::info;
use slog::warn;
use slog::Logger;
use std::fs;
use std::io;
use std::net::SocketAddrV6;
use std::net::UdpSocket;
use std::path::Path;

pub(crate) fn run(
    log: Logger,
    socket: UdpSocket,
    sp: SocketAddrV6,
    image: &Path,
) -> Result<()> {
    let image = fs::read(image)
        .with_context(|| format!("failed to read image {}", image.display()))?;

    let total_size = image.len().try_into().with_context(|| {
        format!("image length ({}) overflows total_size field", image.len())
    })?;
    let start = UpdateStart { total_size };

    info!(log, "sending UpdateStart to SP");
    update_with_retry(&log, &socket, sp, RequestKind::UpdateStart(start), 0)
        .with_context(|| "starting update failed")?;
    info!(log, "SP acknowledged update start");

    for (i, data) in image.chunks(UpdateChunk::MAX_CHUNK_SIZE).enumerate() {
        let mut chunk = UpdateChunk {
            offset: (i * UpdateChunk::MAX_CHUNK_SIZE)
                .try_into()
                .with_context(|| "update size overflowed u32")?,
            chunk_length: data.len() as u16,
            data: [0; UpdateChunk::MAX_CHUNK_SIZE],
        };
        chunk.data[..data.len()].copy_from_slice(data);
        let end_chunk = chunk.offset + u32::from(chunk.chunk_length);

        update_with_retry(
            &log,
            &socket,
            sp,
            RequestKind::UpdateChunk(chunk),
            end_chunk,
        )
        .with_context(|| "update failed partially complete")?;
        debug!(
            log,
            "SP update progress ({} of {} bytes)",
            end_chunk,
            image.len()
        );
    }
    info!(log, "SP update complete");

    Ok(())
}

fn update_with_retry(
    log: &Logger,
    socket: &UdpSocket,
    sp: SocketAddrV6,
    request: RequestKind,
    expected_progress: u32,
) -> Result<()> {
    const MAX_ATTEMPTS: usize = 5;

    for retry in 0..MAX_ATTEMPTS {
        if retry > 0 {
            info!(
                log,
                "update timeout; retrying ({retry} of {})",
                MAX_ATTEMPTS - 1
            );
        }
        let request_id = send_request(log, socket, sp, request.clone())?;

        loop {
            let result =
                match recv_sp_message_ignoring_serial_console(log, socket) {
                    Ok((response_id, result)) => {
                        if request_id == response_id {
                            result
                        } else {
                            warn!(
                                log, "ignoring unexpected response id";
                                "response_id" => response_id,
                            );
                            continue;
                        }
                    }
                    Err(RecvError::Io(err))
                        if err.kind() == io::ErrorKind::WouldBlock =>
                    {
                        break;
                    }
                    Err(err) => return Err(err.into()),
                };

            match result {
                Ok(ResponseKind::UpdateStartAck) if expected_progress == 0 => {
                    return Ok(());
                }
                Ok(ResponseKind::UpdateChunkAck) if expected_progress > 0 => {
                    return Ok(());
                }
                Err(ResponseError::UpdateInProgress { bytes_received })
                    if expected_progress == bytes_received =>
                {
                    return Ok(());
                }
                other => bail!("unexpected response to update: {other:?}"),
            }
        }
    }

    bail!("update failed (giving up after {MAX_ATTEMPTS} attempts)")
}
