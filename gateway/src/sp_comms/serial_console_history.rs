// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_messages::{SerialConsole, SpComponent};
use ringbuffer::{AllocRingBuffer, RingBufferExt, RingBufferWrite};
use schemars::JsonSchema;
use serde::Serialize;
use slog::{warn, Logger};
use std::{cmp::Ordering, collections::HashMap};

/// Current in-memory contents of an SP component's serial console.
///
/// Note that we currently embed a placeholder string for missing data, which
/// adds some nuance around what the "length" of this buffer means: We have both
/// the length in bytes of the buffer itself, but also the nominal length of
/// what range of data from the SP it covers. If we haven't missed any packets,
/// these lengths will be the same. If we have, they will probably be different;
/// typically the nominal length will be longer, but it could be shorter if we
/// missed packets containing chunks shorter than the placeholder string we drop
/// in in their place(s).
///
/// If we have not received any serial console data from this SP component,
/// `start` and `end` will both be `0`, and `buf` will be empty.
// TODO is it a code smell that this type impls Serialize and JsonSchema but
// isn't defined in `http_entrypoints.rs`?
#[derive(Debug, Default, Serialize, JsonSchema)]
pub(crate) struct SerialConsoleContents {
    /// Position since SP component start of the first byte of `buf`.
    ///
    /// This is equal to the number of bytes that we've discarded due to
    /// dropping out of our internal ring buffer.
    pub(crate) start: u64,

    /// Nominal end of the data covered by `buf`.
    ///
    /// If we have not missed any packets since `start`, this will be equal to
    /// `start + data.len()`. If we _have_ missed at least one packet, those
    /// lengths are likely different.
    pub(crate) end: u64,

    /// Contents of the serial console buffer.
    pub(crate) buf: Vec<u8>,
}

// We currently store serial console packets from an SP more or less "as is" in
// a ringbuffer per component. It might be better to keep a ringbuffer backed by
// a `Vec<u8>` to make querying the current serial console state simpler, but
// (a) I'm not aware of a nice ringbuffer API that would let us push in chunks
// of data, and (b) it makes managing gaps in the data more complicated. This
// seems good enough for now (and possibly for the foreseeable future).
#[derive(Debug, Default)]
pub(super) struct SerialConsoleHistory {
    by_component: HashMap<SpComponent, AllocRingBuffer<Slot>>,
}

impl SerialConsoleHistory {
    pub(super) fn contents(
        &self,
        component: &SpComponent,
    ) -> Option<SerialConsoleContents> {
        let slots = self.by_component.get(component)?;
        let mut buf = Vec::new();
        let mut start = None;
        let mut end = None;

        for slot in slots.iter() {
            match slot {
                Slot::MissingData { offset, len } => {
                    buf.extend_from_slice(
                        format!("... MISSING {} BYTES ...", len).as_bytes(),
                    );
                    if start.is_none() {
                        start = Some(*offset);
                    }
                    end = Some(offset + len);
                }
                Slot::Valid { offset, len, data } => {
                    buf.extend_from_slice(&data[..usize::from(*len)]);
                    if start.is_none() {
                        start = Some(*offset);
                    }
                    end = Some(offset + u64::from(*len));
                }
            }
        }

        Some(SerialConsoleContents {
            start: start.unwrap_or(0),
            end: end.unwrap_or(0),
            buf,
        })
    }

    pub(super) fn push(&mut self, packet: SerialConsole, log: &Logger) {
        // TODO do we want this capacity to be configurable, or just pick
        // something small but reasonable?
        const NUM_RINGBUFFER_SLOTS: usize = 32;

        // TODO We're assuming the SP will only send us components it should and
        // are happy to blindly accept its component IDs. Is this right, or
        // should we limit this (if so, based on what?)
        let slots =
            self.by_component.entry(packet.component).or_insert_with(|| {
                AllocRingBuffer::with_capacity(NUM_RINGBUFFER_SLOTS)
            });

        // detect dropped packets - see what we expect `packet.offset` to be
        // based on the end of our most-recently-received packet.
        let expected_offset = slots.back().map_or(0, |slot| slot.end_pos());

        match packet.offset.cmp(&expected_offset) {
            Ordering::Less => {
                // TODO We're currently assuming that a packet offset earlier
                // than what we expect implies we're now receiving an
                // out-of-order back that we missed earlier. This is
                // demonstrably wrong in at least one case (the SP has
                // restarted), but we're punting on that problem for now. It
                // seems likely that an SP restarting is going to end up
                // triggering a lot of state reset (reestablish an authorized
                // connection, for one) which would give us a chance to reset
                // our serial console state too.
                warn!(
                    log,
                    "dropping serial console packet with out-of-order offset"
                );
            }
            Ordering::Greater => {
                // we have a gap; push a "missing data" entry
                slots.push(Slot::MissingData {
                    offset: expected_offset,
                    len: packet.offset - expected_offset,
                });
            }
            Ordering::Equal => (), // nothing to do; this is expected
        }

        slots.push(Slot::Valid {
            offset: packet.offset,
            len: packet.len,
            data: packet.data,
        });
    }
}

#[derive(Debug)]
enum Slot {
    // we had dropped our out-of-order packets amounting to this number of bytes
    MissingData {
        offset: u64,
        len: u64,
    },
    // an in-order serial console packet
    Valid {
        offset: u64,
        len: u8,
        data: [u8; SerialConsole::MAX_DATA_PER_PACKET],
    },
}

impl Slot {
    fn end_pos(&self) -> u64 {
        match self {
            Slot::MissingData { offset, len } => offset + len,
            Slot::Valid { offset, len, .. } => offset + u64::from(*len),
        }
    }
}
