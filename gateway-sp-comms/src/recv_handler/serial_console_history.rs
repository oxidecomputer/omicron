// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use gateway_messages::SerialConsole;
use gateway_messages::SpComponent;
use ringbuffer::AllocRingBuffer;
use ringbuffer::RingBufferExt;
use ringbuffer::RingBufferWrite;
use slog::warn;
use slog::Logger;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem;

/// Current in-memory contents of an SP component's serial console.
///
/// If we have not received any serial console data from this SP component,
/// `start` will be `0` and `chunks` will be empty.
#[derive(Debug, PartialEq)]
pub struct SerialConsoleContents {
    /// Position since SP component start of the first byte of the first element
    /// of `chunks`.
    ///
    /// This is equal to the number of bytes that we've discarded since the SP
    /// started.
    pub start: u64,

    /// Chunks of serial console data.
    ///
    /// We collapse contiguous regions of data (present or missing) into a
    /// single chunk: any two consecutive elements of `chunks` are guaranteed to
    /// be different (i.e., one will be [`SerialConsoleChunk::Missing`] and the
    /// other will be [`SerialConsoleChunk::Data`]). If `chunks` is not empty,
    /// its final element is guaranteed to be a [`SerialConsoleChunk::Data`].
    pub chunks: Vec<SerialConsoleChunk>,
}

/// A chunk of serial console data: either actual data, or an amount of data
/// we missed (presumably due to dropped packets or something similar).
#[derive(Debug, PartialEq)]
pub enum SerialConsoleChunk {
    Data { bytes: Vec<u8> },
    Missing { len: u64 },
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
        let mut chunks = Vec::new();
        let mut buf = Vec::new();
        let mut start = None;

        for slot in slots.iter() {
            match slot {
                &Slot::MissingData { offset, len } => {
                    if !buf.is_empty() {
                        // we've collected some amount of contiguous data; add a
                        // chunk and reset `buf` before appending our missing
                        // chunk
                        let mut steal = Vec::new();
                        mem::swap(&mut buf, &mut steal);
                        chunks.push(SerialConsoleChunk::Data { bytes: steal });
                    }
                    chunks.push(SerialConsoleChunk::Missing { len });
                    if start.is_none() {
                        start = Some(offset);
                    }
                }
                &Slot::Valid { offset, len, data } => {
                    buf.extend_from_slice(&data[..usize::from(len)]);
                    if start.is_none() {
                        start = Some(offset);
                    }
                }
            }
        }

        if !buf.is_empty() {
            chunks.push(SerialConsoleChunk::Data { bytes: buf });
        }

        Some(SerialConsoleContents { start: start.unwrap_or(0), chunks })
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
        len: u16,
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

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use std::convert::TryFrom;

    static COMPONENT: Lazy<SpComponent> =
        Lazy::new(|| SpComponent::try_from("test").unwrap());

    fn make_packet(offset: usize, data: &str) -> SerialConsole {
        let mut packet = SerialConsole {
            component: *COMPONENT,
            offset: u64::try_from(offset).unwrap(),
            len: u16::try_from(data.len()).unwrap(),
            data: [0; SerialConsole::MAX_DATA_PER_PACKET],
        };
        packet.data[..data.len()].copy_from_slice(data.as_bytes());
        packet
    }

    #[test]
    fn consecutive_packets_are_coalesced() {
        let mut hist = SerialConsoleHistory::default();
        let log = Logger::root(slog::Discard, slog::o!());

        // push first packet
        hist.push(make_packet(0, "hello "), &log);
        let contents = hist.contents(&COMPONENT).unwrap();
        assert_eq!(
            contents,
            SerialConsoleContents {
                start: 0,
                chunks: vec![SerialConsoleChunk::Data {
                    bytes: b"hello ".to_vec()
                }]
            }
        );

        // push second packet with an offset that ends at first
        hist.push(make_packet(6, "world"), &log);
        let contents = hist.contents(&COMPONENT).unwrap();
        assert_eq!(
            contents,
            SerialConsoleContents {
                start: 0,
                chunks: vec![SerialConsoleChunk::Data {
                    bytes: b"hello world".to_vec()
                }]
            }
        );
    }

    #[test]
    fn skipped_data_leaves_gaps() {
        let mut hist = SerialConsoleHistory::default();
        let log = Logger::root(slog::Discard, slog::o!());

        // push first packet
        hist.push(make_packet(0, "hello "), &log);
        let contents = hist.contents(&COMPONENT).unwrap();
        assert_eq!(
            contents,
            SerialConsoleContents {
                start: 0,
                chunks: vec![SerialConsoleChunk::Data {
                    bytes: b"hello ".to_vec()
                }]
            }
        );

        // push second packet with an offset that is 5 bytes past the end of the
        // first packet (i.e., 5 bytes were lost)
        hist.push(make_packet(6 + 5, "world"), &log);
        let contents = hist.contents(&COMPONENT).unwrap();
        assert_eq!(
            contents,
            SerialConsoleContents {
                start: 0,
                chunks: vec![
                    SerialConsoleChunk::Data { bytes: b"hello ".to_vec() },
                    SerialConsoleChunk::Missing { len: 5 },
                    SerialConsoleChunk::Data { bytes: b"world".to_vec() },
                ]
            }
        );
    }
}
