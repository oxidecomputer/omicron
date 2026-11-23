// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_messages::DumpCompression;
use gateway_messages::DumpError;
use gateway_messages::DumpSegment;
use gateway_messages::DumpTask;
use gateway_messages::SpError;
use std::collections::HashMap;

/// A simulated SP's task dumps.
///
/// The simulated SP always has exactly one task dump, which consists of three
/// identical segments, each containing the same fixed LZSS-compressed message.
#[derive(Default)]
pub(crate) struct TaskDumps {
    /// The next expected sequence number for each in-progress read, by key.
    dumps: HashMap<[u8; 16], u32>,
}

impl TaskDumps {
    pub(crate) fn get_task_dump_count(&self) -> Result<u32, SpError> {
        Ok(1)
    }

    pub(crate) fn task_dump_read_start(
        &mut self,
        index: u32,
        key: [u8; 16],
    ) -> Result<DumpTask, SpError> {
        if index != 0 {
            return Err(SpError::Dump(DumpError::BadIndex));
        }

        // Hubris allows clients to reuse existing keys.
        // Overwrite any in-flight requests using this key.
        self.dumps.insert(key, 0);

        Ok(DumpTask { time: 1, task: 0, compression: DumpCompression::Lzss })
    }

    pub(crate) fn task_dump_read_continue(
        &mut self,
        key: [u8; 16],
        seq: u32,
        buf: &mut [u8],
    ) -> Result<Option<DumpSegment>, SpError> {
        const UNCOMPRESSED_MSG: &[u8] = b"my cool SP dump";
        // "my cool SP dump" encoded with `lzss-cli e 6,4,0x20`
        const COMPRESSED_MSG: &[u8] = &[
            0xb6, 0xde, 0x64, 0x16, 0x3b, 0x7d, 0xbe, 0xd9, 0x20, 0xa9, 0xd4,
            0x24, 0x16, 0x4b, 0xad, 0xb6, 0xe0,
        ];

        let Some(expected_seq) = self.dumps.get_mut(&key) else {
            return Err(SpError::Dump(DumpError::BadKey));
        };

        if seq != *expected_seq {
            return Err(SpError::Dump(DumpError::BadSequenceNumber));
        }

        buf[..COMPRESSED_MSG.len()].copy_from_slice(COMPRESSED_MSG);

        *expected_seq += 1;

        match seq {
            ..3 => Ok(Some(DumpSegment {
                address: 1,
                compressed_length: COMPRESSED_MSG.len() as u16,
                uncompressed_length: UNCOMPRESSED_MSG.len() as u16,
                seq,
            })),
            3.. => {
                self.dumps.remove(&key);
                Ok(None)
            }
        }
    }
}
