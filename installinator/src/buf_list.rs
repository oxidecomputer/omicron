// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
//   linkerd2-proxy is copyright 2018 the linkerd2-proxy authors. All rights reserved.
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may not use
//   these files except in compliance with the License. You may obtain a copy of the
//   License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software distributed
//   under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
//   CONDITIONS OF ANY KIND, either express or implied. See the License for the
//   specific language governing permissions and limitations under the License.

use std::{collections::VecDeque, io::IoSlice};

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Data composed of multiple `Bytes` chunks.
///
/// TODO: maybe turn this into its own crate
#[derive(Clone, Debug, Default)]
pub struct BufList {
    // Invariant: none of the bufs in this queue are zero-length.
    bufs: VecDeque<Bytes>,
}

impl BufList {
    pub(crate) fn new() -> Self {
        Self { bufs: VecDeque::new() }
    }

    #[inline]
    pub(crate) fn num_bytes(&self) -> usize {
        self.remaining()
    }

    pub(crate) fn push_chunk(&mut self, mut data: impl Buf) -> Bytes {
        let len = data.remaining();
        // `data` is (almost) certainly a `Bytes`, so `copy_to_bytes` should
        // internally be a cheap refcount bump almost all of the time.
        // But, if it isn't, this will copy it to a `Bytes` that we can
        // now clone.
        let bytes = data.copy_to_bytes(len);

        // Buffer a clone of the bytes read on this poll.
        // Don't push zero-length bufs to uphold the invariant.
        if len > 0 {
            self.bufs.push_back(bytes.clone());
        }

        // Return the bytes
        bytes
    }
}

impl Buf for BufList {
    fn remaining(&self) -> usize {
        self.bufs.iter().map(Buf::remaining).sum()
    }

    fn chunk(&self) -> &[u8] {
        self.bufs.front().map(Buf::chunk).unwrap_or(&[])
    }

    fn chunks_vectored<'iovs>(
        &'iovs self,
        iovs: &mut [IoSlice<'iovs>],
    ) -> usize {
        // Are there more than zero iovecs to write to?
        if iovs.is_empty() {
            return 0;
        }

        // Loop over the buffers in the replay buffer list, and try to fill as
        // many iovecs as we can from each buffer.
        let mut filled = 0;
        for buf in &self.bufs {
            filled += buf.chunks_vectored(&mut iovs[filled..]);
            if filled == iovs.len() {
                return filled;
            }
        }

        filled
    }

    fn advance(&mut self, mut amt: usize) {
        while amt > 0 {
            let rem = self.bufs[0].remaining();
            // If the amount to advance by is less than the first buffer in
            // the buffer list, advance that buffer's cursor by `amt`,
            // and we're done.
            if rem > amt {
                self.bufs[0].advance(amt);
                return;
            }

            // Otherwise, advance the first buffer to its end, and
            // continue.
            self.bufs[0].advance(rem);
            amt -= rem;

            self.bufs.pop_front();
        }
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        // If the length of the requested `Bytes` is <= the length of the front
        // buffer, we can just use its `copy_to_bytes` implementation (which is
        // just a reference count bump).
        match self.bufs.front_mut() {
            Some(first) if len <= first.remaining() => {
                let buf = first.copy_to_bytes(len);
                // If we consumed the first buffer, also advance our "cursor" by
                // popping it.
                if first.remaining() == 0 {
                    self.bufs.pop_front();
                }

                buf
            }
            _ => {
                assert!(
                    len <= self.remaining(),
                    "`len` greater than remaining"
                );
                let mut buf = BytesMut::with_capacity(len);
                buf.put(self.take(len));
                buf.freeze()
            }
        }
    }
}
