// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// A queue of bytes that can selectively act as a reader or writer,
/// which can also be cloned.
///
/// This is primarily used to emulate stdin / stdout / stderr.
#[derive(Clone)]
pub struct ByteQueue {
    buf: Arc<Mutex<VecDeque<u8>>>,
}

impl ByteQueue {
    pub fn new() -> Self {
        Self { buf: Arc::new(Mutex::new(VecDeque::new())) }
    }
}

impl std::io::Write for ByteQueue {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl std::io::Read for ByteQueue {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.buf.lock().unwrap().read(buf)
    }
}
