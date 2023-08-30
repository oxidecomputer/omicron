// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

struct ByteQueue {
    bytes: VecDeque<u8>,

    reader_dropped: bool,
    writer_dropped: bool,
}

impl ByteQueue {
    fn new() -> Self {
        Self {
            bytes: VecDeque::new(),
            reader_dropped: false,
            writer_dropped: false,
        }
    }
}

struct SharedByteQueueInner {
    byte_queue: Mutex<ByteQueue>,

    // Allows callers to block until data can be read.
    cvar: Condvar,
}

impl SharedByteQueueInner {
    fn new() -> Self {
        Self { byte_queue: Mutex::new(ByteQueue::new()), cvar: Condvar::new() }
    }
}

/// A queue of bytes that can selectively act as a reader or writer,
/// which can also be cloned.
///
/// This is primarily used to emulate stdin / stdout / stderr.
#[derive(Clone)]
pub struct SharedByteQueue(Arc<SharedByteQueueInner>);

impl SharedByteQueue {
    pub fn new() -> Self {
        Self(Arc::new(SharedByteQueueInner::new()))
    }

    pub fn take_writer(&self) -> SharedByteQueueWriter {
        SharedByteQueueWriter(self.0.clone())
    }

    pub fn take_reader(&self) -> SharedByteQueueReader {
        SharedByteQueueReader(self.0.clone())
    }
}

pub struct SharedByteQueueWriter(Arc<SharedByteQueueInner>);

impl Drop for SharedByteQueueWriter {
    fn drop(&mut self) {
        let mut bq = self.0.byte_queue.lock().unwrap();
        bq.writer_dropped = true;
        self.0.cvar.notify_all();
    }
}

impl std::io::Write for SharedByteQueueWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut bq = self.0.byte_queue.lock().unwrap();
        if bq.reader_dropped {
            return Ok(0);
        }
        let n = bq.bytes.write(buf)?;
        self.0.cvar.notify_all();
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct SharedByteQueueReader(Arc<SharedByteQueueInner>);

impl std::io::Read for SharedByteQueueReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut bq = self.0.byte_queue.lock().unwrap();

        loop {
            let n = bq.bytes.read(buf)?;
            if n > 0 {
                return Ok(n);
            }
            if bq.writer_dropped {
                return Ok(0);
            }

            bq = self
                .0
                .cvar
                .wait_while(bq, |bq| !bq.writer_dropped && bq.bytes.is_empty())
                .unwrap();
        }
    }
}

impl Drop for SharedByteQueueReader {
    fn drop(&mut self) {
        let mut bq = self.0.byte_queue.lock().unwrap();
        bq.reader_dropped = true;
        self.0.cvar.notify_all();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn blocking_reader() {
        let bq = SharedByteQueue::new();

        let mut reader = bq.take_reader();
        let mut writer = bq.take_writer();

        // This represents our "Process", which could be reading a collection
        // of bytes until stdin completes.
        let handle = std::thread::spawn(move || {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).expect("Failed to read");
            buf
        });

        // This represents someone interacting with the process, dumping to
        // stdin.
        let input1 = b"What you're referring to as bytes,\n";
        let input2 = b"is in fact, bytes/Vec<u8>";
        let input = [input1.as_slice(), input2.as_slice()].concat();

        // Write all the bytes, observe that the reader doesn't exit early.
        writer.write_all(input1).expect("Failed to write");
        std::thread::sleep(std::time::Duration::from_millis(10));
        writer.write_all(input2).expect("Failed to write");
        drop(writer);

        let output = handle.join().unwrap();
        assert_eq!(input, output.as_slice());
    }
}
