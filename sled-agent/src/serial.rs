//! Connects to propolis-server's websocket endpoint for an instance's serial console, and
//! maintains a buffer of an instance's serial console data, holding both the first mebibyte and the
//! most recent mebibyte of console output.

use futures::StreamExt;
use omicron_common::backoff::{retry, BackoffError, ExponentialBackoff};
use slog::Logger;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O failure in serial console socket: {0}")]
    SocketIo(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Requested byte offset is no longer cached: {0}")]
    ExpiredRange(usize),

    #[error("No rolling buffer was allocated for this instance.")]
    Existential,
}

const TTY_BUFFER_SIZE: usize = 1024 * 1024;
const DEFAULT_MAX_LENGTH: isize = 16 * 1024;

struct BufferData {
    beginning: Vec<u8>,
    rolling: VecDeque<u8>,
    total_bytes: isize,
}

impl BufferData {
    fn new(buffer_size: usize) -> Self {
        BufferData {
            beginning: Vec::with_capacity(buffer_size),
            rolling: VecDeque::with_capacity(buffer_size),
            total_bytes: 0,
        }
    }

    fn consume(&mut self, msg: Vec<u8>) {
        let headroom = self.rolling.capacity() - self.rolling.len();
        let read_size = msg.len();
        if read_size > headroom {
            let to_capture = self.beginning.capacity() - self.beginning.len();
            let drain =
                self.rolling.drain(0..(read_size - headroom)).take(to_capture);
            self.beginning.extend(drain);
        }
        self.rolling.extend(msg);
        self.total_bytes += read_size as isize;
    }

    /// Get an iterator of serial console bytes from the live buffer.
    /// `since`:
    /// - if positive, is the byte index since instance start.
    /// - if negative, is the byte index backwards from the most recently buffered data.
    /// - if none, yields the most recent `DEFAULT_MAX_LENGTH` of data.
    fn contents_iter(
        &self,
        byte_offset: Option<isize>,
    ) -> Result<(Box<dyn Iterator<Item = u8> + '_>, usize), Error> {
        let start = byte_offset.unwrap_or(-DEFAULT_MAX_LENGTH);
        let (from_start, from_end) = self.offsets_from_start_and_end(start);
        let iter: Box<dyn Iterator<Item = u8> + '_> = if from_end
            > self.rolling.len()
        {
            if from_start < self.beginning.len() {
                // is it still possible to stitch together `beginning` and `rolling`?
                if self.total_bytes as usize
                    == self.rolling.len() + self.beginning.len()
                {
                    Box::new(
                        self.beginning
                            .iter()
                            .skip(from_start)
                            .chain(self.rolling.iter())
                            .copied(),
                    )
                } else {
                    Box::new(self.beginning.iter().copied().skip(from_start))
                }
            } else {
                return Err(Error::ExpiredRange(from_start));
            }
        } else {
            // apologies to Takenobu Mitsuyoshi
            let rolling_start = self.rolling.len() - from_end as usize;
            Box::new(self.rolling.iter().copied().skip(rolling_start))
        };
        Ok((iter, from_start))
    }

    fn contents_vec(
        &self,
        byte_offset: Option<isize>,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<u8>, usize), Error> {
        let (iter, from_start) = self.contents_iter(byte_offset)?;
        let data: Vec<u8> = iter
            .take(max_bytes.unwrap_or(DEFAULT_MAX_LENGTH as usize))
            .collect();
        let end_offset = from_start + data.len();
        Ok((data, end_offset))
    }

    fn offsets_from_start_and_end(&self, start: isize) -> (usize, usize) {
        if start >= 0 {
            if self.total_bytes > start {
                (start as usize, (self.total_bytes - start) as usize)
            } else {
                (self.total_bytes as usize, 0)
            }
        } else {
            if self.total_bytes > -start {
                ((self.total_bytes + start) as usize, -start as usize)
            } else {
                (0, self.total_bytes as usize)
            }
        }
    }
}

pub(crate) struct SerialConsoleBuffer {
    task: JoinHandle<()>,
    data: Arc<RwLock<BufferData>>,
}

impl SerialConsoleBuffer {
    pub(crate) fn new(ws_uri: String, log: Logger) -> Self {
        let data = Arc::new(RwLock::new(BufferData::new(TTY_BUFFER_SIZE)));
        let data_inner = data.clone();
        let task = tokio::task::spawn(async move {
            let connect_future =
                retry(ExponentialBackoff::default(), || async {
                    match tokio_tungstenite::connect_async(&ws_uri).await {
                        Ok(x) => Ok(x),
                        Err(err) => {
                            warn!(
                                log,
                                "TTY connection to {}: {:?}", &ws_uri, err
                            );
                            Err(BackoffError::Transient {
                                err,
                                retry_after: None,
                            })
                        }
                    }
                });
            match connect_future.await {
                Ok((mut websocket, _)) => loop {
                    match websocket.next().await {
                        None => {
                            warn!(log, "Nothing read from {}", &ws_uri);
                        }
                        Some(Err(e)) => {
                            error!(
                                log,
                                "Reading TTY from {}: {:?}", &ws_uri, e
                            );
                        }
                        Some(Ok(Message::Close(details))) => {
                            info!(
                                log,
                                "Closing TTY connection to {}{}",
                                ws_uri,
                                if let Some(cf) = details {
                                    format!(": {}", cf)
                                } else {
                                    String::new()
                                }
                            );
                        }
                        Some(Ok(msg)) => {
                            data_inner.write().await.consume(msg.into_data());
                        }
                    }
                },
                Err(e) => {
                    error!(log, "Failed to open propolis serial console websocket: {:?}", e);
                }
            }
        });

        SerialConsoleBuffer { task, data }
    }

    pub(crate) async fn contents(
        &self,
        byte_offset: Option<isize>,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<u8>, usize), Error> {
        self.data.read().await.contents_vec(byte_offset, max_bytes)
    }
}

impl Drop for SerialConsoleBuffer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // for more legible assertions
    fn sugar(
        buf: &BufferData,
        byte_offset: isize,
        max_bytes: usize,
    ) -> (String, usize) {
        buf.contents_vec(Some(byte_offset), Some(max_bytes))
            .map(|x| (String::from_utf8(x.0).expect("invalid utf-8"), x.1))
            .expect("serial range query failed")
    }

    #[test]
    fn test_continuous_buffer_range_abstraction() {
        let mut buf = BufferData::new(16);

        assert_eq!(buf.contents_vec(None, None).unwrap(), (vec![], 0));
        assert_eq!(sugar(&buf, 0, 0), (String::new(), 0));
        assert_eq!(sugar(&buf, 0, 11), (String::new(), 0));
        assert_eq!(sugar(&buf, 11, 0), (String::new(), 0));
        assert_eq!(sugar(&buf, 11, 11), (String::new(), 0));

        let line = "This is an example of text.";
        let line_bytes = line.as_bytes().to_vec();

        buf.consume(Vec::from(&line_bytes[..9]));
        assert_eq!(sugar(&buf, 8, 5), ("a".to_string(), 9));
        buf.consume(Vec::from(&line_bytes[9..]));

        assert_eq!(
            buf.contents_vec(None, None).unwrap(),
            (line_bytes, line.len())
        );
        assert_eq!(
            sugar(&buf, 0, line.len() + 10),
            (line.to_string(), line.len())
        );
        assert_eq!(sugar(&buf, 8, 5), ("an ex".to_string(), 13));
        assert_eq!(sugar(&buf, 100, 10), (String::new(), line.len()));
        assert_eq!(sugar(&buf, -10, 4), ("e of".to_string(), 21));
        assert_eq!(
            sugar(&buf, -10, 400),
            ("e of text.".to_string(), line.len())
        );
        assert_eq!(sugar(&buf, -100, 4), ("This".to_string(), 4));

        buf.consume("\nNo thing beside remains.".as_bytes().to_vec());
        assert_eq!(sugar(&buf, -10, 4), ("e re".to_string(), 46));
        assert_eq!(sugar(&buf, 8, 8), ("an examp".to_string(), 16));
        assert_eq!(sugar(&buf, 8, 12), ("an examp".to_string(), 16));

        assert!(buf.contents_vec(Some(16), None).is_err());
    }
}
