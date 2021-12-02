//! Instantiate a SPDM requester and responder with particular capabilities,
//! algorithms, and credentials.
//!
//! Sled agents run the SPDM protocol over a tokio TCP stream with a 2 byte size
//! header for framing.

mod error;
pub mod requester;
pub mod responder;

use std::io::{Error, ErrorKind};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use slog::Logger;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// 2^16 - 2 bytes for a header
const MAX_BUF_SIZE: usize = 65534;

const TIMEOUT: Duration = Duration::from_secs(5);

pub use error::SpdmError;

pub struct Transport {
    framed: Framed<TcpStream, LengthDelimitedCodec>,
    log: Logger,
}

impl Transport {
    // We use 2-byte size framed headers.
    #[allow(dead_code)]
    pub const HEADER_LEN: usize = 2;

    #[allow(dead_code)]
    pub fn new(sock: TcpStream, log: Logger) -> Transport {
        Transport {
            framed: LengthDelimitedCodec::builder()
                .length_field_length(Self::HEADER_LEN)
                .new_framed(sock),
            log,
        }
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<(), SpdmError> {
        let data = Bytes::copy_from_slice(data);
        timeout(TIMEOUT, self.framed.send(data)).await??;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<BytesMut, SpdmError> {
        if let Some(rsp) = timeout(TIMEOUT, self.framed.next()).await? {
            let rsp = rsp?;
            debug!(self.log, "Received {:x?}", &rsp[..]);
            Ok(rsp)
        } else {
            Err(Error::new(ErrorKind::ConnectionAborted, "SPDM channel closed")
                .into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_recv_timeout() {
        let log =
            omicron_test_utils::dev::test_slog_logger("test_recv_timeout");
        let addr: SocketAddr = "127.0.0.1:9898".parse().unwrap();
        let listener = TcpListener::bind(addr.clone()).await.unwrap();

        let handle = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut transport = Transport::new(sock, log);
            transport.recv().await
        });

        let _ = TcpStream::connect(addr).await.unwrap();

        assert!(handle.await.unwrap().is_err());
    }
}
