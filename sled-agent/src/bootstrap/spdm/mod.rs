//! Instantiate a SPDM requester and responder with particular capabilities,
//! algorithms, and credentials.
//!
//! Sled agents run the SPDM protocol over a tokio TCP stream with a 2 byte size
//! header for framing.

mod error;
mod requester;
mod responder;

use std::io::{Error, ErrorKind};

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use slog::Logger;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// 2^16 - 2 bytes for a header
const MAX_BUF_SIZE: usize = 65534;

pub use error::SpdmError;

pub struct Transport {
    framed: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Transport {
    // We use 2-byte size framed headers.
    #[allow(dead_code)]
    pub const HEADER_LEN: usize = 2;

    #[allow(dead_code)]
    pub fn new(sock: TcpStream) -> Transport {
        Transport {
            framed: LengthDelimitedCodec::builder()
                .length_field_length(Self::HEADER_LEN)
                .new_framed(sock),
        }
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<(), SpdmError> {
        let data = Bytes::copy_from_slice(data);
        self.framed.send(data).await.map_err(|e| e.into())
    }

    pub async fn recv(&mut self, log: &Logger) -> Result<BytesMut, SpdmError> {
        if let Some(rsp) = self.framed.next().await {
            let rsp = rsp?;
            debug!(log, "Received {:x?}", &rsp[..]);
            Ok(rsp)
        } else {
            Err(Error::new(ErrorKind::ConnectionAborted, "SPDM channel closed")
                .into())
        }
    }
}
