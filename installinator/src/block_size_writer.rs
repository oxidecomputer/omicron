// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use pin_project_lite::pin_project;
use std::io;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;
use tokio::io::AsyncWrite;

pin_project! {
    /// `BlockSizeBufWriter` is analogous to a tokio's `BufWriter`, except it
    /// guarantees that writes made to the underlying writer are always
    /// _exactly_ the requested block size, with two exceptions: explicitly
    /// calling (1) `flush()` or (2) `shutdown()` will write any
    /// buffered-but-not-yet-written data to the underlying buffer regardless of
    /// its length.
    ///
    /// When `BlockSizeBufWriter` is dropped, any buffered data it's holding
    /// will be discarded. It is critical to manually call
    /// `BlockSizeBufWriter:flush()` or `BlockSizeBufWriter::shutdown()` prior
    /// to dropping to avoid data loss.
    pub(crate) struct BlockSizeBufWriter<W> {
        #[pin]
        inner: W,
        buf: Vec<u8>,
        block_size: usize,
    }
}

impl<W: AsyncWrite> BlockSizeBufWriter<W> {
    pub(crate) fn with_block_size(block_size: usize, inner: W) -> Self {
        Self { inner, buf: Vec::with_capacity(block_size), block_size }
    }

    #[cfg(test)]
    fn into_inner(self) -> W {
        self.inner
    }

    fn get_pin_mut(self: Pin<&mut Self>) -> Pin<&mut W> {
        self.project().inner
    }

    fn flush_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let mut me = self.project();
        let mut written = 0;
        let mut ret = Ok(());
        while written < me.buf.len() {
            match ready!(me.inner.as_mut().poll_write(cx, &me.buf[written..])) {
                Ok(0) => {
                    ret = Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write the buffered data",
                    ));
                    break;
                }
                Ok(n) => written += n,
                Err(e) => {
                    ret = Err(e);
                    break;
                }
            }
        }
        if written > 0 {
            me.buf.drain(..written);
        }
        Poll::Ready(ret)
    }
}

impl<W: AsyncWrite> AsyncWrite for BlockSizeBufWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        // We should never buffer up more than `block_size` data.
        assert!(self.buf.len() <= self.block_size);

        // If we already have exactly `block_size` bytes, begin by flushing.
        if self.buf.len() == self.block_size {
            ready!(self.as_mut().flush_buf(cx))?;
        }

        let me = self.project();
        if me.buf.is_empty() {
            // If our buffer is empty, either directly write `block_size` bytes
            // from `buf` to `inner` (if there's enough data in `buf`) or just
            // copy it into our buffer.
            if buf.len() >= *me.block_size {
                me.inner.poll_write(cx, &buf[..*me.block_size])
            } else {
                // `me.buf` is empty and `buf` is strictly less than
                // `self.block_size`, so just copy it.
                me.buf.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
        } else {
            // Our buffer already has data - just copy as much of `buf` as we
            // can onto the end of it.
            let n = usize::min(*me.block_size - me.buf.len(), buf.len());
            me.buf.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        ready!(self.as_mut().flush_buf(cx))?;
        self.get_pin_mut().poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        ready!(self.as_mut().flush_buf(cx))?;
        self.get_pin_mut().poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::with_test_runtime;
    use anyhow::Result;
    use test_strategy::proptest;
    use tokio::io::AsyncWriteExt;

    // Dummy `AsyncWrite` that always accepts all data and keeps track of the
    // sizes of the buffers it was given to write.
    #[derive(Debug, Default)]
    struct InnerWriter {
        data: Vec<u8>,
        write_requests: Vec<usize>,
    }

    impl AsyncWrite for InnerWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            self.data.extend_from_slice(buf);
            self.write_requests.push(buf.len());
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[proptest]
    fn proptest_block_writer(
        chunks: Vec<Vec<u8>>,
        #[strategy((16_usize..4096))] block_size: usize,
    ) {
        with_test_runtime(move || async move {
            proptest_block_writer_impl(chunks, block_size)
                .await
                .expect("test failed");
        })
    }

    async fn proptest_block_writer_impl(
        chunks: Vec<Vec<u8>>,
        block_size: usize,
    ) -> Result<()> {
        // Construct our block writer.
        let inner = InnerWriter::default();
        let mut block_writer =
            BlockSizeBufWriter::with_block_size(block_size, inner);
        let mut expected_data = Vec::new();

        // Feed all chunks into it.
        for chunk in chunks {
            expected_data.extend_from_slice(&chunk);
            block_writer.write_all(&chunk).await.unwrap();
        }
        block_writer.flush().await.unwrap();

        let inner = block_writer.into_inner();

        // Check properties: the underlying writer should have received all the
        // data of `chunks` in order...
        assert_eq!(inner.data, expected_data);

        // ...and all writes to it (except the last) should be exactly the block
        // size; the last should be at most the block size.
        if !inner.write_requests.is_empty() {
            let last = inner.write_requests.len() - 1;
            assert!(
                inner.write_requests[last]
                    <= block_size,
                "last block size too large (expected at most {block_size}, got {})",
                inner.write_requests[last],
            );
            for (i, &wr) in inner.write_requests.iter().take(last).enumerate() {
                assert_eq!(
                    wr,
                    block_size,
                    "write request {i} had size {wr} (expected block size {block_size})",
                );
            }
        }

        Ok(())
    }
}
