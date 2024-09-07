// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::io;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;
use tokio::io::AsyncWrite;

/// `BlockSizeBufWriter` is analogous to a tokio's `BufWriter`, except it
/// guarantees that writes made to the underlying writer are always
/// _exactly_ the requested block size, with three exceptions:
///
/// 1. Calling `flush()` will write any currently-buffered data to the
///    underlying writer, regardless of its length.
/// 2. Similarily, calling `shutdown()` will flush any currently-buffered data
///    to the underlying writer.
/// 3. When `BlockSizeBufWriter` attempts to write a block-length amount of data
///    to the underlying writer, if that writer only accepts a portion of that
///    data, `BlockSizeBufWriter` will continue attempting to write the
///    remainder of the block.
///
/// When `BlockSizeBufWriter` is dropped, any buffered data it's holding
/// will be discarded. It is critical to manually call
/// `BlockSizeBufWriter:flush()` or `BlockSizeBufWriter::shutdown()` prior
/// to dropping to avoid data loss.
pub struct BlockSizeBufWriter<W> {
    inner: W,
    buf: Vec<u8>,
    block_size: usize,
}

impl<W: AsyncWrite + Unpin> BlockSizeBufWriter<W> {
    pub fn with_block_size(block_size: usize, inner: W) -> Self {
        Self { inner, buf: Vec::with_capacity(block_size), block_size }
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    fn get_pin_mut(&mut self) -> Pin<&mut W> {
        Pin::new(&mut self.inner)
    }

    fn flush_buf(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut written = 0;
        let mut ret = Ok(());

        // We expect this loop to execute exactly one time: we try to write the
        // entirety of `self.buf` to `self.inner`, and presumably it is a type
        // that expects to receive a block of data at once, so we'll immediately
        // jump to `written == self.buf.len()`. If it returns `Ok(n)` for some
        // `n < self.buf.len()`, we'll loop and try to write the rest of the
        // data in less-than-block-sized chunks.
        while written < self.buf.len() {
            match ready!(
                Pin::new(&mut self.inner).poll_write(cx, &self.buf[written..])
            ) {
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
            self.buf.drain(..written);
        }
        Poll::Ready(ret)
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for BlockSizeBufWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let block_size = self.block_size;

        // We should never buffer up more than `block_size` data.
        assert!(self.buf.len() <= block_size);

        // If we already have exactly `block_size` bytes, begin by flushing.
        if self.buf.len() == block_size {
            ready!(self.as_mut().flush_buf(cx))?;
        }

        if self.buf.is_empty() {
            // If our buffer is empty, either directly write `block_size` bytes
            // from `buf` to `inner` (if there's enough data in `buf`) or just
            // copy it into our buffer.
            if buf.len() >= block_size {
                Pin::new(&mut self.inner).poll_write(cx, &buf[..block_size])
            } else {
                // `me.buf` is empty and `buf` is strictly less than
                // `block_size`, so just copy it.
                self.buf.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
        } else {
            // Our buffer already has data - just copy as much of `buf` as we
            // can onto the end of it.
            let n = usize::min(block_size - self.buf.len(), buf.len());
            self.buf.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        ready!(self.flush_buf(cx))?;
        self.get_pin_mut().poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        ready!(self.flush_buf(cx))?;
        self.get_pin_mut().poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::future::Future;
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

    fn with_test_runtime<F, Fut, T>(f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("tokio Runtime built successfully");
        runtime.block_on(f())
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
