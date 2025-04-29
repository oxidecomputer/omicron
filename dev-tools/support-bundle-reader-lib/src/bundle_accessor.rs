// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! APIs to help access bundles

use anyhow::Context as _;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Buf;
use bytes::Bytes;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use futures::Future;
use futures::Stream;
use futures::StreamExt;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;

use crate::SupportBundleIndex;

/// An I/O source which can read to a buffer
///
/// This describes access to individual files within the bundle.
pub trait FileAccessor: AsyncRead + Unpin {}
impl<T: AsyncRead + Unpin + ?Sized> FileAccessor for T {}

pub type BoxedFileAccessor<'a> = Box<dyn FileAccessor + 'a>;

/// Describes how the support bundle's data and metadata are accessed.
#[async_trait]
pub trait SupportBundleAccessor {
    /// Access the index of a support bundle
    async fn get_index(&self) -> Result<SupportBundleIndex>;

    /// Access a file within the support bundle
    async fn get_file<'a>(
        &mut self,
        path: &Utf8Path,
    ) -> Result<BoxedFileAccessor<'a>>
    where
        Self: 'a;
}

pub struct StreamedFile<'a> {
    client: &'a nexus_client::Client,
    id: SupportBundleUuid,
    path: Utf8PathBuf,
    stream: Option<Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>>,
    buffer: Bytes,
}

impl<'a> StreamedFile<'a> {
    fn new(
        client: &'a nexus_client::Client,
        id: SupportBundleUuid,
        path: Utf8PathBuf,
    ) -> Self {
        Self { client, id, path, stream: None, buffer: Bytes::new() }
    }

    async fn start_stream(&mut self) -> Result<()> {
        // TODO: Add range headers?
        let stream = self
            .client
            .support_bundle_download_file(
                self.id.as_untyped_uuid(),
                self.path.as_str(),
            )
            .await
            .with_context(|| {
                format!(
                    "downloading support bundle file {}: {}",
                    self.id, self.path
                )
            })?
            .into_inner_stream();

        self.stream = Some(Box::pin(stream));
        Ok(())
    }
}

impl AsyncRead for StreamedFile<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        while self.buffer.is_empty() {
            if self.stream.is_none() {
                // NOTE: this is broken?
                let fut = self.start_stream();
                let mut fut = Box::pin(fut);

                match futures::ready!(fut.as_mut().poll(cx)) {
                    Ok(()) => {}
                    Err(e) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            e,
                        )));
                    }
                }
            }

            match futures::ready!(
                self.stream.as_mut().unwrap().as_mut().poll_next(cx)
            ) {
                Some(Ok(bytes)) => {
                    self.buffer = bytes;
                }
                Some(Err(e)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        e,
                    )));
                }
                None => return Poll::Ready(Ok(())), // EOF
            }
        }

        let to_copy = std::cmp::min(self.buffer.len(), buf.remaining());
        buf.put_slice(&self.buffer[..to_copy]);
        self.buffer.advance(to_copy);

        Poll::Ready(Ok(()))
    }
}

/// Access to a support bundle from the internal API
pub struct InternalApiAccess<'a> {
    client: &'a nexus_client::Client,
    id: SupportBundleUuid,
}

impl<'a> InternalApiAccess<'a> {
    pub fn new(
        client: &'a nexus_client::Client,
        id: SupportBundleUuid,
    ) -> Self {
        Self { client, id }
    }
}

// Access for: The nexus internal API
#[async_trait]
impl<'c> SupportBundleAccessor for InternalApiAccess<'c> {
    async fn get_index(&self) -> Result<SupportBundleIndex> {
        let stream = self
            .client
            .support_bundle_index(self.id.as_untyped_uuid())
            .await
            .with_context(|| {
                format!("downloading support bundle index {}", self.id)
            })?
            .into_inner_stream();
        let s = utf8_stream_to_string(stream).await?;

        Ok(SupportBundleIndex::new(&s))
    }

    async fn get_file<'a>(
        &mut self,
        path: &Utf8Path,
    ) -> Result<BoxedFileAccessor<'a>>
    where
        'c: 'a,
    {
        let mut file =
            StreamedFile::new(self.client, self.id, path.to_path_buf());
        file.start_stream()
            .await
            .with_context(|| "failed to start stream in get_file")?;
        Ok(Box::new(file))
    }
}

pub struct LocalFileAccess {
    archive: zip::read::ZipArchive<std::fs::File>,
}

impl LocalFileAccess {
    pub fn new(path: &Utf8Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        Ok(Self { archive: zip::read::ZipArchive::new(file)? })
    }
}

// Access for: Local zip files
#[async_trait]
impl SupportBundleAccessor for LocalFileAccess {
    async fn get_index(&self) -> Result<SupportBundleIndex> {
        let names: Vec<&str> = self.archive.file_names().collect();
        let all_names = names.join("\n");
        Ok(SupportBundleIndex::new(&all_names))
    }

    async fn get_file<'a>(
        &mut self,
        path: &Utf8Path,
    ) -> Result<BoxedFileAccessor<'a>> {
        let mut file = self.archive.by_name(path.as_str())?;
        let mut buf = Vec::new();
        std::io::copy(&mut file, &mut buf)?;

        Ok(Box::new(AsyncZipFile { buf, copied: 0 }))
    }
}

// We're currently buffering the entire file into memory, mostly because dealing with the lifetime
// of ZipArchive and ZipFile objects is so difficult.
pub struct AsyncZipFile {
    buf: Vec<u8>,
    copied: usize,
}

impl AsyncRead for AsyncZipFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let to_copy =
            std::cmp::min(self.buf.len() - self.copied, buf.remaining());
        if to_copy == 0 {
            return Poll::Ready(Ok(()));
        }
        let src = &self.buf[self.copied..];
        buf.put_slice(&src[..to_copy]);
        self.copied += to_copy;
        Poll::Ready(Ok(()))
    }
}

async fn utf8_stream_to_string(
    mut stream: impl futures::Stream<Item = reqwest::Result<bytes::Bytes>>
    + std::marker::Unpin,
) -> Result<String> {
    let mut result = String::new();

    // When we read from the string, we might not read a whole UTF-8 sequence.
    // Keep this "leftover" type here to concatenate this partially-read data
    // when we read the next sequence.
    let mut leftover: Option<bytes::Bytes> = None;
    while let Some(data) = stream.next().await {
        match data {
            Err(err) => return Err(anyhow::anyhow!(err)),
            Ok(data) => {
                let combined = match leftover.take() {
                    Some(old) => [old, data].concat(),
                    None => data.to_vec(),
                };

                match std::str::from_utf8(&combined) {
                    Ok(data) => result += data,
                    Err(_) => leftover = Some(combined.into()),
                }
            }
        }
    }
    Ok(result)
}
