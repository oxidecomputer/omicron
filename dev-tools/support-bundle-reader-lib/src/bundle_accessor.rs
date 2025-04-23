// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to help insepct support bundles

use anyhow::Context as _;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Buf;
use bytes::Bytes;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use futures::Future;
use futures::Stream;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;

use crate::SupportBundleIndex;
use crate::utf8_stream_to_string;

/// An I/O source which can read to a buffer
///
/// This describes access to individual files within the bundle.
pub trait FileAccessor: AsyncRead {}
impl<T: AsyncRead + ?Sized> FileAccessor for T {}

/// Describes how the support bundle's data and metadata are accessed.
#[async_trait]
pub trait SupportBundleAccessor {
    type FileAccessor<'a>: FileAccessor where Self: 'a;

    /// Access the index of a support bundle
    async fn get_index(&self) -> Result<SupportBundleIndex>;

    /// Access a file within the support bundle
    async fn get_file(&self, path: &Utf8Path) -> Result<Self::FileAccessor<'_>>;
}

pub struct StreamedFile<'a> {
    client: &'a nexus_client::Client,
    id: SupportBundleUuid,
    path: Utf8PathBuf,
    stream: Option<Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>>,
    buffer: Bytes,
}

impl<'a> StreamedFile<'a> {
    fn new(client: &'a nexus_client::Client, id: SupportBundleUuid, path: Utf8PathBuf) -> Self {
        Self {
            client,
            id,
            path,
            stream: None,
            buffer: Bytes::new(),
        }
    }

    async fn start_stream(&mut self) -> Result<()> {
        // TODO: Add range headers?
        let stream = self.client.support_bundle_download_file(
                self.id.as_untyped_uuid(),
                self.path.as_str()
            )
            .await
            .with_context(|| {
                format!("downloading support bundle file {}: {}", self.id, self.path)
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
                    Err(e) => return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
                }
            }

            match futures::ready!(self.stream.as_mut().unwrap().as_mut().poll_next(cx)) {
                Some(Ok(bytes)) => {
                    self.buffer = bytes;
                }
                Some(Err(e)) => return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
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
    pub fn new(client: &'a nexus_client::Client, id: SupportBundleUuid) -> Self {
        Self {
            client,
            id,
        }
    }
}

// Access for: The nexus internal API
#[async_trait]
impl<'c> SupportBundleAccessor for InternalApiAccess<'c> {
    type FileAccessor<'a> = StreamedFile<'a> where Self: 'a;

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

    async fn get_file(&self, path: &Utf8Path) -> Result<Self::FileAccessor<'_>> {
        let mut file = StreamedFile::new(self.client, self.id, path.to_path_buf());
        file.start_stream().await?;
        Ok(file)
    }
}

// TODO: Probably want to impl this on a new struct that contains a "ZipReader"

// Access for: Local zip files
#[async_trait]
impl SupportBundleAccessor for tokio::fs::File {
    type FileAccessor<'a> = tokio::fs::File;

    async fn get_index(&self) -> Result<SupportBundleIndex> {
        todo!();
    }

    async fn get_file(&self, _path: &Utf8Path) -> Result<Self::FileAccessor<'_>> {
        todo!();
    }
}

