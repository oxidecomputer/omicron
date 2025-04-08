// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use camino_tempfile::NamedUtf8TempFile;
use camino_tempfile::Utf8PathPersistError;
use camino_tempfile::Utf8TempPath;
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::fs::File;
use tokio::io::AsyncWrite;

pub(crate) struct AsyncNamedTempFile {
    // `temp_path` is _always_ `Some(_)`, except when we `.take()` from it
    // in our `persist()` method below. This allows us to drop the temp path
    // (deleting the temporary file) if we're dropped before `persist()` is
    // called.
    temp_path: Option<Utf8TempPath>,
    destination: Utf8PathBuf,
    inner: File,
}

impl AsyncNamedTempFile {
    pub(crate) async fn with_destination<T: Into<Utf8PathBuf>>(
        destination: T,
    ) -> io::Result<Self> {
        let destination = destination.into();
        let parent = destination
            .parent()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "destination {destination} has no parent directory"
                    ),
                )
            })?
            .to_owned();

        let temp_file =
            tokio::task::spawn_blocking(|| NamedUtf8TempFile::new_in(parent))
                .await
                .unwrap()?;
        let temp_path = temp_file.into_temp_path();

        let inner = File::create(&temp_path).await?;

        Ok(Self { temp_path: Some(temp_path), destination, inner })
    }

    pub(crate) async fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all().await
    }

    pub(crate) async fn persist(mut self) -> io::Result<()> {
        // self.temp_path is always `Some(_)` until we `take()` it here.
        let temp_path = self.temp_path.take().unwrap();
        let destination = self.destination;
        tokio::task::spawn_blocking(move || temp_path.persist(&destination))
            .await
            .unwrap()
            .map_err(|Utf8PathPersistError { error, .. }| error)
    }
}

impl AsyncWrite for AsyncNamedTempFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
