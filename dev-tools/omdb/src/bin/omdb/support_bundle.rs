// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to access support bundles via the internal API

use anyhow::Context as _;
use anyhow::bail;
use async_trait::async_trait;
use bytes::Buf;
use bytes::Bytes;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use nexus_client::types::SupportBundleInfo;
use nexus_client::types::SupportBundleState;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use std::io;
use std::io::Write;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use support_bundle_viewer::BoxedFileAccessor;
use support_bundle_viewer::SupportBundleAccessor;
use support_bundle_viewer::SupportBundleIndex;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;

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

    // NOTE: This is a distinct method from "new", because ideally some day we could
    // use range requests to stream out portions of the file.
    //
    // This means that we would potentially want to restart the stream with a different position.
    async fn start_stream(&mut self) -> anyhow::Result<()> {
        // TODO: Add range headers, for range requests? Though this
        // will require adding support to Progenitor + Nexus too.
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
            match futures::ready!(
                self.stream
                    .as_mut()
                    .expect("Stream must be initialized before polling")
                    .as_mut()
                    .poll_next(cx)
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

async fn utf8_stream_to_string(
    mut stream: impl futures::Stream<Item = reqwest::Result<bytes::Bytes>>
    + std::marker::Unpin,
) -> anyhow::Result<String> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8(bytes)?)
}

// Access for: The nexus internal API
#[async_trait]
impl<'c> SupportBundleAccessor for InternalApiAccess<'c> {
    async fn get_index(&self) -> anyhow::Result<SupportBundleIndex> {
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
    ) -> anyhow::Result<BoxedFileAccessor<'a>>
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

async fn wait_for_bundle_to_be_collected(
    client: &nexus_client::Client,
    id: SupportBundleUuid,
) -> Result<SupportBundleInfo, anyhow::Error> {
    let mut printed_wait_msg = false;
    loop {
        let sb = client
            .support_bundle_view(id.as_untyped_uuid())
            .await
            .with_context(|| {
                format!("failed to query for support bundle {}", id)
            })?;

        match sb.state {
            SupportBundleState::Active => {
                if printed_wait_msg {
                    eprintln!("");
                }
                return Ok(sb.into_inner());
            }
            SupportBundleState::Collecting => {
                if !printed_wait_msg {
                    eprint!("Waiting for {} to finish collection...", id);
                    printed_wait_msg = true;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                eprint!(".");
                std::io::stderr().flush().context("cannot flush stderr")?;
            }
            other => bail!("Unexepcted state: {other}"),
        }
    }
}

/// Returns either a specific bundle or the latest active bundle.
///
/// If a bundle is being collected, waits for it.
pub async fn access_bundle_from_id(
    client: &nexus_client::Client,
    id: Option<SupportBundleUuid>,
) -> Result<InternalApiAccess<'_>, anyhow::Error> {
    let id = match id {
        Some(id) => {
            // Ensure the bundle has been collected
            let sb = wait_for_bundle_to_be_collected(
                client,
                SupportBundleUuid::from_untyped_uuid(*id.as_untyped_uuid()),
            )
            .await?;
            SupportBundleUuid::from_untyped_uuid(sb.id.into_untyped_uuid())
        }
        None => {
            // Grab the latest if one isn't supplied
            let support_bundle_stream =
                client.support_bundle_list_stream(None, None);
            let mut support_bundles = support_bundle_stream
                .try_collect::<Vec<_>>()
                .await
                .context("listing support bundles")?;
            support_bundles.sort_by_key(|k| k.time_created);

            let active_sb = support_bundles
                .iter()
                .find(|sb| matches!(sb.state, SupportBundleState::Active));

            let sb = match active_sb {
                Some(sb) => sb.clone(),
                None => {
                    // This is a special case, but not an uncommon one:
                    //
                    // - Someone just created a bundle...
                    // - ... but collection is still happening.
                    //
                    // To smooth out this experience for users, we wait for the
                    // collection to complete.
                    let collecting_sb = support_bundles.iter().find(|sb| {
                        matches!(sb.state, SupportBundleState::Collecting)
                    });
                    if let Some(collecting_sb) = collecting_sb {
                        let id = &collecting_sb.id;
                        wait_for_bundle_to_be_collected(
                            client,
                            SupportBundleUuid::from_untyped_uuid(
                                *id.as_untyped_uuid(),
                            ),
                        )
                        .await?
                    } else {
                        bail!(
                            "Cannot find active support bundle. Try creating one"
                        )
                    }
                }
            };

            eprintln!("Inspecting bundle {} from {}", sb.id, sb.time_created);

            SupportBundleUuid::from_untyped_uuid(sb.id.into_untyped_uuid())
        }
    };
    Ok(InternalApiAccess::new(client, id))
}
