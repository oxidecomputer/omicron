// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to help insepct support bundles

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use camino::Utf8Path;
use futures::StreamExt;
use std::pin::Pin;
use tokio::io::AsyncReadExt;

mod bundle_accessor;
mod index;

pub use bundle_accessor::AsyncZipFile;
pub use bundle_accessor::BoxedFileAccessor;
pub use bundle_accessor::FileAccessor;
pub use bundle_accessor::InternalApiAccess;
pub use bundle_accessor::LocalFileAccess;
pub use bundle_accessor::SupportBundleAccessor;
pub use index::SupportBundleIndex;

enum FileState<'a> {
    Open { access: Option<Pin<BoxedFileAccessor<'a>>>, buffered: String },
    Closed,
}

/// A dashboard for inspecting a support bundle contents
pub struct SupportBundleDashboard<'a> {
    access: Box<dyn SupportBundleAccessor + 'a>,
    index: SupportBundleIndex,
    selected: usize,
    file: FileState<'a>,
}

impl<'a> SupportBundleDashboard<'a> {
    pub async fn new(
        access: Box<dyn SupportBundleAccessor + 'a>,
    ) -> Result<Self> {
        let index = access.get_index().await?;
        if index.files().is_empty() {
            bail!("No files found in support bundle");
        }
        Ok(Self { access, index, selected: 0, file: FileState::Closed })
    }

    pub fn index(&self) -> &SupportBundleIndex {
        &self.index
    }

    pub async fn select_up(&mut self, count: usize) -> Result<()> {
        self.selected = self.selected.saturating_sub(count);
        if matches!(self.file, FileState::Open { .. }) {
            self.open_and_buffer().await?;
        }
        Ok(())
    }

    pub async fn select_down(&mut self, count: usize) -> Result<()> {
        self.selected =
            std::cmp::min(self.selected + count, self.index.files().len() - 1);
        if matches!(self.file, FileState::Open { .. }) {
            self.open_and_buffer().await?;
        }
        Ok(())
    }

    pub async fn toggle_file_open(&mut self) -> Result<()> {
        if self.buffered_file_contents().is_none() {
            self.open_and_buffer().await?;
        } else {
            self.close_file();
        }
        Ok(())
    }

    pub async fn open_and_buffer(&mut self) -> Result<()> {
        self.open_file().await?;
        self.read_to_buffer().await?;
        Ok(())
    }

    async fn open_file(&mut self) -> Result<()> {
        let path = &self.index.files()[self.selected];
        if path.as_str().ends_with("/") {
            self.file =
                FileState::Open { access: None, buffered: String::new() };
            return Ok(());
        }

        let file = self
            .access
            .get_file(&path)
            .await
            .with_context(|| format!("Failed to access {path}"))?;
        self.file = FileState::Open {
            access: Some(Box::pin(file)),
            buffered: String::new(),
        };
        Ok(())
    }

    fn close_file(&mut self) {
        self.file = FileState::Closed;
    }

    async fn read_to_buffer(&mut self) -> Result<()> {
        let FileState::Open { access, ref mut buffered } = &mut self.file
        else {
            bail!("File cannot be buffered while closed");
        };
        let Some(file) = access.as_mut() else {
            return Ok(());
        };
        file.read_to_string(buffered).await?;
        Ok(())
    }

    pub fn buffered_file_contents(&self) -> Option<&str> {
        let FileState::Open { ref buffered, .. } = &self.file else {
            return None;
        };
        return Some(buffered);
    }

    pub fn selected_file_index(&self) -> usize {
        self.selected
    }

    pub fn selected_file_name(&self) -> &Utf8Path {
        &self.index.files()[self.selected_file_index()]
    }
}

pub(crate) async fn utf8_stream_to_string(
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
