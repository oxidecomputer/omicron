// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Minimal parsing of Hubris archives.

use anyhow::Context;
use anyhow::Result;
use std::fs;
use std::io::Cursor;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use zip::ZipArchive;

pub struct HubrisArchive {
    archive: ZipArchive<Cursor<Vec<u8>>>,
    path: PathBuf,
}

impl HubrisArchive {
    pub fn open(path: &Path) -> Result<Self> {
        let data = fs::read(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let cursor = Cursor::new(data);
        let archive = ZipArchive::new(cursor).with_context(|| {
            format!(
                "failed to open {} as a zip file - is it a hubris archive?",
                path.display()
            )
        })?;
        Ok(Self { archive, path: path.to_owned() })
    }

    pub fn final_bin(&mut self) -> Result<Vec<u8>> {
        self.extract_by_name("img/final.bin")
    }

    fn extract_by_name(&mut self, name: &str) -> Result<Vec<u8>> {
        let mut f = self.archive.by_name(name).with_context(|| {
            format!(
                "failed to find `{}` within {} - is it a hubris archive?",
                name,
                self.path.display()
            )
        })?;
        let mut data = Vec::new();
        f.read_to_end(&mut data).with_context(|| {
            format!(
                "failed to extract `{}` within {}",
                name,
                self.path.display()
            )
        })?;
        Ok(data)
    }
}
