// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Minimal parsing of Hubris archives.

use std::io::Cursor;
use std::io::Read;
use zip::ZipArchive;

use crate::error::UpdateError;

pub(crate) struct HubrisArchive {
    archive: ZipArchive<Cursor<Vec<u8>>>,
}

impl HubrisArchive {
    pub(crate) fn new(data: Vec<u8>) -> Result<Self, UpdateError> {
        let cursor = Cursor::new(data);
        let archive =
            ZipArchive::new(cursor).map_err(UpdateError::SpUpdateNotZip)?;
        Ok(Self { archive })
    }

    pub(crate) fn final_bin(&mut self) -> Result<Vec<u8>, UpdateError> {
        self.extract_by_name("img/final.bin")
    }

    pub(crate) fn aux_image(&mut self) -> Result<Vec<u8>, UpdateError> {
        self.extract_by_name("img/auxi.tlvc")
    }

    fn extract_by_name(&mut self, name: &str) -> Result<Vec<u8>, UpdateError> {
        let mut f = self.archive.by_name(name).map_err(|err| {
            UpdateError::SpUpdateFileNotFound { path: name.to_string(), err }
        })?;
        let mut data = Vec::new();
        f.read_to_end(&mut data).map_err(|err| {
            UpdateError::SpUpdateDecompressionFailed {
                path: name.to_string(),
                err,
            }
        })?;
        Ok(data)
    }
}
