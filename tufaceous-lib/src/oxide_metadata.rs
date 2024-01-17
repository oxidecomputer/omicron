// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This file is a copy of
//! <https://github.com/oxidecomputer/helios-omicron-brand/blob/d810b76a93c08e36b8e0ff2ce79904232c8ae773/utils/src/metadata.rs>.
//! Once that is open sourced, we should switch to using that.

/*
 * Copyright 2023 Oxide Computer Company
 */

use std::collections::HashMap;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::MtimeSource;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveType {
    Baseline,
    Layer,
    Os,
    Rot,
    ControlPlane,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Metadata {
    v: String,
    t: ArchiveType,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    i: HashMap<String, String>,
}

pub fn parse(s: &str) -> Result<Metadata> {
    let m: Metadata = serde_json::from_str(s)?;
    if m.v != "1" {
        bail!("unexpected metadata version {}", m.v);
    }
    Ok(m)
}

impl Metadata {
    pub fn append_to_tar<T: std::io::Write>(
        &self,
        a: &mut tar::Builder<T>,
        mtime_source: MtimeSource,
    ) -> Result<()> {
        let mut b = serde_json::to_vec(self)?;
        b.push(b'\n');

        // XXX This was changed from upstream to add oxide.json with optionally
        // a zero timestamp, to ensure stability of fake manifests.
        let mtime = mtime_source.into_mtime();

        let mut h = tar::Header::new_ustar();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_username("root")?;
        h.set_uid(0);
        h.set_groupname("root")?;
        h.set_gid(0);
        h.set_path("oxide.json")?;
        h.set_mode(0o444);
        h.set_size(b.len().try_into().unwrap());
        h.set_mtime(mtime);
        h.set_cksum();

        a.append(&h, b.as_slice())?;
        Ok(())
    }

    pub fn is_layer(&self) -> bool {
        matches!(&self.t, ArchiveType::Layer)
    }

    pub fn is_baseline(&self) -> bool {
        matches!(&self.t, ArchiveType::Baseline)
    }

    pub fn is_os(&self) -> bool {
        matches!(&self.t, ArchiveType::Os)
    }

    pub fn is_rot(&self) -> bool {
        matches!(&self.t, ArchiveType::Rot)
    }

    pub fn is_control_plane(&self) -> bool {
        matches!(&self.t, ArchiveType::ControlPlane)
    }

    pub fn archive_type(&self) -> ArchiveType {
        self.t
    }

    pub fn info(&self) -> &HashMap<String, String> {
        &self.i
    }
}

pub struct MetadataBuilder {
    archive_type: ArchiveType,
    info: HashMap<String, String>,
}

impl MetadataBuilder {
    pub fn new(archive_type: ArchiveType) -> MetadataBuilder {
        MetadataBuilder { archive_type, info: Default::default() }
    }

    pub fn info(
        &mut self,
        name: &str,
        value: &str,
    ) -> Result<&mut MetadataBuilder> {
        if name.len() < 3 {
            bail!("info property names must be at least three characters");
        }
        self.info.insert(name.to_string(), value.to_string());
        Ok(self)
    }

    pub fn build(&mut self) -> Result<Metadata> {
        Ok(Metadata {
            v: "1".into(),
            t: self.archive_type,
            i: self.info.clone(),
        })
    }
}
