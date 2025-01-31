// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handling of `oxide.json` metadata files in tarballs.
//!
//! `oxide.json` is originally defined by the omicron1(7) zone brand, which
//! lives at <https://github.com/oxidecomputer/helios-omicron-brand>. tufaceous
//! extended this format with additional archive types for identifying other
//! types of tarballs; this crate covers those extensions so they can be used
//! across the Omicron codebase.

use std::io::{Error, ErrorKind, Read, Result, Write};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Metadata {
    v: String,

    // helios-build-utils defines a top-level `i` field for extra information,
    // but omicron-package doesn't use this for the package name and version.
    // We can also benefit from having rich types for these extra fields, so
    // any additional top-level fields (including `i`) that exist for a given
    // archive type should be deserialized as part of `ArchiveType`.
    #[serde(flatten)]
    t: ArchiveType,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "t")]
pub enum ArchiveType {
    // Originally defined in helios-build-utils (part of helios-omicron-brand):
    Baseline,
    Layer(LayerInfo),
    Os,

    // tufaceous extensions:
    Rot,
    ControlPlane,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LayerInfo {
    pub pkg: String,
    pub version: semver::Version,
}

impl Metadata {
    pub fn new(archive_type: ArchiveType) -> Metadata {
        Metadata { v: "1".into(), t: archive_type }
    }

    pub fn append_to_tar<T: Write>(
        &self,
        a: &mut tar::Builder<T>,
        mtime: u64,
    ) -> Result<()> {
        let mut b = serde_json::to_vec(self)?;
        b.push(b'\n');

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

    /// Read `Metadata` from a tar archive.
    ///
    /// `oxide.json` is generally the first file in the archive, so this should
    /// be a just-opened archive with no entries already read.
    pub fn read_from_tar<T: Read>(a: &mut tar::Archive<T>) -> Result<Metadata> {
        for entry in a.entries()? {
            let mut entry = entry?;
            if entry.path()? == std::path::Path::new("oxide.json") {
                return Ok(serde_json::from_reader(&mut entry)?);
            }
        }
        Err(Error::new(ErrorKind::InvalidData, "oxide.json is not present"))
    }

    pub fn archive_type(&self) -> &ArchiveType {
        &self.t
    }

    pub fn is_layer(&self) -> bool {
        matches!(&self.t, ArchiveType::Layer(_))
    }

    pub fn layer_info(&self) -> Result<&LayerInfo> {
        match &self.t {
            ArchiveType::Layer(info) => Ok(info),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "archive is not the \"layer\" type",
            )),
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() {
        let metadata: Metadata = serde_json::from_str(
            r#"{"v":"1","t":"layer","pkg":"nexus","version":"12.0.0-0.ci+git3a2ed5e97b3"}"#,
        )
        .unwrap();
        assert!(metadata.is_layer());
        let info = metadata.layer_info().unwrap();
        assert_eq!(info.pkg, "nexus");
        assert_eq!(info.version, "12.0.0-0.ci+git3a2ed5e97b3".parse().unwrap());

        let metadata: Metadata = serde_json::from_str(
            r#"{"v":"1","t":"os","i":{"checksum":"42eda100ee0e3bf44b9d0bb6a836046fa3133c378cd9d3a4ba338c3ba9e56eb7","name":"ci 3a2ed5e/9d37813 2024-12-20 08:54"}}"#,
        ).unwrap();
        assert!(metadata.is_os());

        let metadata: Metadata =
            serde_json::from_str(r#"{"v":"1","t":"control_plane"}"#).unwrap();
        assert!(metadata.is_control_plane());
    }
}
