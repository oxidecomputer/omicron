// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This module contains an old format of "OmicronZoneConfig", where
//! the Sled Agent managed both:
//! - A local generation number for the ledger, and
//! - "Sled-selected" filesystem root paths for zones.
//!
//! These have been replaced by subsequent requests from Nexus, but we
//! still maintain support for the old format so we can parse and convert
//! old systems.

use crate::params::OmicronZoneType;

use anyhow::bail;
use camino::{Utf8Path, Utf8PathBuf};
use illumos_utils::zpool::ZpoolName;
use omicron_common::api::external::Generation;
use omicron_common::ledger::Ledgerable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::Ipv6Addr;
use uuid::Uuid;

/// Combines the Nexus-provided `OmicronZonesConfig` (which describes what Nexus
/// wants for all of its zones) with the locally-determined configuration for
/// these zones.
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema,
)]
pub struct OmicronZonesConfigLocal {
    /// generation of the Omicron-provided part of the configuration
    ///
    /// This generation number is outside of Sled Agent's control.  We store
    /// exactly what we were given and use this number to decide when to
    /// fail requests to establish an outdated configuration.
    ///
    /// You can think of this as a major version number, with
    /// `ledger_generation` being a minor version number.  See
    /// `is_newer_than()`.
    pub omicron_generation: Generation,

    /// ledger-managed generation number
    ///
    /// This generation is managed by the ledger facility itself.  It's bumped
    /// whenever we write a new ledger.  In practice, we don't currently have
    /// any reason to bump this _for a given Omicron generation_ so it's
    /// somewhat redundant.  In principle, if we needed to modify the ledgered
    /// configuration due to some event that doesn't change the Omicron config
    /// (e.g., if we wanted to move the root filesystem to a different path), we
    /// could do that by bumping this generation.
    pub ledger_generation: Generation,
    pub zones: Vec<OmicronZoneConfigLocal>,
}

impl Ledgerable for OmicronZonesConfigLocal {
    fn is_newer_than(&self, other: &OmicronZonesConfigLocal) -> bool {
        self.omicron_generation > other.omicron_generation
            || (self.omicron_generation == other.omicron_generation
                && self.ledger_generation >= other.ledger_generation)
    }

    fn generation_bump(&mut self) {
        self.ledger_generation = self.ledger_generation.next();
    }
}

/// Combines the Nexus-provided `OmicronZoneConfig` (which describes what Nexus
/// wants for this zone) with any locally-determined configuration (like the
/// path to the root filesystem)
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema,
)]
pub struct OmicronZoneConfigLocal {
    pub zone: OmicronZoneConfig,
    #[schemars(with = "String")]
    pub root: Utf8PathBuf,
}

/// Describes one Omicron-managed zone running on a sled
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct OmicronZoneConfig {
    pub id: Uuid,
    pub underlay_address: Ipv6Addr,

    pub zone_type: OmicronZoneType,
}

pub fn root_path_to_pool(root: &Utf8Path) -> Result<ZpoolName, anyhow::Error> {
    use camino::Utf8Component as Component;
    let mut iter = root.components();

    let Some(Component::RootDir) = iter.next() else {
        bail!("Root paths should be absolute, but saw {root}");
    };
    let Some(Component::Normal("pool")) = iter.next() else {
        bail!("Expected '/pool' prefix, saw {root}");
    };
    let Some(Component::Normal("ext")) = iter.next() else {
        bail!("Should only act on external paths, saw {root}");
    };
    let id = if let Some(Component::Normal(id)) = iter.next() {
        id
    } else {
        bail!("Missing pool ID saw {root}");
    };
    let id = id.parse()?;

    // There may be parts of the root path after the pool.
    // We ignore this, though it has historically been ".../crypt/zone".
    //
    // The Sled Agent willstill opt to place these paths onto "crypt/zone".

    Ok(ZpoolName::new_external(id))
}

#[cfg(test)]
mod test {
    use super::*;
    use illumos_utils::zpool::ZpoolKind;
    use uuid::Uuid;

    #[test]
    fn test_root_to_pool() {
        let id = Uuid::new_v4();
        let root = Utf8PathBuf::from(format!("/pool/ext/{id}/crypt/zone"));
        let pool = root_path_to_pool(&root).unwrap();
        assert_eq!(pool.id(), id);
        assert!(matches!(pool.kind(), ZpoolKind::External));
    }
}
