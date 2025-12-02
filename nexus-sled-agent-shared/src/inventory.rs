// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inventory types shared between Nexus and sled-agent.

use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use daft::Diffable;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use indent_write::fmt::IndentWriter;
use omicron_common::disk::{DatasetKind, DatasetName, M2Slot};
use omicron_common::ledger::Ledgerable;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use omicron_common::update::OmicronZoneManifestSource;
use omicron_common::{
    api::{
        external::{ByteCount, Generation},
        internal::shared::{NetworkInterface, SourceNatConfig},
    },
    disk::{DatasetConfig, DiskVariant, OmicronPhysicalDiskConfig},
    update::ArtifactId,
    zpool_name::ZpoolName,
};
use omicron_uuid_kinds::{
    DatasetUuid, InternalZpoolUuid, MupdateUuid, OmicronZoneUuid,
};
use omicron_uuid_kinds::{MupdateOverrideUuid, PhysicalDiskUuid};
use omicron_uuid_kinds::{SledUuid, ZpoolUuid};
use schemars::schema::{Schema, SchemaObject};
use schemars::{JsonSchema, SchemaGenerator};
use serde::{Deserialize, Serialize};
// Export these types for convenience -- this way, dependents don't have to
// depend on sled-hardware-types.
pub use sled_hardware_types::{Baseboard, SledCpuFamily};
use strum::EnumIter;
use tufaceous_artifact::{ArtifactHash, KnownArtifactKind};

/// Re-export The latest versions of each type that has older versions
pub use nexus_sled_agent_shared_migrations::latest::*;
