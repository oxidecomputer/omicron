// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use iddqd::id_upcast;
use iddqd::{IdOrdItem, IdOrdMap};
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::{DatasetUuid, OmicronZoneUuid};
use schemars::{
    JsonSchema, SchemaGenerator, schema::Schema, schema::SchemaObject,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::v1::inventory::{
    BootPartitionContents, ConfigReconcilerInventoryResult, OrphanedDataset,
    RemoveMupdateOverrideInventory,
};
use crate::v14::inventory::OmicronSledConfig;

/// Describes the last attempt made by the sled-agent-config-reconciler to
/// reconcile the current sled config against the actual state of the sled.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ConfigReconcilerInventory {
    pub last_reconciled_config: OmicronSledConfig,
    pub external_disks:
        BTreeMap<PhysicalDiskUuid, ConfigReconcilerInventoryResult>,
    pub datasets: BTreeMap<DatasetUuid, ConfigReconcilerInventoryResult>,
    pub orphaned_datasets: IdOrdMap<OrphanedDataset>,
    pub zones: BTreeMap<OmicronZoneUuid, ConfigReconcilerInventoryResult>,
    pub boot_partitions: BootPartitionContents,
    /// The result of removing the mupdate override file on disk.
    ///
    /// `None` if `remove_mupdate_override` was not provided in the sled config.
    pub remove_mupdate_override: Option<RemoveMupdateOverrideInventory>,
}

/// An attempt at resolving a single measurement file to a valid path
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
pub struct SingleMeasurementInventory {
    #[schemars(schema_with = "path_schema")]
    pub path: Utf8PathBuf,
    pub result: ConfigReconcilerInventoryResult,
}

impl IdOrdItem for SingleMeasurementInventory {
    type Key<'a> = &'a Utf8PathBuf;
    fn key(&self) -> Self::Key<'_> {
        &self.path
    }
    id_upcast!();
}

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
fn path_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(generator).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}
