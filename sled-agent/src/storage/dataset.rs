// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::params::DatasetKind;
use illumos_utils::zpool::ZpoolName;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, JsonSchema,
)]
pub struct DatasetName {
    // A unique identifier for the Zpool on which the dataset is stored.
    pool_name: ZpoolName,
    // A name for the dataset within the Zpool.
    kind: DatasetKind,
}

impl DatasetName {
    pub fn new(pool_name: ZpoolName, kind: DatasetKind) -> Self {
        Self { pool_name, kind }
    }

    pub fn pool(&self) -> &ZpoolName {
        &self.pool_name
    }

    pub fn dataset(&self) -> &DatasetKind {
        &self.kind
    }

    pub fn full(&self) -> String {
        format!("{}/{}", self.pool_name, self.kind)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn serialize_dataset_name() {
        let pool = ZpoolName::new_internal(Uuid::new_v4());
        let kind = DatasetKind::Crucible;
        let name = DatasetName::new(pool, kind);
        toml::to_string(&name).unwrap();
    }
}
