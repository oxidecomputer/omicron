// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use illumos_utils::zpool::ZpoolName;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// The type of a dataset, and an auxiliary information necessary
/// to successfully launch a zone managing the associated data.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatasetKind {
    CockroachDb,
    Crucible,
    Clickhouse,
    ClickhouseKeeper,
    ExternalDns,
    InternalDns,
}

impl From<DatasetKind> for sled_agent_client::types::DatasetKind {
    fn from(k: DatasetKind) -> Self {
        use DatasetKind::*;
        match k {
            CockroachDb => Self::CockroachDb,
            Crucible => Self::Crucible,
            Clickhouse => Self::Clickhouse,
            ClickhouseKeeper => Self::ClickhouseKeeper,
            ExternalDns => Self::ExternalDns,
            InternalDns => Self::InternalDns,
        }
    }
}

impl From<DatasetKind> for nexus_client::types::DatasetKind {
    fn from(k: DatasetKind) -> Self {
        use DatasetKind::*;
        match k {
            CockroachDb => Self::Cockroach,
            Crucible => Self::Crucible,
            Clickhouse => Self::Clickhouse,
            ClickhouseKeeper => Self::ClickhouseKeeper,
            ExternalDns => Self::ExternalDns,
            InternalDns => Self::InternalDns,
        }
    }
}

impl std::fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DatasetKind::*;
        let s = match self {
            Crucible => "crucible",
            CockroachDb { .. } => "cockroachdb",
            Clickhouse => "clickhouse",
            ClickhouseKeeper => "clickhouse_keeper",
            ExternalDns { .. } => "external_dns",
            InternalDns { .. } => "internal_dns",
        };
        write!(f, "{}", s)
    }
}

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

impl From<DatasetName> for sled_agent_client::types::DatasetName {
    fn from(n: DatasetName) -> Self {
        Self {
            pool_name: sled_agent_client::types::ZpoolName::from_str(
                &n.pool().to_string(),
            )
            .unwrap(),
            kind: n.dataset().clone().into(),
        }
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
        serde_json::to_string(&name).unwrap();
    }
}
