// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::params;
use omicron_common::api::external;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "block_size", schema = "public"))]
    pub struct BlockSizeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = BlockSizeEnum)]
    pub enum BlockSize;

    // Enum values
    Traditional => b"512"
    Iso => b"2048"
    AdvancedFormat => b"4096"
);

impl BlockSize {
    pub fn to_bytes(&self) -> u32 {
        match self {
            BlockSize::Traditional => 512,
            BlockSize::Iso => 2048,
            BlockSize::AdvancedFormat => 4096,
        }
    }
}

impl Into<external::ByteCount> for BlockSize {
    fn into(self) -> external::ByteCount {
        external::ByteCount::from(self.to_bytes())
    }
}

impl TryFrom<params::BlockSize> for BlockSize {
    type Error = anyhow::Error;
    fn try_from(block_size: params::BlockSize) -> Result<Self, Self::Error> {
        match block_size.0 {
            512 => Ok(BlockSize::Traditional),
            2048 => Ok(BlockSize::Iso),
            4096 => Ok(BlockSize::AdvancedFormat),
            _ => anyhow::bail!("invalid block size {}", block_size.0),
        }
    }
}
