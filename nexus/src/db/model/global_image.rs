// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{impl_enum_type, BlockSize, ByteCount, Digest};
use crate::db::identity::Resource;
use crate::db::schema::global_image;
use crate::external_api::{params, views};
use db_macros::Resource;
use omicron_common::api::external;
use serde::{Deserialize, Serialize};
use std::io::Write;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "distribution"))]
    pub struct DistributionEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq, Serialize, Deserialize)]
    #[diesel(sql_type = DistributionEnum)]
    pub enum Distribution;

    Alpine => b"alpine"
    Debian => b"debian"
    Ubuntu => b"ubuntu"
    Rocky => b"rocky"
    CentOS => b"centos"
    Fedora => b"fedora"
    FreeBSD => b"freebsd"
);

impl From<params::Distribution> for Distribution {
    fn from(distro: params::Distribution) -> Self {
        match distro {
            params::Distribution::Alpine => Self::Alpine,
            params::Distribution::Debian => Self::Debian,
            params::Distribution::Ubuntu => Self::Ubuntu,
            params::Distribution::Rocky => Self::Rocky,
            params::Distribution::CentOS => Self::CentOS,
            params::Distribution::Fedora => Self::Fedora,
        }
    }
}

impl TryFrom<Distribution> for params::Distribution {
    type Error = external::Error;

    fn try_from(
        distro: Distribution,
    ) -> Result<params::Distribution, Self::Error> {
        match distro {
            Distribution::Alpine => Ok(params::Distribution::Alpine),
            Distribution::Debian => Ok(params::Distribution::Debian),
            Distribution::Ubuntu => Ok(params::Distribution::Ubuntu),
            Distribution::Rocky => Ok(params::Distribution::Rocky),
            Distribution::CentOS => Ok(params::Distribution::CentOS),
            Distribution::Fedora => Ok(params::Distribution::Fedora),
            Distribution::FreeBSD => Err(Self::Error::type_version_mismatch(
                "FreeBSD not part of params::Distribution yet!",
            )),
        }
    }
}

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = global_image)]
pub struct GlobalImage {
    #[diesel(embed)]
    pub identity: GlobalImageIdentity,

    pub volume_id: Uuid,
    pub url: Option<String>,
    pub distribution: Distribution,
    pub version: String,
    pub digest: Option<Digest>,

    pub block_size: BlockSize,

    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,
}

impl TryFrom<GlobalImage> for views::GlobalImage {
    type Error = external::Error;

    fn try_from(image: GlobalImage) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: image.identity(),
            url: image.url,
            distribution: image.distribution.try_into()?,
            version: image.version,
            digest: image.digest.map(|x| x.into()),
            block_size: image.block_size.into(),
            size: image.size.into(),
        })
    }
}
