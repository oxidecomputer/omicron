// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::SqlU16;
use crate::ipv6;
use crate::typed_uuid::DbTypedUuid;
use nexus_db_schema::schema::user_data_export;
use omicron_uuid_kinds::UserDataExportKind;
use omicron_uuid_kinds::UserDataExportUuid;
use omicron_uuid_kinds::VolumeKind;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddrV6;
use uuid::Uuid;

impl_enum_type!(
    UserDataExportResourceTypeEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum UserDataExportResourceType;

    // Enum values
    Snapshot => b"snapshot"
    Image => b"image"
);

// FromStr impl required for use with clap (aka omdb)
impl std::str::FromStr for UserDataExportResourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "snapshot" => Ok(UserDataExportResourceType::Snapshot),
            "image" => Ok(UserDataExportResourceType::Image),
            _ => Err(format!("unrecognized value {} for enum", s)),
        }
    }
}

impl UserDataExportResourceType {
    pub fn to_string(&self) -> String {
        String::from(match self {
            UserDataExportResourceType::Snapshot => "snapshot",
            UserDataExportResourceType::Image => "image",
        })
    }
}

/// A "user data export" object represents an attachment of a read-only volume
/// to a Pantry for the purpose of exporting data. As of this writing only
/// snapshots and images are able to be exported this way. Management of these
/// objects is done automatically by a background task.
///
/// Note that read-only volumes should never directly be constructed (read: be
/// passed to Volume::construct). Copies should be created so that the
/// appropriate reference counting for the read-only volume targets can be
/// maintained. The user data export object stores that copied Volume, among
/// other things.
#[derive(Queryable, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = user_data_export)]
pub struct UserDataExportRecord {
    id: DbTypedUuid<UserDataExportKind>,
    resource_type: UserDataExportResourceType,
    resource_id: Uuid,
    resource_deleted: bool,
    pantry_ip: ipv6::Ipv6Addr,
    pantry_port: SqlU16,
    volume_id: DbTypedUuid<VolumeKind>,
}

impl UserDataExportRecord {
    pub fn new(
        id: UserDataExportUuid,
        resource: UserDataExportResource,
        pantry_address: SocketAddrV6,
        volume_id: VolumeUuid,
    ) -> Self {
        let (resource_type, resource_id) = match resource {
            UserDataExportResource::Snapshot { id } => {
                (UserDataExportResourceType::Snapshot, id)
            }

            UserDataExportResource::Image { id } => {
                (UserDataExportResourceType::Image, id)
            }
        };

        Self {
            id: id.into(),
            resource_type,
            resource_id,
            resource_deleted: false,
            pantry_ip: ipv6::Ipv6Addr::from(*pantry_address.ip()),
            pantry_port: SqlU16::from(pantry_address.port()),
            volume_id: volume_id.into(),
        }
    }

    pub fn id(&self) -> UserDataExportUuid {
        self.id.into()
    }

    pub fn resource(&self) -> UserDataExportResource {
        match self.resource_type {
            UserDataExportResourceType::Snapshot => {
                UserDataExportResource::Snapshot { id: self.resource_id }
            }

            UserDataExportResourceType::Image => {
                UserDataExportResource::Image { id: self.resource_id }
            }
        }
    }

    pub fn deleted(&self) -> bool {
        self.resource_deleted
    }

    pub fn pantry_address(&self) -> SocketAddrV6 {
        SocketAddrV6::new(self.pantry_ip.into(), *self.pantry_port, 0, 0)
    }

    pub fn volume_id(&self) -> VolumeUuid {
        self.volume_id.into()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum UserDataExportResource {
    Snapshot { id: Uuid },

    Image { id: Uuid },
}

impl UserDataExportResource {
    pub fn type_string(&self) -> String {
        String::from(match self {
            UserDataExportResource::Snapshot { .. } => "snapshot",
            UserDataExportResource::Image { .. } => "image",
        })
    }

    pub fn id(&self) -> Uuid {
        match self {
            UserDataExportResource::Snapshot { id } => *id,
            UserDataExportResource::Image { id } => *id,
        }
    }
}
