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

impl_enum_type!(
    UserDataExportStateEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum UserDataExportState;

    // Enum values
    Requested => b"requested"
    Assigning => b"assigning"
    Live => b"live"
    Deleting => b"deleting"
    Deleted => b"deleted"
);

/// Instead of working directly with the UserDataExportRecord, callers can use
/// this enum instead, where the call site only cares of the record is live or
/// not.
pub enum UserDataExport {
    NotLive,

    Live { pantry_address: SocketAddrV6, volume_id: VolumeUuid },
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
///
/// The record transitions through the following states:
///
/// ```text
///     Requested   <--              ---
///                   |              |
///         |         |              |
///         v         |              |  responsibility of user
///                   |              |  export create saga
///     Assigning   --               |
///                                  |
///         |                        |
///         v                        ---
///                                  ---
///       Live      <--              |
///                   |              |
///         |         |              |
///         v         |              | responsibility of user
///                   |              | export delete saga
///     Deleting    --               |
///                                  |
///         |                        |
///         v                        |
///                                  ---
///      Deleted
/// ```
///
/// which are captured in the UserDataExportState enum. Annotated on the right
/// are which sagas are responsible for which state transitions. The state
/// transitions themselves are performed by these sagas and all involve a query
/// that:
///
///  - checks that the starting state (and other values as required) make sense
///  - updates the state while setting a unique operating_saga_id id (and any
///    other fields as appropriate)
///
/// As multiple background tasks will be waking up, checking to see what sagas
/// need to be triggered, and requesting that these sagas run, this is meant to
/// block multiple sagas from running at the same time in an effort to cut down
/// on interference - most will unwind at the first step of performing this
/// state transition instead of somewhere in the middle. This is not required
/// for correctness as each saga node can deal with this type of interference.
#[derive(Queryable, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = user_data_export)]
pub struct UserDataExportRecord {
    id: DbTypedUuid<UserDataExportKind>,

    state: UserDataExportState,
    operating_saga_id: Option<Uuid>,
    generation: i64,

    resource_id: Uuid,
    resource_type: UserDataExportResourceType,
    resource_deleted: bool,

    pantry_ip: Option<ipv6::Ipv6Addr>,
    pantry_port: Option<SqlU16>,
    volume_id: Option<DbTypedUuid<VolumeKind>>,
}

impl UserDataExportRecord {
    pub fn new(
        id: UserDataExportUuid,
        resource: UserDataExportResource,
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

            state: UserDataExportState::Requested,
            operating_saga_id: None,
            generation: 0,

            resource_type,
            resource_id,
            resource_deleted: false,

            pantry_ip: None,
            pantry_port: None,
            volume_id: None,
        }
    }

    pub fn id(&self) -> UserDataExportUuid {
        self.id.into()
    }

    pub fn state(&self) -> UserDataExportState {
        self.state
    }

    pub fn operating_saga_id(&self) -> Option<Uuid> {
        self.operating_saga_id
    }

    pub fn generation(&self) -> i64 {
        self.generation
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

    pub fn pantry_address(&self) -> Option<SocketAddrV6> {
        match (&self.pantry_ip, &self.pantry_port) {
            (Some(pantry_ip), Some(pantry_port)) => Some(SocketAddrV6::new(
                (*pantry_ip).into(),
                (*pantry_port).into(),
                0,
                0,
            )),

            (_, _) => None,
        }
    }

    pub fn volume_id(&self) -> Option<VolumeUuid> {
        self.volume_id.map(|i| i.into())
    }

    pub fn is_live(&self) -> Result<UserDataExport, &'static str> {
        match self.state {
            UserDataExportState::Requested
            | UserDataExportState::Assigning
            | UserDataExportState::Deleting
            | UserDataExportState::Deleted => Ok(UserDataExport::NotLive),

            UserDataExportState::Live => {
                let Some(pantry_ip) = self.pantry_ip else {
                    return Err("pantry_ip is None!");
                };

                let Some(pantry_port) = self.pantry_port else {
                    return Err("pantry_port is None!");
                };

                let Some(volume_id) = self.volume_id else {
                    return Err("volume_id is None!");
                };

                Ok(UserDataExport::Live {
                    pantry_address: SocketAddrV6::new(
                        pantry_ip.into(),
                        *pantry_port,
                        0,
                        0,
                    ),

                    volume_id: volume_id.into(),
                })
            }
        }
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
