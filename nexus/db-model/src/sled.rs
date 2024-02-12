// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ByteCount, Generation, SqlU16, SqlU32};
use crate::collection::DatastoreCollectionConfig;
use crate::schema::{physical_disk, service, sled, zpool};
use crate::{ipv6, SledProvisionState};
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_types::{external_api::shared, external_api::views, identity::Asset};
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use uuid::Uuid;

/// Baseboard information about a sled.
///
/// The combination of these columns may be used as a unique identifier for the
/// sled.
pub struct SledBaseboard {
    pub serial_number: String,
    pub part_number: String,
    pub revision: i64,
}

/// Hardware information about the sled.
pub struct SledSystemHardware {
    pub is_scrimlet: bool,
    pub usable_hardware_threads: u32,
    pub usable_physical_ram: ByteCount,

    // current VMM reservoir size
    pub reservoir_size: ByteCount,
}

/// Database representation of a Sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = sled)]
pub struct Sled {
    #[diesel(embed)]
    identity: SledIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    pub rack_id: Uuid,

    is_scrimlet: bool,
    serial_number: String,
    part_number: String,
    revision: i64,

    pub usable_hardware_threads: SqlU32,
    pub usable_physical_ram: ByteCount,
    pub reservoir_size: ByteCount,

    // ServiceAddress (Sled Agent).
    pub ip: ipv6::Ipv6Addr,
    pub port: SqlU16,

    /// The last IP address provided to a propolis instance on this sled
    pub last_used_address: ipv6::Ipv6Addr,

    provision_state: SledProvisionState,
}

impl Sled {
    pub fn is_scrimlet(&self) -> bool {
        self.is_scrimlet
    }

    pub fn ip(&self) -> Ipv6Addr {
        self.ip.into()
    }

    pub fn address(&self) -> SocketAddrV6 {
        self.address_with_port(self.port.into())
    }

    pub fn address_with_port(&self, port: u16) -> SocketAddrV6 {
        SocketAddrV6::new(self.ip(), port, 0, 0)
    }

    pub fn serial_number(&self) -> &str {
        &self.serial_number
    }

    pub fn provision_state(&self) -> SledProvisionState {
        self.provision_state
    }
}

impl From<Sled> for views::Sled {
    fn from(sled: Sled) -> Self {
        Self {
            identity: sled.identity(),
            rack_id: sled.rack_id,
            baseboard: shared::Baseboard {
                serial: sled.serial_number,
                part: sled.part_number,
                revision: sled.revision,
            },
            provision_state: sled.provision_state.into(),
            usable_hardware_threads: sled.usable_hardware_threads.0,
            usable_physical_ram: *sled.usable_physical_ram,
        }
    }
}

impl From<Sled> for nexus_types::internal_api::views::Sled {
    fn from(sled: Sled) -> Self {
        Self {
            identity: sled.identity(),
            rack_id: sled.rack_id,
            baseboard: shared::Baseboard {
                serial: sled.serial_number,
                part: sled.part_number,
                revision: sled.revision,
            },
            provision_state: sled.provision_state.into(),
            usable_hardware_threads: sled.usable_hardware_threads.0,
            usable_physical_ram: *sled.usable_physical_ram,
            address: std::net::SocketAddrV6::new(*sled.ip, *sled.port, 0, 0),
        }
    }
}

impl DatastoreCollectionConfig<super::PhysicalDisk> for Sled {
    type CollectionId = Uuid;
    type GenerationNumberColumn = sled::dsl::rcgen;
    type CollectionTimeDeletedColumn = sled::dsl::time_deleted;
    type CollectionIdColumn = physical_disk::dsl::id;
}

// TODO: Can we remove this? We have one for physical disks now.
impl DatastoreCollectionConfig<super::Zpool> for Sled {
    type CollectionId = Uuid;
    type GenerationNumberColumn = sled::dsl::rcgen;
    type CollectionTimeDeletedColumn = sled::dsl::time_deleted;
    type CollectionIdColumn = zpool::dsl::sled_id;
}

impl DatastoreCollectionConfig<super::Service> for Sled {
    type CollectionId = Uuid;
    type GenerationNumberColumn = sled::dsl::rcgen;
    type CollectionTimeDeletedColumn = sled::dsl::time_deleted;
    type CollectionIdColumn = service::dsl::sled_id;
}

/// Form of `Sled` used for updates from sled-agent. This is missing some
/// columns that are present in `Sled` because sled-agent doesn't control them.
#[derive(Debug, Clone)]
pub struct SledUpdate {
    id: Uuid,

    pub rack_id: Uuid,

    is_scrimlet: bool,
    serial_number: String,
    part_number: String,
    revision: i64,

    pub usable_hardware_threads: SqlU32,
    pub usable_physical_ram: ByteCount,
    pub reservoir_size: ByteCount,

    // ServiceAddress (Sled Agent).
    pub ip: ipv6::Ipv6Addr,
    pub port: SqlU16,
}

impl SledUpdate {
    pub fn new(
        id: Uuid,
        addr: SocketAddrV6,
        baseboard: SledBaseboard,
        hardware: SledSystemHardware,
        rack_id: Uuid,
    ) -> Self {
        Self {
            id,
            rack_id,
            is_scrimlet: hardware.is_scrimlet,
            serial_number: baseboard.serial_number,
            part_number: baseboard.part_number,
            revision: baseboard.revision,
            usable_hardware_threads: SqlU32::new(
                hardware.usable_hardware_threads,
            ),
            usable_physical_ram: hardware.usable_physical_ram,
            reservoir_size: hardware.reservoir_size,
            ip: addr.ip().into(),
            port: addr.port().into(),
        }
    }

    /// Converts self into a form used for inserts of new sleds into the
    /// database.
    ///
    /// This form adds default values for fields that are not present in
    /// `SledUpdate`.
    pub fn into_insertable(self) -> Sled {
        let last_used_address = {
            let mut segments = self.ip().segments();
            // We allocate the entire last segment to control plane services
            segments[7] =
                omicron_common::address::CP_SERVICES_RESERVED_ADDRESSES;
            ipv6::Ipv6Addr::from(Ipv6Addr::from(segments))
        };
        Sled {
            identity: SledIdentity::new(self.id),
            rcgen: Generation::new(),
            time_deleted: None,
            rack_id: self.rack_id,
            is_scrimlet: self.is_scrimlet,
            serial_number: self.serial_number,
            part_number: self.part_number,
            revision: self.revision,
            // By default, sleds start as provisionable.
            provision_state: SledProvisionState::Provisionable,
            usable_hardware_threads: self.usable_hardware_threads,
            usable_physical_ram: self.usable_physical_ram,
            reservoir_size: self.reservoir_size,
            ip: self.ip,
            port: self.port,
            last_used_address,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn is_scrimlet(&self) -> bool {
        self.is_scrimlet
    }

    pub fn ip(&self) -> Ipv6Addr {
        self.ip.into()
    }

    pub fn address(&self) -> SocketAddrV6 {
        self.address_with_port(self.port.into())
    }

    pub fn address_with_port(&self, port: u16) -> SocketAddrV6 {
        SocketAddrV6::new(self.ip(), port, 0, 0)
    }

    pub fn serial_number(&self) -> &str {
        &self.serial_number
    }
}

/// A set of constraints that can be placed on operations that select a sled.
#[derive(Clone, Debug)]
pub struct SledReservationConstraints {
    must_select_from: Vec<Uuid>,
}

impl SledReservationConstraints {
    /// Creates a constraint set with no constraints in it.
    pub fn none() -> Self {
        Self { must_select_from: Vec::new() }
    }

    /// If the constraints include a set of sleds that the caller must select
    /// from, returns `Some` and a slice containing the members of that set.
    ///
    /// If no "must select from these" constraint exists, returns None.
    pub fn must_select_from(&self) -> Option<&[Uuid]> {
        if self.must_select_from.is_empty() {
            None
        } else {
            Some(&self.must_select_from)
        }
    }
}

#[derive(Debug)]
pub struct SledReservationConstraintBuilder {
    constraints: SledReservationConstraints,
}

impl SledReservationConstraintBuilder {
    pub fn new() -> Self {
        SledReservationConstraintBuilder {
            constraints: SledReservationConstraints::none(),
        }
    }

    /// Adds a "must select from the following sled IDs" constraint. If such a
    /// constraint already exists, appends the supplied sled IDs to the "must
    /// select from" list.
    pub fn must_select_from(mut self, sled_ids: &[Uuid]) -> Self {
        self.constraints.must_select_from.extend(sled_ids);
        self
    }

    /// Builds a set of constraints from this builder's current state.
    pub fn build(self) -> SledReservationConstraints {
        self.constraints
    }
}
