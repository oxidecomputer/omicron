// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ByteCount, Generation, SledState, SqlU16, SqlU32};
use crate::collection::DatastoreCollectionConfig;
use crate::ipv6;
use crate::sled::shared::Baseboard;
use crate::sled_cpu_family::SledCpuFamily;
use crate::sled_policy::DbSledPolicy;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_db_schema::schema::{physical_disk, sled, zpool};
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_types::deployment::execution;
use nexus_types::{
    external_api::{shared, views},
    identity::Asset,
    internal_api::params,
};
use omicron_uuid_kinds::{GenericUuid, SledUuid};
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
    pub revision: u32,
}

/// Hardware information about the sled.
pub struct SledSystemHardware {
    pub is_scrimlet: bool,
    pub usable_hardware_threads: u32,
    pub usable_physical_ram: ByteCount,

    // current VMM reservoir size
    pub reservoir_size: ByteCount,

    pub cpu_family: SledCpuFamily,
}

/// Database representation of a Sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = sled)]
// TODO-cleanup: use #[asset(uuid_kind = SledKind)]
pub struct Sled {
    #[diesel(embed)]
    pub identity: SledIdentity,
    time_deleted: Option<DateTime<Utc>>,
    pub rcgen: Generation,

    pub rack_id: Uuid,

    is_scrimlet: bool,
    serial_number: String,
    part_number: String,
    revision: SqlU32,

    pub usable_hardware_threads: SqlU32,
    pub usable_physical_ram: ByteCount,
    pub reservoir_size: ByteCount,

    // ServiceAddress (Sled Agent).
    pub ip: ipv6::Ipv6Addr,
    pub port: SqlU16,

    /// The last IP address provided to a propolis instance on this sled
    pub last_used_address: ipv6::Ipv6Addr,

    #[diesel(column_name = sled_policy)]
    policy: DbSledPolicy,

    #[diesel(column_name = sled_state)]
    state: SledState,

    /// A generation number owned and incremented by sled-agent
    ///
    /// This is specifically distinct from `rcgen`, which is incremented by
    /// child resources as part of `DatastoreCollectionConfig`.
    pub sled_agent_gen: Generation,

    // ServiceAddress (Repo Depot API). Uses `ip`.
    pub repo_depot_port: SqlU16,

    /// The family of this sled's CPU.
    ///
    /// This is primarily useful for questions about instance CPU platform
    /// compatibility; it is too broad for topology-related sled selection
    /// and more precise than a more general report of microarchitecture. We
    /// likely should include much more about the sled's CPU alongside this for
    /// those broader questions and reporting (see
    /// https://github.com/oxidecomputer/omicron/issues/8730 for examples).
    pub cpu_family: SledCpuFamily,
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

    pub fn part_number(&self) -> &str {
        &self.part_number
    }

    /// The policy here is the `views::SledPolicy` because we expect external
    /// users to always use that.
    pub fn policy(&self) -> views::SledPolicy {
        self.policy.into()
    }

    /// Returns the sled's state.
    pub fn state(&self) -> SledState {
        self.state
    }

    pub fn time_modified(&self) -> DateTime<Utc> {
        self.identity.time_modified
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
                revision: *sled.revision,
            },
            policy: sled.policy.into(),
            state: sled.state.into(),
            usable_hardware_threads: sled.usable_hardware_threads.0,
            usable_physical_ram: *sled.usable_physical_ram,
        }
    }
}

impl From<Sled> for execution::Sled {
    fn from(sled: Sled) -> Self {
        Self::new(
            SledUuid::from_untyped_uuid(sled.id()),
            sled.policy(),
            sled.address(),
            *sled.repo_depot_port,
            if sled.is_scrimlet {
                SledRole::Scrimlet
            } else {
                SledRole::Gimlet
            },
        )
    }
}

impl From<Sled> for params::SledAgentInfo {
    fn from(sled: Sled) -> Self {
        let role = if sled.is_scrimlet {
            SledRole::Scrimlet
        } else {
            SledRole::Gimlet
        };
        let decommissioned = match sled.state {
            SledState::Active => false,
            SledState::Decommissioned => true,
        };
        Self {
            sa_address: sled.address(),
            repo_depot_port: sled.repo_depot_port.into(),
            role,
            baseboard: Baseboard {
                serial: sled.serial_number.clone(),
                part: sled.part_number.clone(),
                revision: *sled.revision,
            },
            usable_hardware_threads: sled.usable_hardware_threads.into(),
            usable_physical_ram: sled.usable_physical_ram.into(),
            reservoir_size: sled.reservoir_size.into(),
            generation: sled.sled_agent_gen.into(),
            cpu_family: sled.cpu_family.into(),
            decommissioned,
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

/// Form of `Sled` used for updates from sled-agent. This is missing some
/// columns that are present in `Sled` because sled-agent doesn't control them.
#[derive(Debug, Clone)]
pub struct SledUpdate {
    id: Uuid,

    pub rack_id: Uuid,

    is_scrimlet: bool,
    serial_number: String,
    part_number: String,
    revision: SqlU32,

    pub usable_hardware_threads: SqlU32,
    pub usable_physical_ram: ByteCount,
    pub reservoir_size: ByteCount,

    // ServiceAddress (Sled Agent).
    pub ip: ipv6::Ipv6Addr,
    pub port: SqlU16,

    // ServiceAddress (Repo Depot API). Uses `ip`.
    pub repo_depot_port: SqlU16,

    pub cpu_family: SledCpuFamily,

    // Generation number - owned and incremented by sled-agent.
    pub sled_agent_gen: Generation,
}

impl SledUpdate {
    pub fn new(
        id: Uuid,
        addr: SocketAddrV6,
        repo_depot_port: u16,
        baseboard: SledBaseboard,
        hardware: SledSystemHardware,
        rack_id: Uuid,
        sled_agent_gen: Generation,
    ) -> Self {
        Self {
            id,
            rack_id,
            is_scrimlet: hardware.is_scrimlet,
            serial_number: baseboard.serial_number,
            part_number: baseboard.part_number,
            revision: SqlU32(baseboard.revision),
            usable_hardware_threads: SqlU32::new(
                hardware.usable_hardware_threads,
            ),
            usable_physical_ram: hardware.usable_physical_ram,
            reservoir_size: hardware.reservoir_size,
            ip: addr.ip().into(),
            port: addr.port().into(),
            repo_depot_port: repo_depot_port.into(),
            cpu_family: hardware.cpu_family,
            sled_agent_gen,
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
            // By default, sleds start in-service.
            policy: DbSledPolicy::InService,
            // Currently, new sleds start in the "active" state.
            state: SledState::Active,
            usable_hardware_threads: self.usable_hardware_threads,
            usable_physical_ram: self.usable_physical_ram,
            reservoir_size: self.reservoir_size,
            ip: self.ip,
            port: self.port,
            repo_depot_port: self.repo_depot_port,
            last_used_address,
            sled_agent_gen: self.sled_agent_gen,
            cpu_family: self.cpu_family,
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

mod diesel_util {
    use crate::{sled_policy::DbSledPolicy, to_db_sled_policy};
    use diesel::{
        helper_types::{And, EqAny},
        prelude::*,
        query_dsl::methods::FilterDsl,
    };
    use nexus_db_schema::schema::sled::{sled_policy, sled_state};
    use nexus_types::{
        deployment::SledFilter,
        external_api::views::{SledPolicy, SledState},
    };

    /// An extension trait to apply a [`SledFilter`] to a Diesel expression.
    ///
    /// This is applicable to any Diesel expression which includes the `sled`
    /// table.
    ///
    /// This needs to live here, rather than in `nexus-db-queries`, because it
    /// names the `DbSledPolicy` type which is private to this crate.
    pub trait ApplySledFilterExt {
        type Output;

        /// Applies a [`SledFilter`] to a Diesel expression.
        fn sled_filter(self, filter: SledFilter) -> Self::Output;
    }

    impl<E> ApplySledFilterExt for E
    where
        E: FilterDsl<SledFilterQuery>,
    {
        type Output = E::Output;

        fn sled_filter(self, filter: SledFilter) -> Self::Output {
            use nexus_db_schema::schema::sled::dsl as sled_dsl;

            // These are only boxed for ease of reference above.
            let all_matching_policies: BoxedIterator<DbSledPolicy> = Box::new(
                SledPolicy::all_matching(filter).map(to_db_sled_policy),
            );
            let all_matching_states: BoxedIterator<crate::SledState> =
                Box::new(SledState::all_matching(filter).map(Into::into));

            FilterDsl::filter(
                self,
                sled_dsl::sled_policy
                    .eq_any(all_matching_policies)
                    .and(sled_dsl::sled_state.eq_any(all_matching_states)),
            )
        }
    }

    type BoxedIterator<T> = Box<dyn Iterator<Item = T>>;
    type SledFilterQuery = And<
        EqAny<sled_policy, BoxedIterator<DbSledPolicy>>,
        EqAny<sled_state, BoxedIterator<crate::SledState>>,
    >;
}

pub use diesel_util::ApplySledFilterExt;
