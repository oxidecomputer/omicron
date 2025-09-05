// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    Generation, PhysicalDiskKind, PhysicalDiskPolicy, PhysicalDiskState,
};
use crate::DbTypedUuid;
use crate::collection::DatastoreCollectionConfig;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_db_schema::schema::{physical_disk, zpool};
use nexus_types::{external_api::views, identity::Asset};
use omicron_uuid_kinds::PhysicalDiskKind as PhysicalDiskUuidKind;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;

/// Physical disk attached to sled.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = physical_disk)]
#[asset(uuid_kind = PhysicalDiskKind)]
pub struct PhysicalDisk {
    #[diesel(embed)]
    identity: PhysicalDiskIdentity,
    time_deleted: Option<DateTime<Utc>>,
    rcgen: Generation,

    pub vendor: String,
    pub serial: String,
    pub model: String,

    pub variant: PhysicalDiskKind,
    pub sled_id: DbTypedUuid<SledKind>,
    pub disk_policy: PhysicalDiskPolicy,
    pub disk_state: PhysicalDiskState,
}

impl PhysicalDisk {
    /// Creates a new in-service, active disk
    pub fn new(
        id: PhysicalDiskUuid,
        vendor: String,
        serial: String,
        model: String,
        variant: PhysicalDiskKind,
        sled_id: SledUuid,
    ) -> Self {
        Self {
            identity: PhysicalDiskIdentity::new(id),
            time_deleted: None,
            rcgen: Generation::new(),
            vendor,
            serial,
            model,
            variant,
            sled_id: sled_id.into(),
            disk_policy: PhysicalDiskPolicy::InService,
            disk_state: PhysicalDiskState::Active,
        }
    }

    pub fn id(&self) -> PhysicalDiskUuid {
        self.identity.id.into()
    }

    pub fn time_deleted(&self) -> Option<DateTime<Utc>> {
        self.time_deleted
    }

    pub fn sled_id(&self) -> SledUuid {
        self.sled_id.into()
    }
}

impl From<PhysicalDisk> for views::PhysicalDisk {
    fn from(disk: PhysicalDisk) -> Self {
        Self {
            identity: disk.identity(),
            policy: disk.disk_policy.into(),
            state: disk.disk_state.into(),
            sled_id: Some(disk.sled_id.into()),
            vendor: disk.vendor,
            serial: disk.serial,
            model: disk.model,
            form_factor: disk.variant.into(),
        }
    }
}

impl DatastoreCollectionConfig<super::Zpool> for PhysicalDisk {
    type CollectionId = DbTypedUuid<PhysicalDiskUuidKind>;
    type GenerationNumberColumn = physical_disk::dsl::rcgen;
    type CollectionTimeDeletedColumn = physical_disk::dsl::time_deleted;
    type CollectionIdColumn = zpool::dsl::sled_id;
}

mod diesel_util {
    use diesel::{
        helper_types::{And, EqAny},
        prelude::*,
        query_dsl::methods::FilterDsl,
    };
    use nexus_types::{
        deployment::DiskFilter,
        external_api::views::{PhysicalDiskPolicy, PhysicalDiskState},
    };

    /// An extension trait to apply a [`DiskFilter`] to a Diesel expression.
    ///
    /// This is applicable to any Diesel expression which includes the `physical_disk`
    /// table.
    ///
    /// This needs to live here, rather than in `nexus-db-queries`, because it
    /// names the `DbPhysicalDiskPolicy` type which is private to this crate.
    pub trait ApplyPhysicalDiskFilterExt {
        type Output;

        /// Applies a [`DiskFilter`] to a Diesel expression.
        fn physical_disk_filter(self, filter: DiskFilter) -> Self::Output;
    }

    impl<E> ApplyPhysicalDiskFilterExt for E
    where
        E: FilterDsl<PhysicalDiskFilterQuery>,
    {
        type Output = E::Output;

        fn physical_disk_filter(self, filter: DiskFilter) -> Self::Output {
            use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;

            // These are only boxed for ease of reference above.
            let all_matching_policies: BoxedIterator<
                crate::PhysicalDiskPolicy,
            > = Box::new(
                PhysicalDiskPolicy::all_matching(filter).map(Into::into),
            );
            let all_matching_states: BoxedIterator<crate::PhysicalDiskState> =
                Box::new(
                    PhysicalDiskState::all_matching(filter).map(Into::into),
                );

            FilterDsl::filter(
                self,
                physical_disk_dsl::disk_policy
                    .eq_any(all_matching_policies)
                    .and(
                        physical_disk_dsl::disk_state
                            .eq_any(all_matching_states),
                    ),
            )
        }
    }

    type BoxedIterator<T> = Box<dyn Iterator<Item = T>>;
    type PhysicalDiskFilterQuery = And<
        EqAny<
            nexus_db_schema::schema::physical_disk::disk_policy,
            BoxedIterator<crate::PhysicalDiskPolicy>,
        >,
        EqAny<
            nexus_db_schema::schema::physical_disk::disk_state,
            BoxedIterator<crate::PhysicalDiskState>,
        >,
    >;
}

pub use diesel_util::ApplyPhysicalDiskFilterExt;
