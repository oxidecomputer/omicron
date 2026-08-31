// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::impl_enum_type;
use crate::typed_generation::DbTypedGeneration;
use crate::typed_uuid::DbTypedUuid;
use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use iddqd::{IdOrdItem, id_upcast};
use nexus_db_schema::schema::rendezvous_sled_bp_availability;
use omicron_generation_kinds::{
    UpdateDispositionGeneration, UpdateDispositionGenerationKind,
};
use omicron_uuid_kinds::BlueprintKind;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    SledBpAvailabilityEnum:

    /// The availability state of a sled in the `rendezvous_sled_bp_availability`
    /// table.
    ///
    /// See the table comment in `dbinit.sql`.
    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Eq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
    )]
    pub enum DbSledBpAvailability;

    Available => b"available"
    Unavailable => b"unavailable"
    Decommissioned => b"decommissioned"
);

impl DbSledBpAvailability {
    /// Return a static string label for this availability state, suitable for
    /// display (e.g. in `omdb`).
    pub fn label(self) -> &'static str {
        match self {
            DbSledBpAvailability::Available => "available",
            DbSledBpAvailability::Unavailable => "unavailable",
            DbSledBpAvailability::Decommissioned => "decommissioned",
        }
    }
}

/// Possible availability states for active sleds.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActiveSledBpAvailability {
    Available,
    Unavailable,
}

impl From<ActiveSledBpAvailability> for DbSledBpAvailability {
    fn from(value: ActiveSledBpAvailability) -> Self {
        match value {
            ActiveSledBpAvailability::Available => {
                DbSledBpAvailability::Available
            }
            ActiveSledBpAvailability::Unavailable => {
                DbSledBpAvailability::Unavailable
            }
        }
    }
}

/// The availability of a sled in the blueprint.
///
/// Made out of the `bp_availability` and `update_disposition_generation`
/// columns.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SledBpAvailabilityState {
    /// The sled is active in the blueprint.
    Active {
        /// The availability of the sled.
        availability: ActiveSledBpAvailability,

        /// The update disposition generation of the sled.
        update_disposition_generation: UpdateDispositionGeneration,
    },
    /// The sled is decommissioned.
    Decommissioned,
}

/// Database representation of a sled tracked by the `rendezvous_sled_bp_availability`
/// table.
///
/// A row is created for each sled observed in a target blueprint by a
/// rendezvous pass (or by the migration backfill).
#[derive(Queryable, Insertable, Debug, Clone, Selectable, PartialEq)]
#[diesel(table_name = rendezvous_sled_bp_availability)]
pub struct RendezvousSledBpAvailability {
    sled_id: DbTypedUuid<SledKind>,
    bp_availability: DbSledBpAvailability,
    update_disposition_generation:
        Option<DbTypedGeneration<UpdateDispositionGenerationKind>>,
    blueprint_id: DbTypedUuid<BlueprintKind>,
    time_created: DateTime<Utc>,
    time_modified: DateTime<Utc>,
}

impl RendezvousSledBpAvailability {
    pub fn sled_id(&self) -> SledUuid {
        self.sled_id.into()
    }

    /// Split a [`SledBpAvailabilityState`] into the columns stored on
    /// `rendezvous_sled_bp_availability`.
    ///
    /// See [`RendezvousSledBpAvailability::state`] for the inverse.
    fn state_columns(
        state: SledBpAvailabilityState,
    ) -> (
        DbSledBpAvailability,
        Option<DbTypedGeneration<UpdateDispositionGenerationKind>>,
    ) {
        match state {
            SledBpAvailabilityState::Active {
                availability,
                update_disposition_generation,
            } => (
                availability.into(),
                Some(update_disposition_generation.into()),
            ),
            SledBpAvailabilityState::Decommissioned => {
                (DbSledBpAvailability::Decommissioned, None)
            }
        }
    }

    /// Reassemble the [`SledBpAvailabilityState`] from this row's
    /// `(bp_availability, update_disposition_generation)` columns.
    pub fn state(&self) -> anyhow::Result<SledBpAvailabilityState> {
        reassemble_state(
            self.bp_availability,
            self.update_disposition_generation,
        )
        .with_context(|| {
            format!(
                "invalid rendezvous_sled_bp_availability row for sled {}",
                self.sled_id()
            )
        })
    }

    /// Return the raw `bp_availability` column.
    pub fn bp_availability(&self) -> DbSledBpAvailability {
        self.bp_availability
    }

    pub fn blueprint_id(&self) -> BlueprintUuid {
        self.blueprint_id.into()
    }
}

fn reassemble_state(
    bp_availability: DbSledBpAvailability,
    update_disposition_generation: Option<
        DbTypedGeneration<UpdateDispositionGenerationKind>,
    >,
) -> anyhow::Result<SledBpAvailabilityState> {
    match (bp_availability, update_disposition_generation) {
        (DbSledBpAvailability::Available, Some(generation)) => {
            Ok(SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Available,
                update_disposition_generation: generation.into(),
            })
        }
        (DbSledBpAvailability::Unavailable, Some(generation)) => {
            Ok(SledBpAvailabilityState::Active {
                availability: ActiveSledBpAvailability::Unavailable,
                update_disposition_generation: generation.into(),
            })
        }
        (DbSledBpAvailability::Decommissioned, None) => {
            Ok(SledBpAvailabilityState::Decommissioned)
        }
        // Invalid cases (there's a CHECK constraint to enforce this).
        (
            bp_availability @ (DbSledBpAvailability::Available
            | DbSledBpAvailability::Unavailable),
            None,
        ) => bail!(
            "bp_availability is '{}' but update_disposition_generation is \
             NULL (expected a generation)",
            bp_availability.label(),
        ),
        (DbSledBpAvailability::Decommissioned, Some(generation)) => bail!(
            "bp_availability is 'decommissioned' but \
             update_disposition_generation is {:?} (expected NULL)",
            generation,
        ),
    }
}

impl IdOrdItem for RendezvousSledBpAvailability {
    type Key<'a> = SledUuid;

    fn key(&self) -> Self::Key<'_> {
        self.sled_id()
    }

    id_upcast!();
}

/// The form of [`RendezvousSledBpAvailability`] used to upsert a sled's
/// availability.
#[derive(Debug, Clone)]
pub struct RendezvousSledBpAvailabilityUpdate {
    sled_id: SledUuid,
    availability: ActiveSledBpAvailability,
    update_disposition_generation: UpdateDispositionGeneration,
    blueprint_id: BlueprintUuid,
}

impl RendezvousSledBpAvailabilityUpdate {
    pub fn new(
        sled_id: SledUuid,
        availability: ActiveSledBpAvailability,
        update_disposition_generation: UpdateDispositionGeneration,
        blueprint_id: BlueprintUuid,
    ) -> Self {
        Self {
            sled_id,
            availability,
            update_disposition_generation,
            blueprint_id,
        }
    }

    pub fn update_disposition_generation(&self) -> UpdateDispositionGeneration {
        self.update_disposition_generation
    }

    /// Convert self into an insertable row.
    pub fn into_insertable(self) -> RendezvousSledBpAvailability {
        let now = Utc::now();
        let (bp_availability, update_disposition_generation) =
            RendezvousSledBpAvailability::state_columns(
                SledBpAvailabilityState::Active {
                    availability: self.availability,
                    update_disposition_generation: self
                        .update_disposition_generation,
                },
            );
        RendezvousSledBpAvailability {
            sled_id: self.sled_id.into(),
            bp_availability,
            update_disposition_generation,
            blueprint_id: self.blueprint_id.into(),
            time_created: now,
            time_modified: now,
        }
    }
}

/// The form of [`RendezvousSledBpAvailability`] used to decommission a sled.
#[derive(Debug, Clone)]
pub struct RendezvousSledBpAvailabilityDecommission {
    sled_id: SledUuid,
    blueprint_id: BlueprintUuid,
}

impl RendezvousSledBpAvailabilityDecommission {
    pub fn new(sled_id: SledUuid, blueprint_id: BlueprintUuid) -> Self {
        Self { sled_id, blueprint_id }
    }

    /// Convert self into an insertable row.
    pub fn into_insertable(self) -> RendezvousSledBpAvailability {
        let now = Utc::now();
        let (bp_availability, update_disposition_generation) =
            RendezvousSledBpAvailability::state_columns(
                SledBpAvailabilityState::Decommissioned,
            );
        RendezvousSledBpAvailability {
            sled_id: self.sled_id.into(),
            bp_availability,
            update_disposition_generation,
            blueprint_id: self.blueprint_id.into(),
            time_created: now,
            time_modified: now,
        }
    }
}
