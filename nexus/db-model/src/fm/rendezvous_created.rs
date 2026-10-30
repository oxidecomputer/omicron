// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Marker rows recording that FM rendezvous has successfully created an alert,
//! support bundle, or Active Problem. See the corresponding table comments in
//! `schema/crdb/dbinit.sql` for the tombstone-and-idempotency semantics that
//! motivate these tables.

use crate::typed_generation::DbTypedGeneration;
use crate::typed_uuid::DbTypedUuid;
use nexus_db_schema::schema::rendezvous_active_problem_created;
use nexus_db_schema::schema::rendezvous_alert_created;
use nexus_db_schema::schema::rendezvous_support_bundle_created;
use omicron_generation_kinds::{
    SitrepActiveProblemGeneration, SitrepActiveProblemGenerationKind,
    SitrepAlertGeneration, SitrepAlertGenerationKind,
    SitrepSupportBundleGeneration, SitrepSupportBundleGenerationKind,
};
use omicron_uuid_kinds::{ActiveProblemKind, ActiveProblemUuid};
use omicron_uuid_kinds::{AlertKind, AlertUuid};
use omicron_uuid_kinds::{SupportBundleKind, SupportBundleUuid};

#[derive(Queryable, Insertable, Debug, Clone, Selectable, PartialEq)]
#[diesel(table_name = rendezvous_alert_created)]
pub struct RendezvousAlertCreated {
    alert_id: DbTypedUuid<AlertKind>,
    created_at_generation: DbTypedGeneration<SitrepAlertGenerationKind>,
}

impl RendezvousAlertCreated {
    pub fn new(alert_id: AlertUuid, generation: SitrepAlertGeneration) -> Self {
        Self {
            alert_id: alert_id.into(),
            created_at_generation: generation.into(),
        }
    }

    pub fn alert_id(&self) -> AlertUuid {
        self.alert_id.into()
    }

    pub fn created_at_generation(&self) -> SitrepAlertGeneration {
        self.created_at_generation.into()
    }
}

#[derive(Queryable, Insertable, Debug, Clone, Selectable, PartialEq)]
#[diesel(table_name = rendezvous_support_bundle_created)]
pub struct RendezvousSupportBundleCreated {
    support_bundle_id: DbTypedUuid<SupportBundleKind>,
    created_at_generation: DbTypedGeneration<SitrepSupportBundleGenerationKind>,
}

impl RendezvousSupportBundleCreated {
    pub fn new(
        support_bundle_id: SupportBundleUuid,
        generation: SitrepSupportBundleGeneration,
    ) -> Self {
        Self {
            support_bundle_id: support_bundle_id.into(),
            created_at_generation: generation.into(),
        }
    }

    pub fn support_bundle_id(&self) -> SupportBundleUuid {
        self.support_bundle_id.into()
    }

    pub fn created_at_generation(&self) -> SitrepSupportBundleGeneration {
        self.created_at_generation.into()
    }
}

#[derive(Queryable, Insertable, Debug, Clone, Selectable, PartialEq)]
#[diesel(table_name = rendezvous_active_problem_created)]
pub struct RendezvousActiveProblemCreated {
    problem_id: DbTypedUuid<ActiveProblemKind>,
    created_at_generation: DbTypedGeneration<SitrepActiveProblemGenerationKind>,
}

impl RendezvousActiveProblemCreated {
    pub fn new(
        problem_id: ActiveProblemUuid,
        generation: SitrepActiveProblemGeneration,
    ) -> Self {
        Self {
            problem_id: problem_id.into(),
            created_at_generation: generation.into(),
        }
    }

    pub fn problem_id(&self) -> ActiveProblemUuid {
        self.problem_id.into()
    }

    pub fn created_at_generation(&self) -> SitrepActiveProblemGeneration {
        self.created_at_generation.into()
    }
}
