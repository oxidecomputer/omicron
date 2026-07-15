// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Marker rows recording that FM rendezvous has successfully created an alert
//! or support bundle. See the corresponding table comments in
//! `schema/crdb/dbinit.sql` for the tombstone-and-idempotency semantics that
//! motivate these tables.

use crate::Generation;
use crate::typed_uuid::DbTypedUuid;
use nexus_db_schema::schema::rendezvous_alert_created;
use nexus_db_schema::schema::rendezvous_support_bundle_created;
use omicron_uuid_kinds::{AlertKind, AlertUuid};
use omicron_uuid_kinds::{SupportBundleKind, SupportBundleUuid};

#[derive(Queryable, Insertable, Debug, Clone, Selectable, PartialEq)]
#[diesel(table_name = rendezvous_alert_created)]
pub struct RendezvousAlertCreated {
    alert_id: DbTypedUuid<AlertKind>,
    pub created_at_generation: Generation,
}

impl RendezvousAlertCreated {
    pub fn new(alert_id: AlertUuid, generation: Generation) -> Self {
        Self { alert_id: alert_id.into(), created_at_generation: generation }
    }

    pub fn alert_id(&self) -> AlertUuid {
        self.alert_id.into()
    }
}

#[derive(Queryable, Insertable, Debug, Clone, Selectable, PartialEq)]
#[diesel(table_name = rendezvous_support_bundle_created)]
pub struct RendezvousSupportBundleCreated {
    support_bundle_id: DbTypedUuid<SupportBundleKind>,
    pub created_at_generation: Generation,
}

impl RendezvousSupportBundleCreated {
    pub fn new(
        support_bundle_id: SupportBundleUuid,
        generation: Generation,
    ) -> Self {
        Self {
            support_bundle_id: support_bundle_id.into(),
            created_at_generation: generation,
        }
    }

    pub fn support_bundle_id(&self) -> SupportBundleUuid {
        self.support_bundle_id.into()
    }
}
