// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{siu_lock_instance, siu_unlock_instance, NexusActionContext};
use crate::app::instance::InstanceStateChangeError;
use crate::app::sagas::declare_saga_actions;
use chrono::Utc;
use nexus_db_model::Generation;
use nexus_db_queries::db::{
    datastore::InstanceAndVmms, identity::Resource, lookup::LookupPath,
};
use nexus_db_queries::{authn, authz, db};
use omicron_common::api::external::{Error, InstanceState};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

declare_saga_actions! {
    instance_update_destroyed;

    // Read the target Instance from CRDB and join with its active VMM and
    // migration target VMM records if they exist, and then acquire the
    // "instance updater" lock with this saga's ID if no other saga is currently
    // updating the instance.
    LOCK_INSTANCE -> "instance_and_vmms" {
        + siu_lock_instance
        - siu_unlock_instance
    }

    DELETE_SLED_RESOURCE -> "no_result1" {
        + siud_delete_sled_resource
    }

    DELETE_VIRTUAL_PROVISIONING -> "no_result2" {
        + siud_delete_virtual_provisioning
    }
}

async fn siud_delete_sled_resource(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    todo!()
}

async fn siud_delete_virtual_provisioning(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    todo!()
}
