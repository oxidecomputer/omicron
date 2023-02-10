use std::sync::Arc;

use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup;
use crate::db::lookup::LookupPath;
use db::model::Name;
use nexus_types::external_api::params;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use ref_cast::RefCast;

use super::sagas;

impl super::Nexus {
    // Snapshots

    pub fn snapshot_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        snapshot_selector: &'a params::SnapshotSelector,
    ) -> LookupResult<lookup::Snapshot<'a>> {
        match snapshot_selector {
            params::SnapshotSelector {
                snapshot: NameOrId::Id(id),
                project_selector: None,
            } => {
                let snapshot =
                    LookupPath::new(opctx, &self.db_datastore).snapshot_id(*id);
                Ok(snapshot)
            }
            params::SnapshotSelector {
                snapshot: NameOrId::Name(name),
                project_selector: Some(project_selector),
            } => {
                let snapshot = self
                    .project_lookup(opctx, project_selector)?
                    .snapshot_name(Name::ref_cast(name));
                Ok(snapshot)
            }
            params::SnapshotSelector {
                snapshot: NameOrId::Id(_),
                project_selector: Some(_),
            } => Err(Error::invalid_request(
              "when providing snpashot as an ID, prject should not be specified"
            )),
            _ => Err(Error::invalid_request(
              "snapshot should either be a UUID or project should be specified"
            ))
        }
    }

    pub async fn snapshot_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: &params::SnapshotCreate,
    ) -> CreateResult<db::model::Snapshot> {
        let authz_silo: authz::Silo;
        let _authz_org: authz::Organization;
        let authz_project: authz::Project;
        let authz_disk: authz::Disk;
        let db_disk: db::model::Disk;

        // FIXME: Borrowing error here with project_lookup due some issue with disk_name?
        (authz_silo, _authz_org, authz_project, authz_disk, db_disk) =
            project_lookup
                .disk_name(Name::ref_cast(&params.disk.clone()))
                .fetch_for(authz::Action::Read)
                .await?;

        // If there isn't a running propolis, Nexus needs to use the Crucible
        // Pantry to make this snapshot
        let use_the_pantry = if let Some(attach_instance_id) =
            &db_disk.runtime_state.attach_instance_id
        {
            let (.., db_instance) = LookupPath::new(opctx, &self.db_datastore)
                .instance_id(*attach_instance_id)
                .fetch_for(authz::Action::Read)
                .await?;

            let instance_state: InstanceState = db_instance.runtime().state.0;

            match instance_state {
                // If there's a propolis running, use that
                InstanceState::Running |
                // Rebooting doesn't deactivate the volume
                InstanceState::Rebooting
                => false,

                // If there's definitely no propolis running, then use the
                // pantry
                InstanceState::Stopped | InstanceState::Destroyed => true,

                // If there *may* be a propolis running, then fail: we can't
                // know if that propolis has activated the Volume or not, or if
                // it's in the process of deactivating.
                _ => {
                    return Err(
                        Error::invalid_request(
                            &format!("cannot snapshot attached disk for instance in state {}", instance_state)
                        )
                    );
                }
            }
        } else {
            // This disk is not attached to an instance, use the pantry.
            true
        };

        let saga_params = sagas::snapshot_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            silo_id: authz_silo.id(),
            project_id: authz_project.id(),
            disk_id: authz_disk.id(),
            use_the_pantry,
            create_params: params.clone(),
        };

        let saga_outputs = self
            .execute_saga::<sagas::snapshot_create::SagaSnapshotCreate>(
                saga_params,
            )
            .await?;

        let snapshot_created = saga_outputs
            .lookup_node_output::<db::model::Snapshot>("finalized_snapshot")
            .map_err(|e| Error::InternalError {
                internal_message: e.to_string(),
            })?;

        Ok(snapshot_created)
    }

    pub async fn snapshot_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Snapshot> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;

        self.db_datastore.snapshot_list(opctx, &authz_project, pagparams).await
    }

    pub async fn snapshot_delete(
        self: &Arc<Self>,
        opctx: &OpContext,
        snapshot_lookup: &lookup::Snapshot<'_>,
    ) -> DeleteResult {
        // TODO-correctness
        // This also requires solving how to clean up the associated resources
        // (on-disk snapshots, running read-only downstairs) because disks
        // *could* still be using them (if the snapshot has not yet been turned
        // into a regular crucible volume). It will involve some sort of
        // reference counting for volumes.

        let (.., authz_snapshot, db_snapshot) =
            snapshot_lookup.fetch_for(authz::Action::Delete).await?;

        let saga_params = sagas::snapshot_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            authz_snapshot,
            snapshot: db_snapshot,
        };
        self.execute_saga::<sagas::snapshot_delete::SagaSnapshotDelete>(
            saga_params,
        )
        .await?;
        Ok(())
    }
}
