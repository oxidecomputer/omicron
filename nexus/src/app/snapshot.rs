use crate::app::disk::Disk;
use crate::app::project::Project;
use crate::app::saga;
use crate::app::sagas;
use crate::app::SagaContext;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_types::external_api::params;
use nexus_types::external_api::params::DiskSelector;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use std::sync::Arc;

/// Application level operations on Snapshots
pub struct Snapshot {
    datastore: Arc<db::DataStore>,
    sec_client: Arc<saga::SecClient>,
    project: Project,
    disk: Disk,
}

impl Snapshot {
    pub fn new(
        datastore: Arc<db::DataStore>,
        sec_client: Arc<saga::SecClient>,
        project: Project,
        disk: Disk,
    ) -> Snapshot {
        Snapshot { datastore, sec_client, project, disk }
    }

    pub fn lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        snapshot_selector: params::SnapshotSelector,
    ) -> LookupResult<lookup::Snapshot<'a>> {
        match snapshot_selector {
            params::SnapshotSelector {
                snapshot: NameOrId::Id(id),
                project: None,
            } => {
                let snapshot =
                    LookupPath::new(opctx, &self.datastore).snapshot_id(id);
                Ok(snapshot)
            }
            params::SnapshotSelector {
                snapshot: NameOrId::Name(name),
                project: Some(project),
            } => {
                let snapshot = self.project
                    .lookup(opctx, params::ProjectSelector { project })?
                    .snapshot_name_owned(name.into());
                Ok(snapshot)
            }
            params::SnapshotSelector {
                snapshot: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
              "when providing snpashot as an ID, prject should not be specified"
            )),
            _ => Err(Error::invalid_request(
              "snapshot should either be an ID or project should be specified"
            ))
        }
    }

    pub(crate) async fn create(
        &self,
        opctx: &OpContext,
        saga_context: &SagaContext,
        // Is passed by value due to `disk_name` taking ownership of `self` below
        project_lookup: lookup::Project<'_>,
        params: &params::SnapshotCreate,
    ) -> CreateResult<db::model::Snapshot> {
        let authz_silo: authz::Silo;
        let authz_disk_project: authz::Project;
        let authz_disk: authz::Disk;
        let db_disk: db::model::Disk;

        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        (authz_silo, authz_disk_project, authz_disk, db_disk) = match params
            .disk
            .clone()
        {
            NameOrId::Id(id) => self.disk.lookup(
                opctx,
                DiskSelector { disk: NameOrId::Id(id), project: None },
            )?,
            NameOrId::Name(name) => project_lookup.disk_name_owned(name.into()),
        }
        .fetch_for(authz::Action::Read)
        .await?;

        if authz_disk_project.id() != authz_project.id() {
            return Err(Error::invalid_request(
                "can't create a snapshot of a disk in a different project",
            ));
        }

        // If there isn't a running propolis, Nexus needs to use the Crucible
        // Pantry to make this snapshot
        let use_the_pantry = if let Some(attach_instance_id) =
            &db_disk.runtime_state.attach_instance_id
        {
            let (.., authz_instance) = LookupPath::new(opctx, &self.datastore)
                .instance_id(*attach_instance_id)
                .lookup_for(authz::Action::Read)
                .await?;

            let instance_state = self
                .datastore
                .instance_fetch_with_vmm(&opctx, &authz_instance)
                .await?;

            // If a Propolis _may_ exist, send the snapshot request there,
            // otherwise use the pantry.
            !instance_state.vmm().is_some()
        } else {
            // This disk is not attached to an instance, use the pantry.
            true
        };

        let saga_params = sagas::snapshot_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            silo_id: authz_silo.id(),
            project_id: authz_project.id(),
            disk_id: authz_disk.id(),
            attach_instance_id: db_disk.runtime_state.attach_instance_id,
            use_the_pantry,
            create_params: params.clone(),
        };

        let saga_outputs = self
            .sec_client
            .execute_saga::<sagas::snapshot_create::SagaSnapshotCreate>(
                saga_params,
                saga_context.clone(),
            )
            .await?;

        let snapshot_created = saga_outputs
            .lookup_node_output::<db::model::Snapshot>("finalized_snapshot")
            .map_err(|e| Error::InternalError {
                internal_message: e.to_string(),
            })?;

        Ok(snapshot_created)
    }

    pub(crate) async fn list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Snapshot> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;

        self.datastore.snapshot_list(opctx, &authz_project, pagparams).await
    }

    pub(crate) async fn delete(
        &self,
        opctx: &OpContext,
        saga_context: &SagaContext,
        snapshot_lookup: &lookup::Snapshot<'_>,
    ) -> DeleteResult {
        let (.., authz_snapshot, db_snapshot) =
            snapshot_lookup.fetch_for(authz::Action::Delete).await?;

        let saga_params = sagas::snapshot_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            authz_snapshot,
            snapshot: db_snapshot,
        };

        self.sec_client
            .execute_saga::<sagas::snapshot_delete::SagaSnapshotDelete>(
                saga_params,
                saga_context.clone(),
            )
            .await?;

        Ok(())
    }
}
