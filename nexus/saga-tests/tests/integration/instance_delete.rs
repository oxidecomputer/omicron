use crate::instance_create::verify_clean_slate;
use dropshot::test_util::ClientTestContext;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::{authn::saga::Serialized, context::OpContext, db};
use nexus_saga_interface::create_saga_dag;
use nexus_sagas::{
    sagas::instance_delete::Params, sagas::instance_delete::SagaInstanceDelete,
};
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils_macros::nexus_test;
use nexus_types::{external_api::params, identity::Resource};
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, InstanceCpuCount,
};
use omicron_common::api::internal::shared::SwitchLocation;
use slog::o;
use std::collections::HashSet;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const INSTANCE_NAME: &str = "my-instance";
const PROJECT_NAME: &str = "springfield-squidport";
const DISK_NAME: &str = "my-disk";

async fn create_org_project_and_disk(client: &ClientTestContext) -> Uuid {
    create_default_ip_pool(&client).await;
    let project = create_project(client, PROJECT_NAME).await;
    create_disk(&client, PROJECT_NAME, DISK_NAME).await;
    project.identity.id
}

async fn new_test_params(
    cptestctx: &ControlPlaneTestContext,
    instance_id: Uuid,
) -> Params {
    let opctx = test_opctx(&cptestctx);
    let datastore = cptestctx.server.server_context().nexus.datastore();

    let (.., authz_instance, instance) = LookupPath::new(&opctx, datastore)
        .instance_id(instance_id)
        .fetch()
        .await
        .expect("Failed to lookup instance");
    Params {
        serialized_authn: Serialized::for_opctx(&opctx),
        authz_instance,
        instance,
        boundary_switches: HashSet::from([SwitchLocation::Switch0]),
    }
}

// Helper for creating instance create parameters
fn new_instance_create_params() -> params::InstanceCreate {
    params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: INSTANCE_NAME.parse().unwrap(),
            description: "My instance".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "inst".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: Some(Vec::new()),
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![params::ExternalIpCreate::Ephemeral { pool: None }],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach { name: DISK_NAME.parse().unwrap() },
        )),
        disks: Vec::new(),
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: None,
    }
}

pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
    OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        cptestctx.server.server_context().nexus.datastore().clone(),
    )
}

#[nexus_test(server = omicron_nexus::Server)]
async fn test_saga_basic_usage_succeeds(cptestctx: &ControlPlaneTestContext) {
    DiskTest::new(cptestctx).await;
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    create_org_project_and_disk(&client).await;

    // Build the saga DAG with the provided test parameters and run it.
    let params = new_test_params(
        &cptestctx,
        create_instance(&cptestctx, new_instance_create_params()).await.id(),
    )
    .await;
    nexus
        .sagas()
        .saga_execute2::<SagaInstanceDelete>(params)
        .await
        .expect("Saga should have succeeded");
}

async fn create_instance(
    cptestctx: &ControlPlaneTestContext,
    params: params::InstanceCreate,
) -> db::model::Instance {
    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = test_opctx(&cptestctx);

    let project_selector = params::ProjectSelector {
        project: PROJECT_NAME.to_string().try_into().unwrap(),
    };
    let project_lookup =
        nexus.project_lookup(&opctx, project_selector).unwrap();

    let instance_state = nexus
        .project_create_instance(&opctx, &project_lookup, &params)
        .await
        .unwrap();

    let datastore = cptestctx.server.server_context().nexus.datastore().clone();
    let (.., db_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_state.instance().id())
        .fetch()
        .await
        .expect("test instance should be present in datastore");

    db_instance
}

#[nexus_test(server = omicron_nexus::Server)]
async fn test_actions_succeed_idempotently(
    cptestctx: &ControlPlaneTestContext,
) {
    DiskTest::new(cptestctx).await;

    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    create_org_project_and_disk(&client).await;

    // Build the saga DAG with the provided test parameters
    let dag = create_saga_dag::<SagaInstanceDelete>(
        new_test_params(
            &cptestctx,
            create_instance(&cptestctx, new_instance_create_params())
                .await
                .id(),
        )
        .await,
    )
    .unwrap();

    nexus_saga_tests::actions_succeed_idempotently(nexus, dag).await;

    verify_clean_slate(&cptestctx).await;
}
