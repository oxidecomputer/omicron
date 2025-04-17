use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_queries::db::DataStore;
use omicron_sled_agent::sim::SledAgent;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;
const DISK_NAME: &str = "my-disk";

pub(crate) async fn verify_clean_slate(cptestctx: &ControlPlaneTestContext) {
    let sled_agent = cptestctx.first_sled_agent();
    let datastore = cptestctx.server.server_context().nexus.datastore();

    // Check that no partial artifacts of instance creation exist
    assert!(no_instance_records_exist(datastore).await);
    assert!(no_network_interface_records_exist(datastore).await);
    assert!(no_external_ip_records_exist(datastore).await);
    assert!(
        nexus_saga_tests::no_sled_resource_vmm_records_exist(cptestctx).await
    );
    assert!(
        nexus_saga_tests::no_virtual_provisioning_resource_records_exist(
            cptestctx
        )
        .await
    );
    assert!(
        nexus_saga_tests::no_virtual_provisioning_collection_records_using_instances(
            cptestctx
        )
        .await
    );
    assert!(disk_is_detached(datastore).await);
    assert!(no_instances_or_disks_on_sled(&sled_agent).await);

    let v2p_mappings = &*sled_agent.v2p_mappings.lock().unwrap();
    assert!(v2p_mappings.is_empty());
}

async fn no_instance_records_exist(datastore: &DataStore) -> bool {
    use nexus_db_queries::db::model::Instance;
    use nexus_db_schema::schema::instance::dsl;

    dsl::instance
        .filter(dsl::time_deleted.is_null())
        .select(Instance::as_select())
        .first_async::<Instance>(
            &*datastore.pool_connection_for_tests().await.unwrap(),
        )
        .await
        .optional()
        .unwrap()
        .is_none()
}

async fn no_network_interface_records_exist(datastore: &DataStore) -> bool {
    use nexus_db_queries::db::model::NetworkInterface;
    use nexus_db_queries::db::model::NetworkInterfaceKind;
    use nexus_db_schema::schema::network_interface::dsl;

    dsl::network_interface
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::kind.eq(NetworkInterfaceKind::Instance))
        .select(NetworkInterface::as_select())
        .first_async::<NetworkInterface>(
            &*datastore.pool_connection_for_tests().await.unwrap(),
        )
        .await
        .optional()
        .unwrap()
        .is_none()
}

async fn no_external_ip_records_exist(datastore: &DataStore) -> bool {
    use nexus_db_queries::db::model::ExternalIp;
    use nexus_db_schema::schema::external_ip::dsl;

    dsl::external_ip
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::is_service.eq(false))
        .select(ExternalIp::as_select())
        .first_async::<ExternalIp>(
            &*datastore.pool_connection_for_tests().await.unwrap(),
        )
        .await
        .optional()
        .unwrap()
        .is_none()
}

async fn disk_is_detached(datastore: &DataStore) -> bool {
    use nexus_db_queries::db::model::Disk;
    use nexus_db_schema::schema::disk::dsl;

    dsl::disk
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::name.eq(DISK_NAME))
        .select(Disk::as_select())
        .first_async::<Disk>(
            &*datastore.pool_connection_for_tests().await.unwrap(),
        )
        .await
        .unwrap()
        .runtime_state
        .disk_state
        == "detached"
}

async fn no_instances_or_disks_on_sled(sled_agent: &SledAgent) -> bool {
    sled_agent.vmm_count().await == 0 && sled_agent.disk_count().await == 0
}
