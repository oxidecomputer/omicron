// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast-specific datastore test helpers.

use std::net::Ipv4Addr;

use uuid::Uuid;

use nexus_db_model::MulticastGroupState;
use nexus_db_model::{
    IncompleteVpc, IpPool, IpPoolResource, IpPoolResourceType, IpVersion,
};
use nexus_types::external_api::params;
use nexus_types::external_api::shared::{IpRange, Ipv4Range};
use nexus_types::identity::Resource;
use omicron_common::api::external::{IdentityMetadataCreateParams, LookupType};
use omicron_uuid_kinds::SledUuid;

use crate::authz;
use crate::context::OpContext;
use crate::db::DataStore;
use crate::db::pub_test_utils::helpers::{SledUpdateBuilder, create_project};

/// Common test setup for multicast datastore tests.
pub struct TestSetup {
    pub authz_project: authz::Project,
    pub project_id: Uuid,
    pub authz_pool: authz::IpPool,
    pub authz_vpc: authz::Vpc,
    pub vpc_id: Uuid,
    pub sled_id: SledUuid,
}

/// Create a standard test setup with database, project, IP pool, and sled.
pub async fn create_test_setup(
    opctx: &OpContext,
    datastore: &DataStore,
    pool_name: &'static str,
    project_name: &'static str,
) -> TestSetup {
    create_test_setup_with_range(
        opctx,
        datastore,
        pool_name,
        project_name,
        (224, 10, 1, 1),
        (224, 10, 1, 254),
    )
    .await
}

/// Create a test setup with a custom IPv4 multicast range for the pool.
pub async fn create_test_setup_with_range(
    opctx: &OpContext,
    datastore: &DataStore,
    pool_name: &'static str,
    project_name: &'static str,
    range_start: (u8, u8, u8, u8),
    range_end: (u8, u8, u8, u8),
) -> TestSetup {
    // Create project using the existing helper
    let (authz_project, project) =
        create_project(opctx, datastore, project_name).await;
    let project_id = project.id();

    // Create VPC for multicast groups
    let vpc_params = params::VpcCreate {
        identity: IdentityMetadataCreateParams {
            name: format!("{}-vpc", project_name).parse().unwrap(),
            description: format!("Test VPC for project {}", project_name),
        },
        ipv6_prefix: None,
        dns_name: format!("{}-vpc", project_name).parse().unwrap(),
    };

    let vpc = IncompleteVpc::new(
        Uuid::new_v4(),
        project_id,
        Uuid::new_v4(), // system_router_id
        vpc_params,
    )
    .expect("Should create incomplete VPC");

    let (authz_vpc, vpc_record) = datastore
        .project_create_vpc(&opctx, &authz_project, vpc)
        .await
        .expect("Should create VPC");
    let vpc_id = vpc_record.id();

    // Create multicast IP pool
    let pool_identity = IdentityMetadataCreateParams {
        name: pool_name.parse().unwrap(),
        description: format!("Test multicast pool: {}", pool_name),
    };

    let ip_pool = datastore
        .ip_pool_create(
            &opctx,
            IpPool::new_multicast(&pool_identity, IpVersion::V4),
        )
        .await
        .expect("Should create multicast IP pool");

    let authz_pool = authz::IpPool::new(
        crate::authz::FLEET,
        ip_pool.id(),
        LookupType::ById(ip_pool.id()),
    );

    // Add range to pool
    let range = IpRange::V4(
        Ipv4Range::new(
            Ipv4Addr::new(
                range_start.0,
                range_start.1,
                range_start.2,
                range_start.3,
            ),
            Ipv4Addr::new(range_end.0, range_end.1, range_end.2, range_end.3),
        )
        .unwrap(),
    );
    datastore
        .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
        .await
        .expect("Should add multicast range to pool");

    // Link pool to silo
    let link = IpPoolResource {
        resource_id: opctx.authn.silo_required().unwrap().id(),
        resource_type: IpPoolResourceType::Silo,
        ip_pool_id: ip_pool.id(),
        is_default: false,
    };
    datastore
        .ip_pool_link_silo(&opctx, link)
        .await
        .expect("Should link multicast pool to silo");

    // Create sled
    let sled_id = SledUuid::new_v4();
    let sled_update = SledUpdateBuilder::new().sled_id(sled_id).build();
    datastore.sled_upsert(sled_update).await.unwrap();

    TestSetup {
        authz_project,
        project_id,
        authz_pool,
        authz_vpc,
        vpc_id,
        sled_id,
    }
}

/// Create a test multicast group with the given parameters.
pub async fn create_test_group(
    opctx: &OpContext,
    datastore: &DataStore,
    setup: &TestSetup,
    group_name: &str,
    multicast_ip: &str,
) -> nexus_db_model::ExternalMulticastGroup {
    create_test_group_with_state(
        opctx,
        datastore,
        setup,
        group_name,
        multicast_ip,
        false,
    )
    .await
}

/// Create a test multicast group, optionally transitioning to "Active" state.
pub async fn create_test_group_with_state(
    opctx: &OpContext,
    datastore: &DataStore,
    setup: &TestSetup,
    group_name: &str,
    multicast_ip: &str,
    make_active: bool,
) -> nexus_db_model::ExternalMulticastGroup {
    let params = params::MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group_name.parse().unwrap(),
            description: format!("Test group: {}", group_name),
        },
        multicast_ip: Some(multicast_ip.parse().unwrap()),
        source_ips: None,
        pool: None,
        vpc: None,
    };

    let group = datastore
        .multicast_group_create(
            &opctx,
            setup.project_id,
            Uuid::new_v4(),
            &params,
            Some(setup.authz_pool.clone()),
            Some(setup.vpc_id), // VPC ID from test setup
        )
        .await
        .expect("Should create multicast group");

    if make_active {
        datastore
            .multicast_group_set_state(
                opctx,
                group.id(),
                MulticastGroupState::Active,
            )
            .await
            .expect("Should transition group to 'Active' state");
    }

    group
}
