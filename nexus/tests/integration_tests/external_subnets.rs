// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for External Subnets API endpoints.

use crate::integration_tests::instances::instance_simulate;
use crate::integration_tests::instances::instance_wait_for_state;
use dropshot::ResultsPage;
use dropshot::test_util::ClientTestContext;
use http::Method;
use http::StatusCode;
use nexus_db_queries::db::queries::external_subnet::MAX_ATTACHED_SUBNETS_PER_INSTANCE;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_default_ip_pools;
use nexus_test_utils::resource_helpers::create_default_subnet_pool;
use nexus_test_utils::resource_helpers::create_external_subnet_in_pool;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_instance_with;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_subnet_pool;
use nexus_test_utils::resource_helpers::create_subnet_pool_member;
use nexus_test_utils::resource_helpers::object_create_error;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::external_subnet as external_subnet_types;
use nexus_types::external_api::external_subnet::ExternalSubnet;
use nexus_types::external_api::ip_pool;
use omicron_common::address::IpVersion;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_uuid_kinds::GenericUuid as _;
use omicron_uuid_kinds::InstanceUuid;
use oxnet::IpNet;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "test-project";
const SECOND_PROJECT_NAME: &str = "test-project2";
const SUBNET_POOL_NAME: &str = "geo";
const EXTERNAL_SUBNET_NAME: &str = "schist";
const SECOND_EXTERNAL_SUBNET_NAME: &str = "shale";
const INSTANCE_NAME: &str = "granite";
const SECOND_INSTANCE_NAME: &str = "hematite";

// Note: These tests verify that stub endpoints return 500 Internal Server Error.
// The detailed "endpoint is not implemented" message is intentionally not exposed
// to clients for security reasons (internal messages are logged server-side only).

fn external_subnets_url(project: &str) -> String {
    format!("/v1/external-subnets?project={}", project)
}

fn external_subnet_url(name: &str, project: &str) -> String {
    format!("/v1/external-subnets/{}?project={}", name, project)
}

fn external_subnet_attach_url(name: &str, project: &str) -> String {
    format!("/v1/external-subnets/{}/attach?project={}", name, project)
}

fn external_subnet_detach_url(name: &str, project: &str) -> String {
    format!("/v1/external-subnets/{}/detach?project={}", name, project)
}

fn instance_external_subnets_url(instance: &str, project: &str) -> String {
    format!("/v1/instances/{}/external-subnets?project={}", instance, project)
}

#[nexus_test]
async fn external_subnet_basic_crud(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a pool, member, and project first
    let _pool =
        create_default_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4)
            .await;
    let member_subnet = "8.8.8.0/24".parse().unwrap();
    let ip_subnet: IpNet = "8.8.8.0/28".parse().unwrap();
    let _member =
        create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet)
            .await;
    let _project = create_project(client, PROJECT_NAME).await;

    // Sanity check, can we CRUD a single subnet.
    let create_params = external_subnet_types::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
            prefix_len: 28,
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
            },
        },
    };
    let external_subnet = NexusRequest::objects_post(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert_eq!(external_subnet.subnet, ip_subnet);
    assert_eq!(external_subnet.identity.name.as_str(), EXTERNAL_SUBNET_NAME);
    let time_created = external_subnet.identity.time_created;
    assert_eq!(time_created, external_subnet.identity.time_modified);

    // Same thing if we read it.
    let via_read = NexusRequest::object_get(
        client,
        &external_subnet_url(EXTERNAL_SUBNET_NAME, PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert_eq!(external_subnet, via_read);

    // Or list it.
    let via_list =
        NexusRequest::object_get(client, &external_subnets_url(PROJECT_NAME))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<ResultsPage<ExternalSubnet>>()
            .await
            .items;
    assert_eq!(via_list.len(), 1);
    assert_eq!(via_list[0], via_read);

    // Update the metadata
    let new_name = "quartzite".parse::<Name>().unwrap();
    let updates = external_subnet_types::ExternalSubnetUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some(new_name.clone()),
            description: None,
        },
    };
    let updated = NexusRequest::object_put(
        client,
        &external_subnet_url(EXTERNAL_SUBNET_NAME, PROJECT_NAME),
        Some(&updates),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert_eq!(updated.identity.name, new_name);
    assert_eq!(
        updated.identity.time_created,
        external_subnet.identity.time_created
    );
    assert!(
        updated.identity.time_modified > external_subnet.identity.time_modified
    );

    // Now delete.
    NexusRequest::object_delete(
        client,
        &external_subnet_url(new_name.as_str(), PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // And it should really be gone.
    let via_list =
        NexusRequest::object_get(client, &external_subnets_url(PROJECT_NAME))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<ResultsPage<ExternalSubnet>>()
            .await
            .items;
    assert!(via_list.is_empty());
}

#[nexus_test]
async fn external_subnet_pagination(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a pool, member, and project first
    let _pool =
        create_default_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4)
            .await;
    let member_subnet = "8.8.8.0/24".parse().unwrap();
    let _member =
        create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet)
            .await;
    let _project = create_project(client, PROJECT_NAME).await;

    // Create several subnets.
    let n_subnets = 10;
    let mut subnets = Vec::with_capacity(n_subnets);
    for i in 0..n_subnets {
        let create_params = external_subnet_types::ExternalSubnetCreate {
            identity: IdentityMetadataCreateParams {
                name: format!("{EXTERNAL_SUBNET_NAME}-{i}").parse().unwrap(),
                description: String::from("A test external subnet"),
            },
            allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
                prefix_len: 28,
                pool_selector: ip_pool::PoolSelector::Explicit {
                    pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
                },
            },
        };
        let external_subnet = NexusRequest::objects_post(
            client,
            &external_subnets_url(PROJECT_NAME),
            &create_params,
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ExternalSubnet>()
        .await;
        assert!(member_subnet.is_supernet_of(&external_subnet.subnet));
        subnets.push(external_subnet);
    }

    // We're using name url, so sort everything by that.
    subnets.sort_by(|a, b| a.identity.name.cmp(&b.identity.name));

    // List them in several pages, and ensure we get the same thing.
    let n_pages = 2;
    let page_size = n_subnets / n_pages;
    let mut as_pages: Vec<ExternalSubnet> = Vec::with_capacity(n_subnets);
    let mut page_token = None;
    for _ in 0..n_pages {
        let page_url = if let Some(token) = &page_token {
            format!(
                "{}&limit={}&page_token={}",
                external_subnets_url(PROJECT_NAME),
                page_size,
                token,
            )
        } else {
            format!(
                "{}&limit={}",
                external_subnets_url(PROJECT_NAME),
                page_size
            )
        };
        let ResultsPage { mut items, next_page } =
            objects_list_page_authz(client, &page_url).await;
        assert_eq!(items.len(), page_size);
        as_pages.append(&mut items);
        page_token = next_page;
    }

    // We should get the same thing, in the same order.
    assert_eq!(subnets, as_pages);

    // And there should be nothing left
    let page_url = format!(
        "{}&limit={}&page_token={}",
        external_subnets_url(PROJECT_NAME),
        page_size,
        page_token.unwrap(),
    );
    let ResultsPage { items, next_page } =
        objects_list_page_authz::<ExternalSubnet>(client, &page_url).await;
    assert!(items.is_empty());
    assert!(next_page.is_none());
}

#[nexus_test]
async fn test_external_subnet_attach(cptestctx: &ControlPlaneTestContext) {
    attach_test_impl(cptestctx, InstanceState::Running).await
}

#[nexus_test]
async fn test_can_attach_external_subnet_to_stopped_instance(
    cptestctx: &ControlPlaneTestContext,
) {
    attach_test_impl(cptestctx, InstanceState::Stopped).await
}

async fn attach_test_impl(
    cptestctx: &ControlPlaneTestContext,
    instance_state: InstanceState,
) {
    let client = &cptestctx.external_client;

    // Create a pool, member, IP Pool, range, and project first
    let _pool =
        create_default_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4)
            .await;
    let member_subnet = "8.8.8.0/24".parse().unwrap();
    let _member =
        create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet)
            .await;
    let (_v4_pool, _v6_pool) = create_default_ip_pools(client).await;
    let _ = create_project(client, PROJECT_NAME).await;

    // Then create a subnet in the pool.
    let create_params = external_subnet_types::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
            prefix_len: 28,
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
            },
        },
    };
    let external_subnet = NexusRequest::objects_post(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert!(member_subnet.is_supernet_of(&external_subnet.subnet));

    // Create an instance and wait for it to go to the desired state.
    let nexus = &cptestctx.server.server_context().nexus;
    let instance = match instance_state {
        InstanceState::Running => {
            let instance =
                create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;
            let instance_id =
                InstanceUuid::from_untyped_uuid(instance.identity.id);
            instance_simulate(nexus, &instance_id).await;
            instance_wait_for_state(
                client,
                instance_id,
                InstanceState::Running,
            )
            .await;
            instance
        }
        InstanceState::Stopped => {
            create_instance_with(
                client,
                PROJECT_NAME,
                INSTANCE_NAME,
                &Default::default(),
                vec![],
                vec![],
                /* start = */ false,
                None,
                None,
                vec![],
            )
            .await
        }
        _ => panic!("this test should only use a stopped or running instance"),
    };

    // Now attach the subnet to it.
    let updated =
        attach_external_subnet(client, INSTANCE_NAME, EXTERNAL_SUBNET_NAME)
            .await;

    assert_eq!(
        updated
            .instance_id
            .expect("Should have the newly-attached instance ID"),
        instance.identity.id
    );
    assert_eq!(updated.identity.id, external_subnet.identity.id);

    // We should see this subnet in the list of those attached to the instance
    // now.
    let subnets = objects_list_page_authz::<ExternalSubnet>(
        client,
        &instance_external_subnets_url(INSTANCE_NAME, PROJECT_NAME),
    )
    .await
    .items;
    assert_eq!(subnets.len(), 1);
    assert_eq!(subnets[0].identity.id, updated.identity.id);
    assert_eq!(subnets[0].instance_id, updated.instance_id);
    assert_eq!(subnets[0].subnet, updated.subnet);

    // Attaching a second time is also fine.
    let _ = attach_external_subnet(client, INSTANCE_NAME, EXTERNAL_SUBNET_NAME)
        .await;

    // Now let's delete it and make sure we don't see it anymore.
    let detached = detach_external_subnet(client, EXTERNAL_SUBNET_NAME).await;
    assert!(detached.instance_id.is_none());
    assert!(
        objects_list_page_authz::<ExternalSubnet>(
            client,
            &instance_external_subnets_url(INSTANCE_NAME, PROJECT_NAME),
        )
        .await
        .items
        .is_empty(),
    );
}

#[nexus_test]
async fn cannot_attach_subnet_in_another_project(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a pool, member, IP Pool, range, and project first
    let _pool =
        create_default_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4)
            .await;
    let member_subnet = "8.8.8.0/24".parse().unwrap();
    let _member =
        create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet)
            .await;
    let (_v4_pool, _v6_pool) = create_default_ip_pools(client).await;
    let _ = create_project(client, PROJECT_NAME).await;

    // Then create a subnet in the pool.
    let create_params = external_subnet_types::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
            prefix_len: 28,
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
            },
        },
    };
    let external_subnet = NexusRequest::objects_post(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert!(member_subnet.is_supernet_of(&external_subnet.subnet));

    // Create the second project
    let _ = create_project(client, SECOND_PROJECT_NAME).await;

    // Create an instance and wait for it to go to the running state.
    let instance =
        create_instance(client, SECOND_PROJECT_NAME, INSTANCE_NAME).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // We should not be able to attach the subnet to it.
    let params = external_subnet_types::ExternalSubnetAttach {
        instance: NameOrId::Id(instance.identity.id),
    };
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        &external_subnet_attach_url(EXTERNAL_SUBNET_NAME, PROJECT_NAME),
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn cannot_attach_subnet_attached_to_another_instance(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a pool, member, IP Pool, range, and project first
    let _pool =
        create_default_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4)
            .await;
    let member_subnet = "8.8.8.0/24".parse().unwrap();
    let _member =
        create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet)
            .await;
    let (_v4_pool, _v6_pool) = create_default_ip_pools(client).await;
    let _ = create_project(client, PROJECT_NAME).await;

    // Then create a subnet in the pool.
    let create_params = external_subnet_types::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
            prefix_len: 28,
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
            },
        },
    };
    let external_subnet = NexusRequest::objects_post(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert!(member_subnet.is_supernet_of(&external_subnet.subnet));

    // Create an instance and wait for it to go to the running state.
    let instance = create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // Now attach the subnet to it.
    let updated =
        attach_external_subnet(client, INSTANCE_NAME, EXTERNAL_SUBNET_NAME)
            .await;

    assert_eq!(
        updated
            .instance_id
            .expect("Should have the newly-attached instance ID"),
        instance.identity.id
    );
    assert_eq!(updated.identity.id, external_subnet.identity.id);

    // Now create a second instance.
    let instance2 =
        create_instance(client, PROJECT_NAME, SECOND_INSTANCE_NAME).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance2.identity.id);
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // We should not be able to attach the subnet to it.
    let params = external_subnet_types::ExternalSubnetAttach {
        instance: NameOrId::Id(instance2.identity.id),
    };
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        &external_subnet_attach_url(EXTERNAL_SUBNET_NAME, PROJECT_NAME),
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn cannot_detach_subnet_that_is_not_attached(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a pool, member, IP Pool, range, and project first
    let _pool =
        create_default_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4)
            .await;
    let member_subnet = "8.8.8.0/24".parse().unwrap();
    let _member =
        create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet)
            .await;
    let (_v4_pool, _v6_pool) = create_default_ip_pools(client).await;
    let _ = create_project(client, PROJECT_NAME).await;

    // Then create a subnet in the pool.
    let create_params = external_subnet_types::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
            prefix_len: 28,
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
            },
        },
    };
    let external_subnet = NexusRequest::objects_post(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert!(member_subnet.is_supernet_of(&external_subnet.subnet));

    // Create an instance and wait for it to go to the running state.
    let instance = create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // But do NOT attach anything. Instead, just try to detach it directly,
    // which should fail.
    NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        &external_subnet_detach_url(EXTERNAL_SUBNET_NAME, PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn cannot_attach_too_many_subnets(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a pool, member, IP Pool, range, and project first
    let _pool =
        create_default_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4)
            .await;
    let member_subnet = "8.8.8.0/24".parse().unwrap();
    let _member =
        create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet)
            .await;
    let (_v4_pool, _v6_pool) = create_default_ip_pools(client).await;
    let _ = create_project(client, PROJECT_NAME).await;

    // Create an instance and wait for it to go to the running state.
    let instance = create_instance(client, PROJECT_NAME, INSTANCE_NAME).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // Create and attach a bunch of subnets.
    let mut subnets = Vec::with_capacity(
        usize::try_from(MAX_ATTACHED_SUBNETS_PER_INSTANCE).unwrap(),
    );
    for i in 0..MAX_ATTACHED_SUBNETS_PER_INSTANCE {
        let name = format!("{EXTERNAL_SUBNET_NAME}-{i}");
        let create_params = external_subnet_types::ExternalSubnetCreate {
            identity: IdentityMetadataCreateParams {
                name: name.parse().unwrap(),
                description: String::from("A test external subnet"),
            },
            allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
                prefix_len: 30,
                pool_selector: ip_pool::PoolSelector::Explicit {
                    pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
                },
            },
        };
        let external_subnet = NexusRequest::objects_post(
            client,
            &external_subnets_url(PROJECT_NAME),
            &create_params,
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ExternalSubnet>()
        .await;
        assert!(member_subnet.is_supernet_of(&external_subnet.subnet));
        subnets.push(external_subnet);
        let _ = attach_external_subnet(client, INSTANCE_NAME, &name).await;
    }

    // Create one more external subnet.
    let create_params = external_subnet_types::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: SECOND_EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
            prefix_len: 30,
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
            },
        },
    };
    let external_subnet = NexusRequest::objects_post(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert!(member_subnet.is_supernet_of(&external_subnet.subnet));

    // Trying to attach it to the instance should fail.
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        &external_subnet_attach_url(SECOND_EXTERNAL_SUBNET_NAME, PROJECT_NAME),
        &external_subnet_types::ExternalSubnetAttach {
            instance: INSTANCE_NAME.parse::<Name>().unwrap().into(),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn cannot_delete_attached_external_subnet(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a pool, member, IP Pool, range, and project first
    let _pool =
        create_default_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4)
            .await;
    let member_subnet = "8.8.8.0/24".parse().unwrap();
    let _member =
        create_subnet_pool_member(client, SUBNET_POOL_NAME, member_subnet)
            .await;
    let (_v4_pool, _v6_pool) = create_default_ip_pools(client).await;
    let _ = create_project(client, PROJECT_NAME).await;

    // Then create a subnet in the pool.
    let create_params = external_subnet_types::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
            prefix_len: 28,
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
            },
        },
    };
    let external_subnet = NexusRequest::objects_post(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ExternalSubnet>()
    .await;
    assert!(member_subnet.is_supernet_of(&external_subnet.subnet));

    // Create a stopped instance, and attach the subnet.
    let _ = create_instance_with(
        client,
        PROJECT_NAME,
        INSTANCE_NAME,
        &Default::default(),
        vec![],
        vec![],
        /* start = */ false,
        None,
        None,
        vec![],
    )
    .await;
    let _ = attach_external_subnet(client, INSTANCE_NAME, EXTERNAL_SUBNET_NAME)
        .await;

    // We shouldn't be able to delete the subnet yet.
    NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &external_subnet_url(EXTERNAL_SUBNET_NAME, PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

// We use a custom request builder for this, because we return 202 Accepted
// instead of 201 Created.
async fn attach_external_subnet(
    client: &ClientTestContext,
    instance_name: &str,
    subnet_name: &str,
) -> ExternalSubnet {
    let url = external_subnet_attach_url(subnet_name, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&external_subnet_types::ExternalSubnetAttach {
                instance: instance_name.parse::<Name>().unwrap().into(),
            }))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

async fn detach_external_subnet(
    client: &ClientTestContext,
    subnet_name: &str,
) -> ExternalSubnet {
    let url = external_subnet_detach_url(subnet_name, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

// https://github.com/oxidecomputer/omicron/issues/9873
#[nexus_test]
async fn external_subnet_create_name_conflict(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _pool =
        create_default_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4)
            .await;
    create_subnet_pool_member(
        client,
        SUBNET_POOL_NAME,
        "8.8.8.0/24".parse().unwrap(),
    )
    .await;
    create_project(client, PROJECT_NAME).await;

    // First create succeeds.
    create_external_subnet_in_pool(
        client,
        SUBNET_POOL_NAME,
        PROJECT_NAME,
        EXTERNAL_SUBNET_NAME,
        28,
    )
    .await;

    // Second create with the same name should return ObjectAlreadyExists,
    // not 500.
    let create_params = external_subnet_types::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
            prefix_len: 28,
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
            },
        },
    };
    let error = object_create_error(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(error.error_code.as_deref(), Some("ObjectAlreadyExists"));
    assert!(
        error.message.contains(EXTERNAL_SUBNET_NAME),
        "error message should contain the subnet name: {}",
        error.message,
    );
}

// https://github.com/oxidecomputer/omicron/issues/9872
#[nexus_test]
async fn external_subnet_create_nonexistent_pool(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project(client, PROJECT_NAME).await;

    // Try to create a subnet from a pool that doesn't exist.
    let create_params = external_subnet_types::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
            prefix_len: 28,
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name("no-such-pool".parse().unwrap()),
            },
        },
    };
    let error = object_create_error(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(error.error_code.as_deref(), Some("ObjectNotFound"));
}

// Verify that creating a subnet from a pool that exists but is not linked to
// the current silo fails. This covers the guard CTE
// (`ensure_silo_is_linked_to_pool`) that checks linking — if the CTE were
// elided (e.g., if MATERIALIZED were removed and the optimizer skipped it),
// this test would fail because the create would succeed.
#[nexus_test]
async fn external_subnet_create_unlinked_pool(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a pool and member but do NOT link the pool to the silo.
    let _pool =
        create_subnet_pool(client, SUBNET_POOL_NAME, IpVersion::V4).await;
    create_subnet_pool_member(
        client,
        SUBNET_POOL_NAME,
        "8.8.8.0/24".parse().unwrap(),
    )
    .await;
    create_project(client, PROJECT_NAME).await;

    let create_params = external_subnet_types::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: EXTERNAL_SUBNET_NAME.parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        allocator: external_subnet_types::ExternalSubnetAllocator::Auto {
            prefix_len: 28,
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name(SUBNET_POOL_NAME.parse().unwrap()),
            },
        },
    };
    let error = object_create_error(
        client,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(error.error_code.as_deref(), Some("ObjectNotFound"));
}
