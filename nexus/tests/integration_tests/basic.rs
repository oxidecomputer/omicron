// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Smoke tests against the API server
//!
//! This file defines a very basic set of tests against the API.
//! TODO-coverage add test for racks, sleds

use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use http::method::Method;
use http::StatusCode;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::project_get;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Name;
use omicron_nexus::external_api::{
    params,
    views::{Project, Sled},
};
use serde::Serialize;
use uuid::Uuid;

use nexus_test_interface::NexusServer;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_organization;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::start_sled_agent;
use nexus_test_utils_macros::nexus_test;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_basic_failures(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;

    struct TestCase<'a> {
        method: http::Method,
        uri: &'a str,
        expected_code: http::StatusCode,
        expected_error: &'a str,
        body: Option<String>,
    }

    let test_cases = vec![
        // Error case: GET /nonexistent (a path with no route at all)(
        TestCase {
            method: Method::GET,
            uri: "/nonexistent",
            expected_code: StatusCode::NOT_FOUND,
            expected_error: "Not Found",
            body: None,
        },

        // Error case: GET /organizations/test-org/projects/nonexistent (a
        // possible value that does not exist inside a collection that does
        // exist) from an authorized user results in a 404.
        TestCase {
            method: Method::GET,
            uri: "/organizations/test-org/projects/nonexistent",
            expected_code: StatusCode::NOT_FOUND,
            expected_error: "not found: project with name \"nonexistent\"",
            body: None,
        },
        // Error case: GET /organizations/test-org/projects/-invalid-name
        // TODO-correctness is 400 the right error code here or is 404 more
        // appropriate?
        TestCase {
            method: Method::GET,
            uri: "/organizations/test-org/projects/-invalid-name",
            expected_code: StatusCode::BAD_REQUEST,
            expected_error: "bad parameter in URL path: name must begin with \
            an ASCII lowercase character",
            body: None,
        },
        // Error case: PUT /organizations/test-org/projects
        TestCase {
            method: Method::PUT,
            uri: "/organizations/test-org/projects",
            expected_code: StatusCode::METHOD_NOT_ALLOWED,
            expected_error: "Method Not Allowed",
            body: None,
        },
        // Error case: DELETE /organizations/test-org/projects
        TestCase {
            method: Method::DELETE,
            uri: "/organizations/test-org/projects",
            expected_code: StatusCode::METHOD_NOT_ALLOWED,
            expected_error: "Method Not Allowed",
            body: None,
        },
        // Error case: list instances in a nonexistent project
        TestCase {
            method: Method::GET,
            uri: "/organizations/test-org/projects/nonexistent/instances",
            expected_code: StatusCode::NOT_FOUND,
            expected_error: "not found: project with name \"nonexistent\"",
            body: Some("".into()),
        },
        // Error case: fetch an instance in a nonexistent project
        TestCase {
            method: Method::GET,
            uri: "/organizations/test-org/projects/nonexistent/instances/my-instance",
            expected_code: StatusCode::NOT_FOUND,
            expected_error: "not found: project with name \"nonexistent\"",
            body: Some("".into()),
        },
        // Error case: fetch an instance with an invalid name
        TestCase {
            method: Method::GET,
            uri: "/organizations/test-org/projects/nonexistent/instances/my_instance",
            expected_code: StatusCode::BAD_REQUEST,
            expected_error: "bad parameter in URL path: name contains \
                invalid character: \"_\" (allowed characters are lowercase \
                ASCII, digits, and \"-\")",
            body: Some("".into()),
        },
        // Error case: delete an instance with an invalid name
        TestCase {
            method: Method::DELETE,
            uri: "/organizations/test-org/projects/nonexistent/instances/my_instance",
            expected_code: StatusCode::BAD_REQUEST,
            expected_error: "bad parameter in URL path: name contains \
                invalid character: \"_\" (allowed characters are lowercase \
                ASCII, digits, and \"-\")",
            body: Some("".into()),
        },
    ];

    for test_case in test_cases {
        let error: HttpErrorResponseBody = if let Some(body) = test_case.body {
            NexusRequest::expect_failure_with_body(
                client,
                test_case.expected_code,
                test_case.method,
                test_case.uri,
                &body,
            )
        } else {
            NexusRequest::expect_failure(
                client,
                test_case.expected_code,
                test_case.method,
                test_case.uri,
            )
        }
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap()
        .await;

        assert_eq!(test_case.expected_error, error.message);
    }
}

#[nexus_test]
async fn test_projects_basic(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;
    let projects_url = "/organizations/test-org/projects";

    // Verify that there are no projects to begin with.
    let projects = projects_list(&client, &projects_url).await;
    assert_eq!(0, projects.len());

    // Create three projects used by the rest of this test.
    let projects_to_create = vec!["simproject1", "simproject2", "simproject3"];
    let new_project_ids = {
        let mut project_ids: Vec<Uuid> = Vec::new();
        for project_name in projects_to_create {
            let new_project: Project = NexusRequest::objects_post(
                client,
                projects_url,
                &params::ProjectCreate {
                    identity: IdentityMetadataCreateParams {
                        name: project_name.parse().unwrap(),
                        description: String::from(
                            "<auto-generated by test suite>",
                        ),
                    },
                },
            )
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to make request")
            .parsed_body()
            .unwrap();
            assert_eq!(new_project.identity.name.as_str(), project_name);
            assert_eq!(
                new_project.identity.description,
                String::from("<auto-generated by test suite>")
            );
            project_ids.push(new_project.identity.id);
        }

        project_ids
    };

    // Error case: GET /organizations/test-org/projects/simproject1/nonexistent
    // (a path that does not exist beneath a resource that does exist)
    let error = client
        .make_request_error(
            Method::GET,
            "/organizations/test-org/projects/simproject1/nonexistent",
            StatusCode::NOT_FOUND,
        )
        .await;
    assert_eq!("Not Found", error.message);

    // Basic GET /organizations/test-org/projects now that we've created a few.
    // TODO-coverage: pagination
    // TODO-coverage: marker even without pagination
    let initial_projects = projects_list(&client, &projects_url).await;
    assert_eq!(initial_projects.len(), 3);
    assert_eq!(initial_projects[0].identity.id, new_project_ids[0]);
    assert_eq!(initial_projects[0].identity.name, "simproject1");
    assert!(initial_projects[0].identity.description.len() > 0);
    assert_eq!(initial_projects[1].identity.id, new_project_ids[1]);
    assert_eq!(initial_projects[1].identity.name, "simproject2");
    assert!(initial_projects[1].identity.description.len() > 0);
    assert_eq!(initial_projects[2].identity.id, new_project_ids[2]);
    assert_eq!(initial_projects[2].identity.name, "simproject3");
    assert!(initial_projects[2].identity.description.len() > 0);

    // Basic test of out-of-the-box GET
    // /organizations/test-org/projects/simproject2
    let project =
        project_get(&client, "/organizations/test-org/projects/simproject2")
            .await;
    let expected = &initial_projects[1];
    assert_eq!(project.identity.id, expected.identity.id);
    assert_eq!(project.identity.name, expected.identity.name);
    assert_eq!(project.identity.description, expected.identity.description);
    assert!(project.identity.description.len() > 0);

    // Delete "simproject2", but first delete:
    // - The default subnet within the default VPC
    // - The default VPC
    NexusRequest::object_delete(
        client,
        "/organizations/test-org/projects/simproject2/vpcs/default/subnets/default",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    NexusRequest::object_delete(
        client,
        "/organizations/test-org/projects/simproject2/vpcs/default",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    NexusRequest::object_delete(
        client,
        "/organizations/test-org/projects/simproject2",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Having deleted "simproject2", verify "GET", "PUT", and "DELETE" on
    // "/organizations/test-org/projects/simproject2".
    for method in [Method::GET, Method::DELETE] {
        NexusRequest::expect_failure(
            client,
            StatusCode::NOT_FOUND,
            method,
            "/organizations/test-org/projects/simproject2",
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");
    }
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PUT,
            "/organizations/test-org/projects/simproject2",
        )
        .body(Some(&params::ProjectUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: None,
            },
        }))
        .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Similarly, verify "GET /organizations/test-org/projects"
    let expected_projects: Vec<&Project> = initial_projects
        .iter()
        .filter(|p| p.identity.name != "simproject2")
        .collect();
    let new_projects =
        projects_list(&client, "/organizations/test-org/projects").await;
    assert_eq!(new_projects.len(), expected_projects.len());
    assert_eq!(new_projects[0].identity.id, expected_projects[0].identity.id);
    assert_eq!(
        new_projects[0].identity.name,
        expected_projects[0].identity.name
    );
    assert_eq!(
        new_projects[0].identity.description,
        expected_projects[0].identity.description
    );
    assert_eq!(new_projects[1].identity.id, expected_projects[1].identity.id);
    assert_eq!(
        new_projects[1].identity.name,
        expected_projects[1].identity.name
    );
    assert_eq!(
        new_projects[1].identity.description,
        expected_projects[1].identity.description
    );

    // Update "simproject3".  We'll make sure that's reflected in the other
    // requests.
    let project_update = params::ProjectUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Li'l lightnin'".to_string()),
        },
    };
    let project = NexusRequest::object_put(
        client,
        "/organizations/test-org/projects/simproject3",
        Some(&project_update),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("expected success")
    .parsed_body::<Project>()
    .expect("failed to parse Project from PUT response");
    assert_eq!(project.identity.id, new_project_ids[2]);
    assert_eq!(project.identity.name, "simproject3");
    assert_eq!(project.identity.description, "Li'l lightnin'");

    let expected = project;
    let project =
        project_get(&client, "/organizations/test-org/projects/simproject3")
            .await;
    assert_eq!(project.identity.name, expected.identity.name);
    assert_eq!(project.identity.description, expected.identity.description);
    assert_eq!(project.identity.description, "Li'l lightnin'");

    // Update "simproject3" in a way that changes its name.  This is a deeper
    // operation under the hood.  This case also exercises changes to multiple
    // fields in one request.
    let project_update = params::ProjectUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some("lil-lightnin".parse().unwrap()),
            description: Some("little lightning".to_string()),
        },
    };
    let project = NexusRequest::object_put(
        client,
        "/organizations/test-org/projects/simproject3",
        Some(&project_update),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("expected success")
    .parsed_body::<Project>()
    .expect("failed to parse Project from PUT response");
    assert_eq!(project.identity.id, new_project_ids[2]);
    assert_eq!(project.identity.name, "lil-lightnin");
    assert_eq!(project.identity.description, "little lightning");

    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        "/organizations/test-org/projects/simproject3",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("expected success");

    // Try to create a project with a name that conflicts with an existing one.
    let project_create = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "simproject1".parse().unwrap(),
            description: "a duplicate of simproject1".to_string(),
        },
    };
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &projects_url)
            .body(Some(&project_create))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("expected request to fail")
    .parsed_body::<HttpErrorResponseBody>()
    .unwrap();
    assert_eq!("already exists: project \"simproject1\"", error.message);

    // TODO-coverage try to rename it to a name that conflicts

    // Try to create a project with an unsupported name.
    // TODO-polish why doesn't serde include the field name in this error?
    #[derive(Serialize)]
    struct BadProject {
        name: &'static str,
        description: &'static str,
    }
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &projects_url)
            .body(Some(&BadProject {
                name: "sim_project",
                description: "underscore",
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("expected request to fail")
    .parsed_body::<HttpErrorResponseBody>()
    .unwrap();
    assert!(error.message.starts_with(
        "unable to parse JSON body: name contains invalid character: \"_\" \
         (allowed characters are lowercase ASCII, digits, and \"-\""
    ));

    // Now, really do create another project.
    let project_create = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "honor-roller".parse().unwrap(),
            description: "a soapbox racer".to_string(),
        },
    };
    let project: Project =
        NexusRequest::objects_post(client, projects_url, &project_create)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to make request")
            .parsed_body()
            .unwrap();
    assert_eq!(project.identity.name, "honor-roller");
    assert_eq!(project.identity.description, "a soapbox racer");

    // List projects again and verify all of our changes.  We should have:
    //
    // - "honor-roller" with description "a soapbox racer"
    // - "lil-lightnin" with description "little lightning"
    // - "simproject1", same as when it was created.
    let projects = projects_list(&client, &projects_url).await;
    assert_eq!(projects.len(), 3);
    assert_eq!(projects[0].identity.name, "honor-roller");
    assert_eq!(projects[0].identity.description, "a soapbox racer");
    assert_eq!(projects[1].identity.name, "lil-lightnin");
    assert_eq!(projects[1].identity.description, "little lightning");
    assert_eq!(projects[2].identity.name, "simproject1");
    assert!(projects[2].identity.description.len() > 0);
}

#[nexus_test]
async fn test_projects_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;

    // Verify that there are no projects to begin with.
    let projects_url = "/organizations/test-org/projects";
    assert_eq!(projects_list(&client, &projects_url).await.len(), 0);

    // Create a large number of projects that we can page through.
    let projects_total = 10;
    let projects_subset = 3;
    let mut projects_created = Vec::with_capacity(projects_total);
    for _ in 0..projects_total {
        // We'll use uuids for the names to make sure that works, and that we
        // can paginate through by _name_ even though the names happen to be
        // uuids.  Names have to start with a letter and can't exactly match
        // a uuid though, so we'll use a prefix.
        let mut name = Uuid::new_v4().to_string();
        name.insert_str(0, "project-");
        let project = create_project(&client, org_name, &name).await;
        projects_created.push(project.identity);
    }

    let project_names_by_name = {
        let mut clone = projects_created.clone();
        clone.sort_by_key(|v| v.name.clone());
        assert_ne!(clone, projects_created);
        clone.iter().map(|v| v.name.clone()).collect::<Vec<Name>>()
    };

    let project_names_by_id = {
        let mut clone = projects_created.clone();
        clone.sort_by_key(|v| v.id);
        assert_ne!(clone, projects_created);
        clone.iter().map(|v| v.id).collect::<Vec<Uuid>>()
    };

    // Page through all the projects in the default order, which should be in
    // increasing order of name.
    let found_projects_by_name =
        NexusRequest::iter_collection_authn::<Project>(
            &client,
            projects_url,
            "",
            Some(projects_subset),
        )
        .await
        .expect("failed to list projects")
        .all_items;
    assert_eq!(found_projects_by_name.len(), project_names_by_name.len());
    assert_eq!(
        project_names_by_name,
        found_projects_by_name
            .iter()
            .map(|v| v.identity.name.clone())
            .collect::<Vec<Name>>()
    );

    // Page through all the projects in ascending order by name, which should be
    // the same as above.
    let found_projects_by_name =
        NexusRequest::iter_collection_authn::<Project>(
            &client,
            projects_url,
            "sort_by=name_ascending",
            Some(projects_subset),
        )
        .await
        .expect("failed to list projects")
        .all_items;
    assert_eq!(found_projects_by_name.len(), project_names_by_name.len());
    assert_eq!(
        project_names_by_name,
        found_projects_by_name
            .iter()
            .map(|v| v.identity.name.clone())
            .collect::<Vec<Name>>()
    );

    // Page through all the projects in descending order by name, which should be
    // the reverse of the above.
    let mut found_projects_by_name =
        NexusRequest::iter_collection_authn::<Project>(
            &client,
            projects_url,
            "sort_by=name_descending",
            Some(projects_subset),
        )
        .await
        .expect("failed to list projects")
        .all_items;
    assert_eq!(found_projects_by_name.len(), project_names_by_name.len());
    found_projects_by_name.reverse();
    assert_eq!(
        project_names_by_name,
        found_projects_by_name
            .iter()
            .map(|v| v.identity.name.clone())
            .collect::<Vec<Name>>()
    );

    // Page through the projects in ascending order by id.
    let found_projects_by_id = NexusRequest::iter_collection_authn::<Project>(
        &client,
        projects_url,
        "sort_by=id_ascending",
        Some(projects_subset),
    )
    .await
    .expect("failed to list projects")
    .all_items;
    assert_eq!(found_projects_by_id.len(), project_names_by_id.len());
    assert_eq!(
        project_names_by_id,
        found_projects_by_id
            .iter()
            .map(|v| v.identity.id)
            .collect::<Vec<Uuid>>()
    );
}

#[nexus_test]
async fn test_sleds_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Verify that there is one sled to begin with.
    let sleds_url = "/system/hardware/sleds";
    assert_eq!(sleds_list(&client, &sleds_url).await.len(), 1);

    // Now start a few more sled agents.
    let nsleds = 3;
    let mut sas = Vec::with_capacity(nsleds);
    for _ in 0..nsleds {
        let sa_id = Uuid::new_v4();
        let log =
            cptestctx.logctx.log.new(o!( "sled_id" => sa_id.to_string() ));
        let addr = cptestctx.server.get_http_server_internal_address().await;
        sas.push(start_sled_agent(log, addr, sa_id).await.unwrap());
    }

    // List sleds again.
    let sleds_found = sleds_list(&client, &sleds_url).await;
    assert_eq!(sleds_found.len(), nsleds + 1);

    let sledids_found =
        sleds_found.iter().map(|sv| sv.identity.id).collect::<Vec<Uuid>>();
    let mut sledids_found_sorted = sledids_found.clone();
    sledids_found_sorted.sort();
    assert_eq!(sledids_found, sledids_found_sorted);

    // Tear down the agents.
    for sa in sas {
        sa.http_server.close().await.unwrap();
    }
}

async fn projects_list(
    client: &ClientTestContext,
    projects_url: &str,
) -> Vec<Project> {
    NexusRequest::iter_collection_authn(client, projects_url, "", None)
        .await
        .expect("failed to list projects")
        .all_items
}

async fn sleds_list(client: &ClientTestContext, sleds_url: &str) -> Vec<Sled> {
    objects_list_page_authz::<Sled>(client, sleds_url).await.items
}
