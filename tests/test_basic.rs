/*!
 * Smoke tests against the API server.
 *
 * This file defines a very basic set of tests against the API.
 * TODO-coverage add test for racks
 */

use http::method::Method;
use http::StatusCode;
use oxide_api_prototype::api_model::ApiByteCount;
use oxide_api_prototype::api_model::ApiIdentityMetadata;
use oxide_api_prototype::api_model::ApiIdentityMetadataCreateParams;
use oxide_api_prototype::api_model::ApiIdentityMetadataUpdateParams;
use oxide_api_prototype::api_model::ApiInstanceCpuCount;
use oxide_api_prototype::api_model::ApiInstanceCreateParams;
use oxide_api_prototype::api_model::ApiInstanceView;
use oxide_api_prototype::api_model::ApiName;
use oxide_api_prototype::api_model::ApiProjectCreateParams;
use oxide_api_prototype::api_model::ApiProjectUpdateParams;
use oxide_api_prototype::api_model::ApiProjectView;
use std::convert::TryFrom;
use uuid::Uuid;

use dropshot::test_util::read_json;
use dropshot::test_util::read_ndjson;

pub mod common;
use common::test_setup;

#[macro_use]
extern crate slog;

#[tokio::test]
async fn test_basic_failures() {
    let testctx = test_setup("basic_failures").await;

    /* Error case: GET /nonexistent (a path with no route at all) */
    let error = testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/nonexistent",
            None as Option<()>,
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected error");
    assert_eq!("Not Found", error.message);

    /*
     * Error case: GET /projects/nonexistent (a possible value that does not
     * exist inside a collection that does exist)
     */
    let error = testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/projects/nonexistent",
            None as Option<()>,
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected error");
    assert_eq!("not found: project with name \"nonexistent\"", error.message);

    /*
     * Error case: GET /projects/-invalid-name
     * TODO-correctness is 400 the right error code here or is 404 more
     * appropriate?
     */
    let error = testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/projects/-invalid-name",
            None as Option<()>,
            StatusCode::BAD_REQUEST,
        )
        .await
        .expect_err("expected error");
    assert_eq!(
        "bad parameter in URL path: name must begin with an ASCII lowercase \
         character",
        error.message
    );

    /* Error case: PUT /projects */
    let error = testctx
        .client_testctx
        .make_request(
            Method::PUT,
            "/projects",
            None as Option<()>,
            StatusCode::METHOD_NOT_ALLOWED,
        )
        .await
        .expect_err("expected error");
    assert_eq!("Method Not Allowed", error.message);

    /* Error case: DELETE /projects */
    let error = testctx
        .client_testctx
        .make_request(
            Method::DELETE,
            "/projects",
            None as Option<()>,
            StatusCode::METHOD_NOT_ALLOWED,
        )
        .await
        .expect_err("expected error");
    assert_eq!("Method Not Allowed", error.message);

    /* Error case: list instances in a nonexistent project. */
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            "/projects/nonexistent/instances",
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected error");
    assert_eq!("not found: project with name \"nonexistent\"", error.message);

    /* Error case: fetch an instance in a nonexistent project. */
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            "/projects/nonexistent/instances/my-instance",
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected error");
    assert_eq!("not found: project with name \"nonexistent\"", error.message);

    /* Error case: fetch an instance with an invalid name. */
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            "/projects/nonexistent/instances/my_instance",
            "".into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .expect_err("expected error");
    assert_eq!(
        "bad parameter in URL path: name contains invalid character: \"_\" \
         (allowed characters are lowercase ASCII, digits, and \"-\")",
        error.message
    );

    /* Error case: delete an instance with an invalid name. */
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::DELETE,
            "/projects/nonexistent/instances/my_instance",
            "".into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .expect_err("expected error");
    assert_eq!(
        "bad parameter in URL path: name contains invalid character: \"_\" \
         (allowed characters are lowercase ASCII, digits, and \"-\")",
        error.message
    );
}

/*
 * TODO-cleanup: At this point, it would probably be best to remove any
 * pre-created projects from the server and update this test to assume no
 * initial projects.
 */
#[tokio::test]
async fn test_projects() {
    let testctx = test_setup("test_projects").await;

    /*
     * Error case: GET /projects/simproject1/nonexistent (a path that does not
     * exist beneath a resource that does exist)
     */
    let error = testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/projects/simproject1/nonexistent",
            None as Option<()>,
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected error");
    assert_eq!("Not Found", error.message);

    /*
     * Basic test of out-of-the-box GET /projects
     * TODO-coverage: pagination
     * TODO-coverage: marker even without pagination
     */
    let mut response = testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/projects",
            None as Option<()>,
            StatusCode::OK,
        )
        .await
        .expect("expected success");
    let initial_projects: Vec<ApiProjectView> =
        read_ndjson(&mut response).await;
    let simid1 =
        Uuid::parse_str("1eb2b543-b199-405f-b705-1739d01a197c").unwrap();
    let simid2 =
        Uuid::parse_str("4f57c123-3bda-4fae-94a2-46a9632d40b6").unwrap();
    let simid3 =
        Uuid::parse_str("4aac89b0-df9a-441d-b050-f953476ea290").unwrap();
    assert_eq!(initial_projects.len(), 3);
    assert_eq!(initial_projects[0].identity.id, simid1);
    assert_eq!(initial_projects[0].identity.name, "simproject1");
    assert!(initial_projects[0].identity.description.len() > 0);
    assert_eq!(initial_projects[1].identity.id, simid2);
    assert_eq!(initial_projects[1].identity.name, "simproject2");
    assert!(initial_projects[1].identity.description.len() > 0);
    assert_eq!(initial_projects[2].identity.id, simid3);
    assert_eq!(initial_projects[2].identity.name, "simproject3");
    assert!(initial_projects[2].identity.description.len() > 0);

    /*
     * Basic test of out-of-the-box GET /projects/simproject2
     */
    let mut response = testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/projects/simproject2",
            None as Option<()>,
            StatusCode::OK,
        )
        .await
        .expect("expected success");
    let project: ApiProjectView = read_json(&mut response).await;
    let expected = &initial_projects[1];
    assert_eq!(project.identity.id, expected.identity.id);
    assert_eq!(project.identity.name, expected.identity.name);
    assert_eq!(project.identity.description, expected.identity.description);
    assert!(project.identity.description.len() > 0);

    /*
     * Delete "simproject2".  We'll make sure that's reflected in the other
     * requests.
     */
    testctx
        .client_testctx
        .make_request(
            Method::DELETE,
            "/projects/simproject2",
            None as Option<()>,
            StatusCode::NO_CONTENT,
        )
        .await
        .expect("expected success");

    /*
     * Having deleted "simproject2", verify "GET", "PUT", and "DELETE" on
     * "/projects/simproject2".
     */
    testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/projects/simproject2",
            None as Option<()>,
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected failure");
    testctx
        .client_testctx
        .make_request(
            Method::DELETE,
            "/projects/simproject2",
            None as Option<()>,
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected failure");
    testctx
        .client_testctx
        .make_request(
            Method::PUT,
            "/projects/simproject2",
            Some(ApiProjectUpdateParams {
                identity: ApiIdentityMetadataUpdateParams {
                    name: None,
                    description: None,
                },
            }),
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected failure");

    /*
     * Similarly, verify "GET /projects"
     */
    let mut response = testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/projects",
            None as Option<()>,
            StatusCode::OK,
        )
        .await
        .expect("expected success");
    let expected_projects: Vec<&ApiProjectView> = initial_projects
        .iter()
        .filter(|p| p.identity.name != "simproject2")
        .collect();
    let new_projects: Vec<ApiProjectView> = read_ndjson(&mut response).await;
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

    /*
     * Update "simproject3".  We'll make sure that's reflected in the other
     * requests.
     */
    let project_update = ApiProjectUpdateParams {
        identity: ApiIdentityMetadataUpdateParams {
            name: None,
            description: Some("Li'l lightnin'".to_string()),
        },
    };
    let mut response = testctx
        .client_testctx
        .make_request(
            Method::PUT,
            "/projects/simproject3",
            Some(project_update),
            StatusCode::OK,
        )
        .await
        .expect("expected success");
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.identity.id, simid3);
    assert_eq!(project.identity.name, "simproject3");
    assert_eq!(project.identity.description, "Li'l lightnin'");

    let mut response = testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/projects/simproject3",
            None as Option<()>,
            StatusCode::OK,
        )
        .await
        .expect("expected success");
    let expected = project;
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.identity.name, expected.identity.name);
    assert_eq!(project.identity.description, expected.identity.description);
    assert_eq!(project.identity.description, "Li'l lightnin'");

    /*
     * Update "simproject3" in a way that changes its name.  This is a deeper
     * operation under the hood.  This case also exercises changes to multiple
     * fields in one request.
     */
    let project_update = ApiProjectUpdateParams {
        identity: ApiIdentityMetadataUpdateParams {
            name: Some(ApiName::try_from("lil-lightnin").unwrap()),
            description: Some("little lightning".to_string()),
        },
    };
    let mut response = testctx
        .client_testctx
        .make_request(
            Method::PUT,
            "/projects/simproject3",
            Some(project_update),
            StatusCode::OK,
        )
        .await
        .expect("failed to make request to server");
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.identity.id, simid3);
    assert_eq!(project.identity.name, "lil-lightnin");
    assert_eq!(project.identity.description, "little lightning");

    testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/projects/simproject3",
            None as Option<()>,
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected failure");

    /*
     * Try to create a project with a name that conflicts with an existing one.
     */
    let project_create = ApiProjectCreateParams {
        identity: ApiIdentityMetadataCreateParams {
            name: ApiName::try_from("simproject1".to_string()).unwrap(),
            description: "a duplicate of simproject1".to_string(),
        },
    };
    let error = testctx
        .client_testctx
        .make_request(
            Method::POST,
            "/projects",
            Some(project_create),
            StatusCode::BAD_REQUEST,
        )
        .await
        .expect_err("expected failure");
    assert_eq!("already exists: project \"simproject1\"", error.message);

    /*
     * Try to create a project with an unsupported name.
     * TODO-polish why doesn't serde include the field name in this error?
     */
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::POST,
            "/projects",
            "{\"name\": \"sim_project\", \"description\": \"underscore\"}"
                .into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .expect_err("expected failure");
    assert!(error.message.starts_with(
        "unable to parse body: name contains invalid character: \"_\" \
         (allowed characters are lowercase ASCII, digits, and \"-\""
    ));

    /*
     * Now, really do create a new project.
     */
    let project_create = ApiProjectCreateParams {
        identity: ApiIdentityMetadataCreateParams {
            name: ApiName::try_from("honor-roller").unwrap(),
            description: "a soapbox racer".to_string(),
        },
    };
    let mut response = testctx
        .client_testctx
        .make_request(
            Method::POST,
            "/projects",
            Some(project_create),
            StatusCode::CREATED,
        )
        .await
        .expect("expected success");
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.identity.name, "honor-roller");
    assert_eq!(project.identity.description, "a soapbox racer");

    /*
     * List projects again and verify all of our changes.  We should have:
     *
     * - "honor-roller" with description "a soapbox racer"
     * - "lil-lightnin" with description "little lightning"
     * - "simproject1", same as out-of-the-box
     */
    let mut response = testctx
        .client_testctx
        .make_request(
            Method::GET,
            "/projects",
            None as Option<()>,
            StatusCode::OK,
        )
        .await
        .expect("expected success");
    let projects: Vec<ApiProjectView> = read_ndjson(&mut response).await;
    assert_eq!(projects.len(), 3);
    assert_eq!(projects[0].identity.name, "honor-roller");
    assert_eq!(projects[0].identity.description, "a soapbox racer");
    assert_eq!(projects[1].identity.name, "lil-lightnin");
    assert_eq!(projects[1].identity.description, "little lightning");
    assert_eq!(projects[2].identity.name, "simproject1");
    assert!(projects[2].identity.description.len() > 0);

    testctx.teardown().await;
}

#[tokio::test]
async fn test_instances() {
    let testctx = test_setup("test_instances").await;

    /* Create a project that we'll use for testing. */
    let project_name = "springfield-squidport";
    let url_instances = format!("/projects/{}/instances", project_name);
    testctx
        .client_testctx
        .make_request(
            Method::POST,
            "/projects",
            Some(ApiProjectCreateParams {
                identity: ApiIdentityMetadataCreateParams {
                    name: ApiName::try_from(project_name).unwrap(),
                    description: "a pier".to_string(),
                },
            }),
            StatusCode::CREATED,
        )
        .await
        .unwrap();

    /* List instances.  There aren't any yet. */
    let mut response = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            &url_instances,
            "".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();
    let instances: Vec<ApiInstanceView> = read_ndjson(&mut response).await;
    assert_eq!(instances.len(), 0);

    /* Make sure we get a 404 if we fetch one. */
    let instance_url = format!("{}/just-rainsticks", url_instances);
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            &instance_url,
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.message,
        "not found: instance with name \"just-rainsticks\""
    );

    /* Ditto if we try to delete one. */
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::DELETE,
            &instance_url,
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.message,
        "not found: instance with name \"just-rainsticks\""
    );

    /* Create an instance. */
    let new_instance = ApiInstanceCreateParams {
        identity: ApiIdentityMetadataCreateParams {
            name: ApiName::try_from("just-rainsticks").unwrap(),
            description: String::from("sells rainsticks"),
        },
        ncpus: ApiInstanceCpuCount(4),
        memory: ApiByteCount::from_mebibytes(256),
        boot_disk_size: ApiByteCount::from_gibibytes(1),
        hostname: String::from("rainsticks"),
    };
    let mut response = testctx
        .client_testctx
        .make_request(
            Method::POST,
            &url_instances,
            Some(new_instance.clone()),
            StatusCode::CREATED,
        )
        .await
        .unwrap();
    let instance: ApiInstanceView = read_json(&mut response).await;
    assert_eq!(instance.identity.name, "just-rainsticks");
    assert_eq!(instance.identity.description, "sells rainsticks");
    let ApiInstanceCpuCount(nfoundcpus) = instance.ncpus;
    assert_eq!(nfoundcpus, 4);
    assert_eq!(instance.memory.to_whole_mebibytes(), 256);
    assert_eq!(instance.boot_disk_size.to_whole_mebibytes(), 1024);
    assert_eq!(instance.hostname, "rainsticks");

    /* Attempt to create a second instance with a conflicting name. */
    let error = testctx
        .client_testctx
        .make_request(
            Method::POST,
            &url_instances,
            Some(new_instance),
            StatusCode::BAD_REQUEST,
        )
        .await
        .unwrap_err();
    assert_eq!(error.message, "already exists: instance \"just-rainsticks\"");

    /* List instances again and expect to find the one we just created. */
    let mut response = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            &url_instances,
            "".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();
    let instances: Vec<ApiInstanceView> = read_ndjson(&mut response).await;
    assert_eq!(instances.len(), 1);
    instances_eq(&instances[0], &instance);

    /* Fetch the instance and expect it to match. */
    let mut response = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            &instance_url,
            "".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();
    let instance_get: ApiInstanceView = read_json(&mut response).await;
    instances_eq(&instances[0], &instance_get);

    /* Delete the instance. */
    testctx
        .client_testctx
        .make_request_with_body(
            Method::DELETE,
            &instance_url,
            "".into(),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    /* Make sure we get a 404 if we fetch it. */
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            &instance_url,
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.message,
        "not found: instance with name \"just-rainsticks\""
    );

    /* Try to delete it again.  This should fail with a 404, too. */
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::DELETE,
            &instance_url,
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.message,
        "not found: instance with name \"just-rainsticks\""
    );

    /* List instances again.  We should find none. */
    let mut response = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            &url_instances,
            "".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();
    let instances: Vec<ApiInstanceView> = read_ndjson(&mut response).await;
    assert_eq!(instances.len(), 0);

    /*
     * The rest of these examples attempt to create invalid instances.  We don't
     * do exhaustive tests of the model here -- those are part of unit tests --
     * but we exercise a few different types of errors to make sure those get
     * passed through properly.
     */

    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::POST,
            &url_instances,
            "{".into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .unwrap_err();
    assert!(error
        .message
        .starts_with("unable to parse body: EOF while parsing an object"));

    let request_body = r##"
        {
            "name": "an-instance",
            "description": "will never exist",
            "ncpus": -3,
            "memory": 256,
            "boot_disk_size": 2048,
            "hostname": "localhost",
        }
    "##;
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::POST,
            &url_instances,
            request_body.into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .unwrap_err();
    assert!(error
        .message
        .starts_with("unable to parse body: invalid value: integer `-3`"));

    testctx.teardown().await;
}

fn instances_eq(instance1: &ApiInstanceView, instance2: &ApiInstanceView) {
    identity_eq(&instance1.identity, &instance2.identity);
    assert_eq!(instance1.project_id, instance2.project_id);

    let ApiInstanceCpuCount(nfoundcpus1) = instance1.ncpus;
    let ApiInstanceCpuCount(nfoundcpus2) = instance2.ncpus;
    assert_eq!(nfoundcpus1, nfoundcpus2);

    assert_eq!(instance1.memory.to_bytes(), instance2.memory.to_bytes());
    assert_eq!(
        instance1.boot_disk_size.to_bytes(),
        instance2.boot_disk_size.to_bytes()
    );
    assert_eq!(instance1.hostname, instance2.hostname);
    assert_eq!(instance1.state, instance2.state);
}

fn identity_eq(ident1: &ApiIdentityMetadata, ident2: &ApiIdentityMetadata) {
    assert_eq!(ident1.id, ident2.id);
    assert_eq!(ident1.name, ident2.name);
    assert_eq!(ident1.description, ident2.description);
    assert_eq!(ident1.time_created, ident2.time_created);
    assert_eq!(ident1.time_modified, ident2.time_modified);
}
