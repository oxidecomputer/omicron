/*!
 * Smoke tests against the API server.
 *
 * This file defines a very basic set of tests against the API.
 * TODO-coverage add test for racks, servers
 */

use http::method::Method;
use http::StatusCode;
use oxide_api_prototype::api_model::ApiIdentityMetadataCreateParams;
use oxide_api_prototype::api_model::ApiIdentityMetadataUpdateParams;
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
