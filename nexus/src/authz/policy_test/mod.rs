// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Unit tests for the Oso policy
//!
//! These differ from the end-to-end integration tests for authz.  The tests
//! here only exercise the code path for [`OpContext::authorize()`] and below
//! (including the Oso policy file).  They do not verify HTTP endpoint behavior.
//! The integration tests verify HTTP endpoint behavior but are not nearly so
//! exhaustive in testing the policy itself.
//!
//! XXX-dap TODO:
//! - add test for other policies: that the policy reflects the privileges that
//!   we expect (e.g., ordinary users don't have internal roles)
//! - clean up, document test
//! - figure out what other types to add
//! - review remaining XXX-dap

mod coverage;
mod resources;

use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use coverage::Coverage;
use futures::StreamExt;
use nexus_test_utils::db::test_setup_database;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_test_utils::dev;
use resources::DynAuthorizedResource;
use resources::ResourceBuilder;
use resources::Resources;
use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;
use strum::IntoEnumIterator;
use uuid::Uuid;

/// Verifies that all roles grant precisely the privileges that we expect them
/// to
///
/// This test constructs a hierarchy of resources, from the Fleet all the way
/// down to things like Instances and Disks.  For every resource that supports
/// roles (Fleet, Silo, Organization, and Project), for every supported role, we
/// create one user that has that role on one of the resources that supports it
/// (i.e., one "fleet-admin", one "fleet-viewer", one "silo-admin" for one Silo,
/// etc.).  Then we exhaustively test `authorize()` for all of these users
/// attempting every possible action on every resource we created.  This tests
/// not only whether "silo1-admin" has all privileges on "silo1", but also that
/// they have no privileges on "silo2" or "fleet".
///
/// When we say we create resources in this test, we just create the `authz`
/// objects needed to do an authz check.  We're not going through the API and we
/// don't do anything with Nexus or the database except for the creation of
/// users and role assignments.
#[tokio::test(flavor = "multi_thread")]
async fn test_iam_roles_behavior() {
    let logctx = dev::test_setup_log("test_iam_roles");
    let mut db = test_setup_database(&logctx.log).await;
    let (opctx, datastore) = db::datastore::datastore_test(&logctx, &db).await;

    // Assemble the list of resources that we'll use for testing.  As we create
    // these resources, create the users and role assignments needed for the
    // exhaustive test.  `Coverage` is used to help verify that all resources
    // are tested or explicitly opted out.
    let mut coverage = Coverage::new(&logctx.log);
    let main_silo_id = Uuid::new_v4();
    let builder =
        ResourceBuilder::new(&opctx, &datastore, &mut coverage, main_silo_id);
    let test_resources = make_resources(builder, main_silo_id).await;
    coverage.verify();

    // For each user that was created, create an OpContext that we'll use to
    // authorize various actions as that user.
    let authz = Arc::new(authz::Authz::new(&logctx.log));
    let mut user_contexts: Vec<Arc<(String, OpContext)>> = test_resources
        .users()
        .map(|(username, user_id)| {
            let user_id = *user_id;
            let user_log = logctx.log.new(o!(
                "user_id" => user_id.to_string(),
                "username" => username.clone(),
            ));
            let opctx = OpContext::for_background(
                user_log,
                Arc::clone(&authz),
                authn::Context::for_test_user(user_id, main_silo_id),
                Arc::clone(&datastore),
            );

            Arc::new((username.clone(), opctx))
        })
        .collect();

    // Create and test an unauthenticated OpContext as well.
    //
    // We could also test the "test-privileged" and "test-unprivileged" users,
    // but it wouldn't be very interesting: they're in a different Silo than the
    // resources that we're checking against so even "test-privileged" won't
    // have privileges here.   Anyway, they're composed of ordinary role
    // assignments so they're just a special case of what we're already testing.
    let user_log = logctx.log.new(o!(
        "username" => "unauthenticated",
    ));
    user_contexts.push(Arc::new((
        String::from("unauthenticated"),
        OpContext::for_background(
            user_log,
            Arc::clone(&authz),
            authn::Context::internal_unauthenticated(),
            Arc::clone(&datastore),
        ),
    )));

    // Create an output stream that writes to stdout as well as an in-memory
    // buffer.  The test run will write a textual summary to the stream.  Then
    // we'll use  use expectorate to verify it.  We do this rather than assert
    // the conditions we expect for a few reasons: first, it's handy to have a
    // printed summary of this information anyway.  Second, when there's a
    // change in behavior, it's a lot easier to review a diff of the output
    // table than to debug individual panics from deep in this test, especially
    // in the common case where there are many results that changed, not just
    // one.
    let mut buffer = Vec::new();
    {
        let mut out = StdoutTee::new(&mut buffer);
        authorize_everything(
            &mut out,
            &logctx.log,
            &user_contexts,
            &test_resources,
        )
        .await
        .unwrap();
    }

    expectorate::assert_contents(
        "tests/output/authz-roles.out",
        &std::str::from_utf8(buffer.as_ref()).expect("non-UTF8 output"),
    );

    db.cleanup().await.unwrap();
    logctx.cleanup_successful();
}

/// Assemble the set of resources that we'll test
// The main hierarchy looks like this:
//
//     fleet
//     fleet/s1
//     fleet/s1/o1
//     fleet/s1/o1/p1
//     fleet/s1/o1/p1/vpc1
//     fleet/s1/o1/p2
//     fleet/s1/o1/p2/vpc1
//     fleet/s1/o2
//     fleet/s1/o2/p1
//     fleet/s1/o2/p1/vpc1
//     fleet/s2
//     fleet/s2/o1
//     fleet/s2/o1/p1
//     fleet/s2/o1/p1/vpc1
//
// For one branch of the hierarchy, for each resource that supports roles, for
// each supported role, we will create one user with that role on that resource.
// Concretely, we'll create users like fleet-admin, silo1-admin,
// silo1-org1-viewer, silo1-org1-proj1-viewer, etc.  This is enough to check
// what privileges are granted by that role (i.e., privileges on that resource)
// as well as verify that those privileges are _not_ granted on resources in the
// other branches. We don't need to explicitly create users to test silo2 or
// silo1-org2 or silo1-org1-proj2 (for examples) because those cases are
// identical.
//
// IF YOU WANT TO ADD A NEW RESOURCE TO THIS TEST: the goal is to have this test
// show exactly what roles grant what permissions on your resource.  Generally,
// that means you'll need to create more than one instance of the resource, with
// different levels of access by different users.  This is probably easier than
// it sounds!
//
// - If your resource is NOT a collection, you only need to modify the function
//   that creates the parent collection to create an instance of your resource.
//   That's likely `make_project()`, `make_organization()`, `make_silo()`, etc.
//   If your resource is essentially a global singleton (like "Fleet"), you can
//   modify `make_resources()` directly.
//
// - If your resource is a collection, then you want to create a new function
//   similar to the other functions that make collections (`make_project()`,
//   `make_organization()`, etc.)  You'll likely need the `first_branch`
//   argument that says whether to create users and how many child hierarchies
//   to create.
// XXX-dap put this in a separate module
// XXX-dap abstract better?
async fn make_resources<'a>(
    mut builder: ResourceBuilder<'a>,
    main_silo_id: Uuid,
) -> Resources {
    builder.new_resource(authz::DATABASE.clone());
    builder.new_resource_with_users(authz::FLEET.clone()).await;

    make_silo(&mut builder, "silo1", main_silo_id, true).await;
    make_silo(&mut builder, "silo2", Uuid::new_v4(), false).await;

    builder.build()
}

/// Helper for `make_resources()` that constructs a small Silo hierarchy
async fn make_silo(
    builder: &mut ResourceBuilder<'_>,
    silo_name: &str,
    silo_id: Uuid,
    first_branch: bool,
) {
    let silo1 = authz::Silo::new(
        authz::FLEET,
        silo_id,
        LookupType::ByName(silo_name.to_string()),
    );
    if first_branch {
        builder.new_resource_with_users(silo1.clone()).await;
    } else {
        builder.new_resource(silo1.clone());
    }

    let norganizations = if first_branch { 2 } else { 1 };
    for i in 0..norganizations {
        let organization_name = format!("{}-org{}", silo_name, i + 1);
        let org_first_branch = first_branch && i == 0;
        make_organization(
            builder,
            &silo1,
            &organization_name,
            org_first_branch,
        )
        .await;
    }
}

/// Helper for `make_resources()` that constructs a small Organization hierarchy
async fn make_organization(
    builder: &mut ResourceBuilder<'_>,
    silo: &authz::Silo,
    organization_name: &str,
    first_branch: bool,
) {
    let organization = authz::Organization::new(
        silo.clone(),
        Uuid::new_v4(),
        LookupType::ByName(organization_name.to_string()),
    );
    if first_branch {
        builder.new_resource_with_users(organization.clone()).await;
    } else {
        builder.new_resource(organization.clone());
    }

    let nprojects = if first_branch { 2 } else { 1 };
    for i in 0..nprojects {
        let project_name = format!("{}-proj{}", organization_name, i + 1);
        let create_project_users = first_branch && i == 0;
        make_project(
            builder,
            &organization,
            &project_name,
            create_project_users,
        )
        .await;
    }
}

/// Helper for `make_resources()` that constructs a small Project hierarchy
async fn make_project(
    builder: &mut ResourceBuilder<'_>,
    organization: &authz::Organization,
    project_name: &str,
    first_branch: bool,
) {
    let project = authz::Project::new(
        organization.clone(),
        Uuid::new_v4(),
        LookupType::ByName(project_name.to_string()),
    );
    if first_branch {
        builder.new_resource_with_users(project.clone()).await;
    } else {
        builder.new_resource(project.clone());
    }

    let vpc1_name = format!("{}-vpc1", project_name);
    let vpc1 = authz::Vpc::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(vpc1_name.clone()),
    );

    // XXX-dap TODO-coverage add more different kinds of children
    builder.new_resource(authz::Disk::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(format!("{}-disk1", project_name)),
    ));
    builder.new_resource(authz::Instance::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(format!("{}-instance1", project_name)),
    ));
    builder.new_resource(vpc1.clone());
    // Test a resource nested two levels below Project
    builder.new_resource(authz::VpcSubnet::new(
        vpc1,
        Uuid::new_v4(),
        LookupType::ByName(format!("{}-subnet1", vpc1_name)),
    ));
}

/// Now that we've set up the resource hierarchy and users with associated
/// roles, exhaustively attempt to authorize every action for every resource by
/// every user and write a human-readable summary to `out`
///
/// The caller is responsible for checking that the output matches what's
/// expected.
async fn authorize_everything<W: Write>(
    mut out: W,
    log: &slog::Logger,
    user_contexts: &[Arc<(String, OpContext)>],
    test_resources: &Resources,
) -> std::io::Result<()> {
    // Run the per-resource tests in parallel.  Since the caller will be
    // checking the overall output against some expected output, it's important
    // to emit the results in a consistent order.
    let mut futures = futures::stream::FuturesOrdered::new();
    for resource in test_resources.resources() {
        let log = log.new(o!("resource" => format!("{:?}", resource)));
        futures.push(authorize_one_resource(
            log,
            user_contexts.to_owned(),
            Arc::clone(&resource),
        ));
    }

    let outputs: Vec<String> = futures.collect().await;
    for o in outputs {
        write!(out, "{}", o)?;
    }

    write!(out, "ACTIONS:\n\n")?;
    for action in authz::Action::iter() {
        write!(out, "  {:>2} = {:?}\n", action_abbreviation(action), action)?;
    }
    write!(out, "\n")?;

    Ok(())
}

/// Exhaustively attempt to authorize every action on this resource by each user
/// in `user_contexts`, returning a human-readable summary of what succeeded
async fn authorize_one_resource(
    log: slog::Logger,
    user_contexts: Vec<Arc<(String, OpContext)>>,
    resource: Arc<dyn DynAuthorizedResource>,
) -> String {
    let task = tokio::spawn(async move {
        let mut buffer = Vec::new();
        let mut out = Cursor::new(&mut buffer);
        write!(out, "resource: {}\n\n", resource.resource_name())?;

        write!(out, "  {:31}", "USER")?;
        for action in authz::Action::iter() {
            write!(out, " {:>2}", action_abbreviation(action))?;
        }
        write!(out, "\n")?;

        for ctx_tuple in user_contexts.iter() {
            let (ref username, ref opctx) = **ctx_tuple;
            write!(out, "  {:31}", &username)?;
            for action in authz::Action::iter() {
                let result = resource.do_authorize(opctx, action).await;
                trace!(
                    log,
                    "do_authorize result";
                    "username" => username.clone(),
                    "resource" => ?resource,
                    "action" => ?action,
                    "result" => ?result,
                );
                let summary = match result {
                    Ok(_) => '\u{2714}',
                    Err(Error::Forbidden)
                    | Err(Error::ObjectNotFound { .. }) => '\u{2718}',
                    Err(Error::Unauthenticated { .. }) => '!',
                    Err(_) => '\u{26a0}',
                };
                write!(out, " {:>2}", summary)?;
            }
            write!(out, "\n")?;
        }

        write!(out, "\n")?;
        Ok(buffer)
    });

    let result: std::io::Result<Vec<u8>> =
        task.await.expect("failed to wait for task");
    let result_str = result.expect("failed to write to string buffer");
    String::from_utf8(result_str).expect("unexpected non-UTF8 output")
}

/// Return the column header used for each action
fn action_abbreviation(action: authz::Action) -> &'static str {
    match action {
        authz::Action::Query => "Q",
        authz::Action::Read => "R",
        authz::Action::ListChildren => "LC",
        authz::Action::ReadPolicy => "RP",
        authz::Action::Modify => "M",
        authz::Action::ModifyPolicy => "MP",
        authz::Action::CreateChild => "CC",
        authz::Action::Delete => "D",
        authz::Action::ListIdentityProviders => "LP",
    }
}

/// `Write` impl that writes everything it's given to both a destination `Write`
/// and stdout via `print!`.
///
/// It'd be nice if this were instead a generic `Tee` that took an arbitrary
/// number of `Write`s and wrote data to all of them.  That's possible, but it
/// wouldn't do what we want.  We need to use `print!` in order for output to be
/// captured by the test runner.  See rust-lang/rust#12309.
struct StdoutTee<W> {
    sink: W,
}

impl<W: Write> StdoutTee<W> {
    fn new(sink: W) -> StdoutTee<W> {
        StdoutTee { sink }
    }
}

impl<W: Write> Write for StdoutTee<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        print!("{}", std::str::from_utf8(buf).expect("non-UTF8 in stdout tee"));
        self.sink.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.sink.flush()
    }
}
