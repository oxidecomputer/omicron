// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Unit tests for the Oso policy
//!
//! This differs from the end-to-end integration tests for authz.  The tests
//! here verify:
//!
//! - for resources covered by RBAC: that the roles in the policy grant the
//!   permissions that we expect that they do
//!
//! - for other policies: that the policy reflects the privileges that we expect
//!   (e.g., ordinary users don't have internal roles)
//!
//! XXX-dap TODO:
//! - review above comment
//! - review remaining XXX-dap
//! - clean up, document test
//! - figure out what other types to add

use self::resources::Authorizable;
use self::resources::ResourceBuilder;
use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use futures::StreamExt;
use nexus_test_utils::db::test_setup_database;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_test_utils::dev;
use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;
use strum::IntoEnumIterator;
use uuid::Uuid;

mod coverage;
mod resources;
use coverage::Coverage;
use resources::Resources;

#[tokio::test(flavor = "multi_thread")]
async fn test_iam_roles_behavior() {
    let logctx = dev::test_setup_log("test_iam_roles");
    let mut db = test_setup_database(&logctx.log).await;
    let (opctx, datastore) = db::datastore::datastore_test(&logctx, &db).await;

    let mut coverage = Coverage::new(&logctx.log);
    let silo1_id = *resources::SILO1_ID;
    let builder =
        ResourceBuilder::new(&opctx, &datastore, &mut coverage, silo1_id);
    let test_resources = make_resources(builder).await;
    coverage.verify();

    // Create an OpContext for each user for testing.
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
                authn::Context::for_test_user(user_id, silo1_id),
                Arc::clone(&datastore),
            );

            Arc::new((username.clone(), opctx))
        })
        .collect();

    // Create and test an unauthenticated OpContext as well.
    //
    // We could test the "test-privileged" and "test-unprivileged" users, but it
    // wouldn't be very interesting: they're in a different Silo than the
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

    let mut buffer = Vec::new();
    {
        let mut out = StdoutTee::new(&mut buffer);
        run_test_operations(
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

// The hierarchy looks like this:
// fleet
// fleet/s1
// fleet/s1/o1
// fleet/s1/o1/p1
// fleet/s1/o1/p1/vpc1
// fleet/s1/o1/p2
// fleet/s1/o1/p2/vpc1
// fleet/s1/o2
// fleet/s1/o2/p1
// fleet/s1/o2/p1/vpc1
// fleet/s2
// fleet/s2/o1
// fleet/s2/o1/p1
// fleet/s2/o1/p1/vpc1
async fn make_resources<'a>(mut builder: ResourceBuilder<'a>) -> Resources {
    builder.new_resource(authz::DATABASE.clone());
    builder.new_resource_with_roles(authz::FLEET.clone()).await;

    let silo1 = authz::Silo::new(
        authz::FLEET,
        *resources::SILO1_ID,
        LookupType::ByName(String::from("silo1")),
    );
    builder.new_resource_with_roles(silo1.clone()).await;

    let silo1_org1 = authz::Organization::new(
        silo1.clone(),
        make_uuid(),
        LookupType::ByName(String::from("silo1-org1")),
    );
    builder.new_resource_with_roles(silo1_org1.clone()).await;

    make_project(&mut builder, &silo1_org1, "silo1-org1-proj1", true).await;
    make_project(&mut builder, &silo1_org1, "silo1-org1-proj2", false).await;

    let silo1_org2 = authz::Organization::new(
        silo1.clone(),
        make_uuid(),
        LookupType::ByName(String::from("silo1-org2")),
    );
    builder.new_resource(silo1_org2.clone());
    make_project(&mut builder, &silo1_org2, "silo1-org2-proj1", false).await;

    let silo2 = authz::Silo::new(
        authz::FLEET,
        make_uuid(),
        LookupType::ByName(String::from("silo2")),
    );
    builder.new_resource(silo2.clone());
    let silo2_org1 = authz::Organization::new(
        silo2.clone(),
        make_uuid(),
        LookupType::ByName(String::from("silo2-org1")),
    );
    builder.new_resource(silo2_org1.clone());
    make_project(&mut builder, &silo2_org1, "silo2-org1-proj1", false).await;

    builder.build()
}

async fn make_project(
    builder: &mut ResourceBuilder<'_>,
    organization: &authz::Organization,
    project_name: &str,
    with_roles: bool,
) {
    let project = authz::Project::new(
        organization.clone(),
        make_uuid(),
        LookupType::ByName(project_name.to_string()),
    );
    if with_roles {
        builder.new_resource_with_roles(project.clone()).await;
    } else {
        builder.new_resource(project.clone());
    }

    let vpc1_name = format!("{}-vpc1", project_name);
    let vpc1 = authz::Vpc::new(
        project.clone(),
        make_uuid(),
        LookupType::ByName(vpc1_name.clone()),
    );

    // XXX-dap TODO-coverage add more different kinds of children
    builder.new_resource(authz::Disk::new(
        project.clone(),
        make_uuid(),
        LookupType::ByName(format!("{}-disk1", project_name)),
    ));
    builder.new_resource(authz::Instance::new(
        project.clone(),
        make_uuid(),
        LookupType::ByName(format!("{}-instance1", project_name)),
    ));
    builder.new_resource(vpc1.clone());
    // Test a resource nested two levels below Project
    builder.new_resource(authz::VpcSubnet::new(
        vpc1,
        make_uuid(),
        LookupType::ByName(format!("{}-subnet1", vpc1_name)),
    ));
}

async fn run_test_operations<W: Write>(
    mut out: W,
    log: &slog::Logger,
    user_contexts: &[Arc<(String, OpContext)>],
    test_resources: &Resources,
) -> std::io::Result<()> {
    let mut futures = futures::stream::FuturesOrdered::new();

    // Run the per-resource tests in parallel.
    for resource in test_resources.resources() {
        let log = log.new(o!("resource" => format!("{:?}", resource)));
        futures.push(test_one_resource(
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

async fn test_one_resource(
    log: slog::Logger,
    user_contexts: Vec<Arc<(String, OpContext)>>,
    resource: Arc<dyn Authorizable>,
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

// XXX-dap make this deterministic
// Most of the uuids here are hardcoded rather than randomly generated for
// debuggability.
pub fn make_uuid() -> Uuid {
    Uuid::new_v4()
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
