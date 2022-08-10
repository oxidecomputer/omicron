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

use super::ApiResourceWithRolesType;
use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::model::DatabaseString;
use crate::external_api::shared;
use authz::ApiResourceWithRoles;
use futures::StreamExt;
use nexus_test_utils::db::test_setup_database;
use omicron_common::api::external::Error;
use omicron_test_utils::dev;
use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;
use strum::IntoEnumIterator;
use uuid::Uuid;

mod coverage;
mod resources;
use self::resources::Authorizable;
use coverage::Coverage;
use resources::Resources;

#[tokio::test(flavor = "multi_thread")]
async fn test_iam_roles_behavior() {
    let logctx = dev::test_setup_log("test_iam_roles");
    let mut db = test_setup_database(&logctx.log).await;
    let (opctx, datastore) = db::datastore::datastore_test(&logctx, &db).await;

    let mut coverage = Coverage::new(&logctx.log);
    let test_resources = resources::make_resources(&mut coverage);
    coverage.verify();

    let silo1_id = test_resources.silo1.resource_id();
    let mut users: Vec<(String, Uuid)> = Vec::new();

    create_users(
        &opctx,
        &*datastore,
        "fleet",
        silo1_id,
        &authz::FLEET,
        &mut users,
    )
    .await;

    create_users(
        &opctx,
        &*datastore,
        "silo1",
        silo1_id,
        &test_resources.silo1,
        &mut users,
    )
    .await;

    create_users(
        &opctx,
        &*datastore,
        "silo1-org1",
        silo1_id,
        &test_resources.silo1_org1,
        &mut users,
    )
    .await;

    create_users(
        &opctx,
        &*datastore,
        "silo1-org1-proj1",
        silo1_id,
        &test_resources.silo1_org1_proj1,
        &mut users,
    )
    .await;

    // Create an OpContext for each user for testing.
    let authz = Arc::new(authz::Authz::new(&logctx.log));
    let mut user_contexts: Vec<Arc<(String, OpContext)>> = users
        .iter()
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

async fn run_test_operations<W: Write>(
    mut out: W,
    log: &slog::Logger,
    user_contexts: &[Arc<(String, OpContext)>],
    test_resources: &Resources,
) -> std::io::Result<()> {
    let mut futures = futures::stream::FuturesOrdered::new();

    // Run the per-resource tests in parallel.
    for resource in test_resources.all_resources() {
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

async fn create_users<T>(
    opctx: &OpContext,
    datastore: &db::DataStore,
    resource_name: &str,
    silo_id: Uuid,
    authz_resource: &T,
    users: &mut Vec<(String, Uuid)>,
) where
    T: ApiResourceWithRolesType + oso::PolarClass + Clone,
    T::AllowedRoles: IntoEnumIterator,
{
    for role in T::AllowedRoles::iter() {
        let role_name = role.to_database_string();
        let username = format!("{}-{}", resource_name, role_name);
        let user_id = make_uuid();
        println!("creating user: {}", &username);
        users.push((username.clone(), user_id));

        let silo_user = db::model::SiloUser::new(silo_id, user_id, username);
        datastore
            .silo_user_create(silo_user)
            .await
            .expect("failed to create silo user");

        let old_role_assignments = datastore
            .role_assignment_fetch_visible(opctx, authz_resource)
            .await
            .expect("fetching policy");
        let new_role_assignments = old_role_assignments
            .into_iter()
            .map(|r| r.try_into().unwrap())
            .chain(std::iter::once(shared::RoleAssignment {
                identity_type: shared::IdentityType::SiloUser,
                identity_id: user_id,
                role_name: role,
            }))
            .collect::<Vec<_>>();
        datastore
            .role_assignment_replace_visible(
                opctx,
                authz_resource,
                &new_role_assignments,
            )
            .await
            .expect("failed to assign role");
    }
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
