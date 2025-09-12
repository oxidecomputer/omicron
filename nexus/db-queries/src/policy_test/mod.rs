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

mod coverage;
mod resource_builder;
mod resources;

use crate::db::pub_test_utils::TestDatabase;
use coverage::Coverage;
use futures::StreamExt;
use nexus_auth::authn;
use nexus_auth::authn::SiloAuthnPolicy;
use nexus_auth::authn::USER_TEST_PRIVILEGED;
use nexus_auth::authz;
use nexus_auth::context::OpContext;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::FleetRole;
use nexus_types::external_api::shared::SiloRole;
use nexus_types::identity::Asset;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_test_utils::dev;
use resource_builder::DynAuthorizedResource;
use resource_builder::ResourceBuilder;
use resource_builder::ResourceSet;
use slog::{o, trace};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
    let db = TestDatabase::new_with_datastore(&logctx.log).await;
    let (opctx, datastore) = (db.opctx(), db.datastore());

    // Before we can create the resources, users, and role assignments that we
    // need, we must grant the "test-privileged" user privileges to fetch and
    // modify policies inside the "main" Silo (the one we create users in).
    let main_silo_id = Uuid::new_v4();
    let main_silo = authz::Silo::new(
        authz::FLEET,
        main_silo_id,
        LookupType::ById(main_silo_id),
    );
    datastore
        .role_assignment_replace_visible(
            &opctx,
            &main_silo,
            &[shared::RoleAssignment::for_silo_user(
                USER_TEST_PRIVILEGED.id(),
                SiloRole::Admin,
            )],
        )
        .await
        .unwrap();

    // Assemble the list of resources that we'll use for testing.  As we create
    // these resources, create the users and role assignments needed for the
    // exhaustive test.  `Coverage` is used to help verify that all resources
    // are tested or explicitly opted out.
    let exemptions = resources::exempted_authz_classes();
    let mut coverage = Coverage::new(&logctx.log, exemptions);
    let builder =
        ResourceBuilder::new(&opctx, &datastore, &mut coverage, main_silo_id);
    let test_resources = resources::make_resources(builder, main_silo_id).await;
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
                authn::Context::for_test_user(
                    user_id,
                    main_silo_id,
                    SiloAuthnPolicy::default(),
                ),
                Arc::clone(&datastore) as Arc<dyn nexus_auth::storage::Storage>,
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
            Arc::clone(&datastore) as Arc<dyn nexus_auth::storage::Storage>,
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
            true,
        )
        .await
        .unwrap();
    }

    expectorate::assert_contents(
        "tests/output/authz-roles.out",
        &std::str::from_utf8(buffer.as_ref()).expect("non-UTF8 output"),
    );

    db.terminate().await;
    logctx.cleanup_successful();
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
    test_resources: &ResourceSet,
    print_actions: bool,
) -> std::io::Result<()> {
    // Run the per-resource tests in parallel.  Since the caller will be
    // checking the overall output against some expected output, it's important
    // to emit the results in a consistent order.
    let mut futures = futures::stream::FuturesOrdered::new();
    for resource in test_resources.resources() {
        let log = log.new(o!("resource" => format!("{:?}", resource)));
        futures.push_back(authorize_one_resource(
            log,
            user_contexts.to_owned(),
            Arc::clone(&resource),
        ));
    }

    let outputs: Vec<String> = futures.collect().await;
    for o in outputs {
        write!(out, "{}", o)?;
    }

    if print_actions {
        write!(out, "ACTIONS:\n\n")?;
        for action in authz::Action::iter() {
            write!(
                out,
                "  {:>2} = {:?}\n",
                action_abbreviation(action),
                action
            )?;
        }
        write!(out, "\n")?;
    }

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

/// Verifies that Fleet-level roles are correctly conferred by Silo-level roles
///
/// The way to think about what's happening here is some chunk of the
/// authorization policy lives with each Silo (in the Silo's database record,
/// more precisely).  This test exercises various combinations of that policy,
/// plus that it's honored correctly by the authz subsystem.
#[tokio::test]
async fn test_conferred_roles() {
    // To start, this test looks a lot like the test above.
    let logctx = dev::test_setup_log("test_conferred_roles");
    let db = TestDatabase::new_with_datastore(&logctx.log).await;
    let (opctx, datastore) = (db.opctx(), db.datastore());

    // Before we can create the resources, users, and role assignments that we
    // need, we must grant the "test-privileged" user privileges to fetch and
    // modify policies inside the "main" Silo (the one we create users in).
    let main_silo_id = Uuid::new_v4();
    let main_silo = authz::Silo::new(
        authz::FLEET,
        main_silo_id,
        LookupType::ById(main_silo_id),
    );
    datastore
        .role_assignment_replace_visible(
            &opctx,
            &main_silo,
            &[shared::RoleAssignment::for_silo_user(
                USER_TEST_PRIVILEGED.id(),
                SiloRole::Admin,
            )],
        )
        .await
        .unwrap();

    let exemptions = resources::exempted_authz_classes();
    let mut coverage = Coverage::new(&logctx.log, exemptions);

    // Assemble the list of resources that we'll use for testing.  This is much
    // more limited than the main policy test because we only care about the
    // behavior on the Fleet itself, as well as some top-level resources that
    // exist outside of a silo.
    let mut builder =
        ResourceBuilder::new(&opctx, &datastore, &mut coverage, main_silo_id);
    builder.new_resource(authz::FLEET);
    builder.new_resource(authz::IP_POOL_LIST);
    let test_resources = builder.build();

    // We also create a Silo because the ResourceBuilder will create for us
    // users that we can use to test the behavior of each role.
    let mut silo_builder =
        ResourceBuilder::new(&opctx, &datastore, &mut coverage, main_silo_id);
    silo_builder.new_resource(authz::FLEET);
    silo_builder.new_resource_with_users(main_silo).await;
    let silo_resources = silo_builder.build();

    // Up to this point, this looks similar to the main policy test.  Here's
    // where things get different.
    //
    // Our goal is to run a battery of authz checks against a bunch of different
    // configurations.  These configurations vary only in the part of the policy
    // that normally comes from the Silo.  As far as the authz subsystem is
    // concerned, though, that policy comes from the authn::Context.  So we
    // don't actually need to create a bunch of different Silos with different
    // policies.  Instead, we can use different authn:::Contexts.
    let authz = Arc::new(authz::Authz::new(&logctx.log));
    let policies = vec![
        // empty policy
        BTreeMap::new(),
        // silo admin confers fleet admin
        BTreeMap::from([(SiloRole::Admin, BTreeSet::from([FleetRole::Admin]))]),
        // silo viewer confers fleet viewer
        BTreeMap::from([(
            SiloRole::Viewer,
            BTreeSet::from([FleetRole::Viewer]),
        )]),
        // silo admin confers fleet viewer (i.e., it's not hardcoded to confer
        // the same-named role)
        BTreeMap::from([(
            SiloRole::Admin,
            BTreeSet::from([FleetRole::Viewer]),
        )]),
        // It's not possible to effectively test conferring multiple roles
        // because the roles we have are hierarchical, so conferring any number
        // of roles is equivalent to conferring just the most privileged of
        // them.  Still, at least make sure it doesn't panic or something.
        BTreeMap::from([(
            SiloRole::Viewer,
            BTreeSet::from([FleetRole::Viewer, FleetRole::Admin]),
        )]),
        // Different roles can be conferred different roles.
        BTreeMap::from([
            (SiloRole::Admin, BTreeSet::from([FleetRole::Admin])),
            (SiloRole::Viewer, BTreeSet::from([FleetRole::Viewer])),
        ]),
    ];

    let mut buffer = Vec::new();
    {
        let mut out = StdoutTee::new(&mut buffer);
        for policy in policies {
            write!(out, "policy: {:?}\n", policy).unwrap();
            let policy = SiloAuthnPolicy::new(policy, false);

            let user_contexts: Vec<Arc<(String, OpContext)>> = silo_resources
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
                        authn::Context::for_test_user(
                            user_id,
                            main_silo_id,
                            policy.clone(),
                        ),
                        Arc::clone(&datastore)
                            as Arc<dyn nexus_auth::storage::Storage>,
                    );
                    Arc::new((username.clone(), opctx))
                })
                .collect();

            authorize_everything(
                &mut out,
                &logctx.log,
                &user_contexts,
                &test_resources,
                false,
            )
            .await
            .unwrap();
        }
    }

    expectorate::assert_contents(
        "tests/output/authz-conferred-roles.out",
        &std::str::from_utf8(buffer.as_ref()).expect("non-UTF8 output"),
    );

    db.terminate().await;
    logctx.cleanup_successful();
}
