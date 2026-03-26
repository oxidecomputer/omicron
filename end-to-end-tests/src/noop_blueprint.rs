//! Test that generating a new blueprint on a freshly-installed system will not
//! change anything.

#![cfg(test)]

use internal_dns_resolver::Resolver;
use internal_dns_types::names::ServiceName;
use nexus_lockstep_client::Client as NexusClient;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_test_utils::dev::test_setup_log;
use omicron_uuid_kinds::GenericUuid;
use slog::{debug, info};
use slog_error_chain::InlineErrorChain;
use std::time::Duration;
use thiserror::Error;

/// Test that generating a new blueprint on a freshly-installed system will not
/// change anything.
///
/// If this test fails, there's probably a bug somewhere.  Maybe the initial
/// system blueprint is incorrect or incomplete or maybe the planner is doing
/// the wrong thing after initial setup.
#[tokio::test]
async fn new_blueprint_noop() {
    // In order to check anything with blueprints, we need to reach the Nexus
    // internal API.  This in turn requires finding the internal DNS servers.
    let rss_config =
        crate::helpers::ctx::rss_config().expect("loading RSS config");
    let rack_subnet = &rss_config.rack_network_config.rack_subnet;
    println!("rack subnet: {}", rack_subnet);
    let logctx = test_setup_log("new_blueprint_noop");
    let resolver =
        Resolver::new_from_ip(logctx.log.clone(), rack_subnet.addr())
            .expect("creating internal DNS resolver");

    // Wait up to 5 minutes to get a working Nexus client.
    let nexus_client = wait_for_condition(
        || async {
            match make_nexus_client(&resolver, &logctx.log).await {
                Ok(nexus_client) => Ok(nexus_client),
                Err(e) => {
                    debug!(
                        &logctx.log,
                        "obtaining a working Nexus client failed";
                        InlineErrorChain::new(&e),
                    );
                    Err(CondCheckError::<()>::NotYet)
                }
            }
        },
        &Duration::from_millis(500),
        &Duration::from_secs(300),
    )
    .await
    .expect("timed out waiting to obtain a working Nexus client");
    println!("Nexus is running and has a target blueprint");

    // Now generate a new blueprint.
    let new_blueprint = nexus_client
        .blueprint_regenerate()
        .await
        .expect("failed to generate new blueprint")
        .into_inner();
    println!("new blueprint generated: {}", new_blueprint.id);
    let parent_blueprint_id = new_blueprint
        .parent_blueprint_id
        .expect("generated blueprint always has a parent");
    println!("parent blueprint id:     {}", parent_blueprint_id);

    // Fetch its parent.
    let parent_blueprint = nexus_client
        .blueprint_view(parent_blueprint_id.as_untyped_uuid())
        .await
        .expect("failed to fetch parent blueprint")
        .into_inner();

    let diff = new_blueprint.diff_since_blueprint(&parent_blueprint);
    println!("new blueprint:     {}", new_blueprint.id);
    println!("differences:");
    println!("{}", diff.display());

    if diff.has_changes() {
        panic!(
            "unexpected changes between initial blueprint and \
             newly-generated one (see above)"
        );
    }

    logctx.cleanup_successful();
}

/// Error returned by [`make_nexus_client()`].
#[derive(Debug, Error)]
enum MakeNexusError {
    #[error("looking up Nexus IP in internal DNS")]
    Resolve(#[from] internal_dns_resolver::ResolveError),
    #[error("making request to Nexus")]
    Request(
        #[from]
        nexus_lockstep_client::Error<nexus_lockstep_client::types::Error>,
    ),
}

/// Make one attempt to look up the IP of Nexus in internal DNS and make an HTTP
/// request to its internal API to fetch its current target blueprint.
///
/// If this succeeds, Nexus is ready for the rest of this test to proceed.
///
/// Returns a client for this Nexus.
async fn make_nexus_client(
    resolver: &Resolver,
    log: &slog::Logger,
) -> Result<NexusClient, MakeNexusError> {
    debug!(log, "doing DNS lookup for Nexus");
    let nexus_ip =
        resolver.lookup_socket_v6(ServiceName::NexusLockstep).await?;
    let url = format!("http://{}", nexus_ip);
    debug!(log, "found Nexus IP"; "nexus_ip" => %nexus_ip, "url" => &url);

    let client = NexusClient::new(&url, log.clone());

    // Once this call succeeds, Nexus is ready for us to proceed.
    let blueprint_response = client.blueprint_target_view().await?.into_inner();
    info!(log, "found target blueprint (Nexus is ready)";
        "target_blueprint" => ?blueprint_response
    );

    Ok(client)
}
