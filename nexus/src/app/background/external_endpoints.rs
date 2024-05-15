// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for keeping track of all externally-visible endpoints:
//! all Silos, their externally-visible DNS names, and the TLS certificates
//! associated with those names

use super::common::BackgroundTask;
use crate::app::external_endpoints::read_all_endpoints;
pub use crate::app::external_endpoints::ExternalEndpoints;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

/// Background task that keeps track of the latest list of TLS certificates for
/// Nexus's external endpoint
pub struct ExternalEndpointsWatcher {
    datastore: Arc<DataStore>,
    last: Option<ExternalEndpoints>,
    tx: watch::Sender<Option<ExternalEndpoints>>,
    rx: watch::Receiver<Option<ExternalEndpoints>>,
}

impl ExternalEndpointsWatcher {
    pub fn new(datastore: Arc<DataStore>) -> ExternalEndpointsWatcher {
        let (tx, rx) = watch::channel(None);
        ExternalEndpointsWatcher { datastore, last: None, tx, rx }
    }

    /// Exposes the latest set of TLS certificates
    ///
    /// You can use the returned [`watch::Receiver`] to look at the latest
    /// configuration or to be notified when it changes.
    pub fn watcher(&self) -> watch::Receiver<Option<ExternalEndpoints>> {
        self.rx.clone()
    }
}

impl BackgroundTask for ExternalEndpointsWatcher {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;

            let result = read_all_endpoints(&self.datastore, opctx).await;

            if let Err(error) = result {
                warn!(
                    &log,
                    "failed to read Silo/DNS/TLS configuration";
                    "error" => format!("{:#}", error)
                );
                return json!({
                    "error":
                        format!(
                            "failed to read Silo/DNS/TLS configuration: \
                            {:#}",
                            error
                        )
                });
            }

            let new_config = result.unwrap();
            let rv =
                serde_json::to_value(&new_config).unwrap_or_else(|error| {
                    json!({
                        "error":
                            format!(
                                "failed to serialize final value: {:#}",
                                error
                            )
                    })
                });

            match &self.last {
                None => {
                    info!(
                        &log,
                        "found Silo/DNS/TLS config (initial)";
                        "config" => ?new_config,
                    );
                    self.last = Some(new_config.clone());
                    self.tx.send_replace(Some(new_config));
                }

                Some(old) => {
                    if *old == new_config {
                        debug!(
                            &log,
                            "found Silo/DNS/TLS config (no change)";
                            "config" => ?new_config,
                        );
                    } else {
                        info!(
                            &log,
                            "found Silo/DNS/TLS config (changed)";
                            "config" => ?new_config,
                        );
                        self.last = Some(new_config.clone());
                        self.tx.send_replace(Some(new_config));
                    }
                }
            };

            rv
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use crate::app::background::common::BackgroundTask;
    use crate::app::background::external_endpoints::ExternalEndpointsWatcher;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
    use nexus_test_utils::resource_helpers::create_silo;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::shared::SiloIdentityMode;
    use nexus_types::identity::Resource;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_basic(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Verify the initial state.
        let mut task = ExternalEndpointsWatcher::new(datastore.clone());
        let watcher = task.watcher();
        assert!(watcher.borrow().is_none());

        // The datastore from the ControlPlaneTestContext is initialized with
        // two Silos: the built-in Silo and the recovery Silo.
        let recovery_silo_dns_name = format!(
            "{}.sys.{}",
            cptestctx.silo_name.as_str(),
            cptestctx.external_dns_zone_name
        );
        let builtin_silo_dns_name = format!(
            "{}.sys.{}",
            DEFAULT_SILO.identity().name.as_str(),
            cptestctx.external_dns_zone_name,
        );
        let _ = task.activate(&opctx).await;
        let initial_state_raw = watcher.borrow();
        let initial_state = initial_state_raw.as_ref().unwrap();
        assert!(initial_state.has_domain(recovery_silo_dns_name.as_str()));
        assert!(initial_state.has_domain(builtin_silo_dns_name.as_str()));
        // There are no other Silos.
        assert_eq!(initial_state.ndomains(), 2);
        // Neither of these will have a valid certificate in this configuration.
        assert_eq!(initial_state.nwarnings(), 2);
        drop(initial_state_raw);

        // If we create another Silo, we should see that one, too.
        let new_silo_name = "test-silo";
        create_silo(
            &cptestctx.external_client,
            new_silo_name,
            false,
            SiloIdentityMode::LocalOnly,
        )
        .await;
        let new_silo_dns_name = format!(
            "{}.sys.{}",
            new_silo_name, cptestctx.external_dns_zone_name
        );
        let _ = task.activate(&opctx).await;
        let new_state_raw = watcher.borrow();
        let new_state = new_state_raw.as_ref().unwrap();
        assert!(new_state.has_domain(recovery_silo_dns_name.as_str()));
        assert!(new_state.has_domain(builtin_silo_dns_name.as_str()));
        assert!(new_state.has_domain(new_silo_dns_name.as_str()));
        // There are no other Silos.
        assert_eq!(new_state.ndomains(), 3);
        // None of these will have a valid certificate in this configuration.
        assert_eq!(new_state.nwarnings(), 3);

        // That's it.  We're not testing all possible cases.  That's done with
        // unit tests for the underlying function.  We're just testing that the
        // background task reports updated state when the underlying state
        // changes.
    }
}
