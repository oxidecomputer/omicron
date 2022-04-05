// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack Setup Service implementation

use super::config::SetupServiceConfig as Config;
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use slog::Logger;
use thiserror::Error;

/// Describes errors which may occur while operating the setup service.
#[derive(Error, Debug)]
pub enum SetupServiceError {
    #[error("Error accessing filesystem: {0}")]
    Io(#[from] std::io::Error),

    #[error("Error making HTTP request to Nexus: {0}")]
    NexusApi(#[from] nexus_client::Error<nexus_client::types::Error>),

    #[error("Error making HTTP request to Sled Agent: {0}")]
    SledApi(#[from] sled_agent_client::Error<sled_agent_client::types::Error>),

    #[error("Cannot deserialize TOML file")]
    Toml(#[from] toml::de::Error),

    #[error("Configuration changed")]
    Configuration,
}

/// The interface to the Rack Setup Service.
pub struct Service {
    handle: tokio::task::JoinHandle<Result<(), SetupServiceError>>,
}

impl Service {
    pub fn new(log: Logger, config: Config) -> Self {
        let handle = tokio::task::spawn(async move {
            let svc = ServiceInner::new(log);
            svc.inject_rack_setup_requests(&config).await
        });

        Service { handle }
    }

    /// Awaits the completion of the RSS service.
    pub async fn join(self) -> Result<(), SetupServiceError> {
        self.handle.await.expect("Rack Setup Service Task panicked")
    }
}

/// The implementation of the Rack Setup Service.
struct ServiceInner {
    log: Logger,
}

impl ServiceInner {
    pub fn new(log: Logger) -> Self {
        ServiceInner { log }
    }

    // In lieu of having an operator send requests to all sleds via an
    // initialization service, the sled-agent configuration may allow for the
    // automated injection of setup requests from a sled.
    async fn inject_rack_setup_requests(
        &self,
        config: &Config,
    ) -> Result<(), SetupServiceError> {
        info!(self.log, "Injecting RSS configuration: {:#?}", config);

        let serialized_config = toml::Value::try_from(&config)
            .expect("Cannot serialize configuration");
        let config_str = toml::to_string(&serialized_config)
            .expect("Cannot turn config to string");

        // First, check if this request has previously been made.
        //
        // Normally, the rack setup service is run with a human-in-the-loop,
        // but with this automated injection, we need a way to determine the
        // (destructive) initialization has occurred.
        //
        // We do this by storing the configuration at "rss_config_path"
        // after successfully performing initialization.
        let rss_config_path = std::path::Path::new(crate::OMICRON_CONFIG_PATH)
            .join("config-rss.toml");
        if rss_config_path.exists() {
            info!(
                self.log,
                "RSS configuration already exists at {}",
                rss_config_path.to_string_lossy()
            );
            let old_config: Config = toml::from_str(
                &tokio::fs::read_to_string(&rss_config_path).await?,
            )?;
            if &old_config == config {
                info!(
                    self.log,
                    "RSS config already applied from: {}",
                    rss_config_path.to_string_lossy()
                );
                return Ok(());
            }

            // TODO(https://github.com/oxidecomputer/omicron/issues/724):
            // We could potentially handle this case by deleting all
            // partitions (in preparation for applying the new
            // configuration), but at the moment it's an error.
            warn!(
                self.log,
                "Rack Setup Service Config was already applied, but has changed.
                This means that you may have partitions set up on this sled, but they
                may not match the ones requested by the supplied configuration.\n
                To re-initialize this sled:
                   - Disable all Oxide services
                   - Delete all partitions within the attached zpool
                   - Delete the configuration file ({})
                   - Restart the sled agent",
                rss_config_path.to_string_lossy()
            );
            return Err(SetupServiceError::Configuration);
        } else {
            info!(
                self.log,
                "No RSS configuration found at {}",
                rss_config_path.to_string_lossy()
            );
        }

        // Issue the dataset initialization requests to all sleds.
        futures::future::join_all(
            config.requests.iter().map(|request| async move {
                info!(self.log, "observing request: {:#?}", request);
                let dur = std::time::Duration::from_secs(60);
                let client = reqwest::ClientBuilder::new()
                    .connect_timeout(dur)
                    .timeout(dur)
                    .build()
                    .map_err(|e| nexus_client::Error::<nexus_client::types::Error>::from(e))?;
                let client = sled_agent_client::Client::new_with_client(
                    &format!("http://{}", request.sled_address),
                    client,
                    self.log.new(o!("SledAgentClient" => request.sled_address)),
                );

                info!(self.log, "sending partition requests...");
                for partition in &request.partitions {
                    let filesystem_put = || async {
                        info!(self.log, "creating new filesystem: {:?}", partition);
                        client.filesystem_put(&partition.clone().into())
                            .await
                            .map_err(BackoffError::transient)?;
                        Ok::<
                            (),
                            BackoffError<
                                sled_agent_client::Error<sled_agent_client::types::Error>,
                            >,
                        >(())
                    };
                    let log_failure = |error, _| {
                        warn!(self.log, "failed to create filesystem"; "error" => ?error);
                    };
                    retry_notify(
                        internal_service_policy(),
                        filesystem_put,
                        log_failure,
                    ).await?;
                }
                Ok(())
            })
        ).await.into_iter().collect::<Result<Vec<()>, SetupServiceError>>()?;

        // Issue service initialization requests.
        //
        // Note that this must happen *after* the partition initialization,
        // to ensure that CockroachDB has been initialized before Nexus
        // starts.
        futures::future::join_all(
            config.requests.iter().map(|request| async move {
                info!(self.log, "observing request: {:#?}", request);
                let dur = std::time::Duration::from_secs(60);
                let client = reqwest::ClientBuilder::new()
                    .connect_timeout(dur)
                    .timeout(dur)
                    .build()
                    .map_err(|e| nexus_client::Error::<nexus_client::types::Error>::from(e))?;
                let client = sled_agent_client::Client::new_with_client(
                    &format!("http://{}", request.sled_address),
                    client,
                    self.log.new(o!("SledAgentClient" => request.sled_address)),
                );

                info!(self.log, "sending service requests...");
                let services_put = || async {
                    info!(self.log, "initializing sled services: {:?}", request.services);
                    client.services_put(
                        &sled_agent_client::types::ServiceEnsureBody {
                            services: request.services.iter().map(|s| s.clone().into()).collect()
                        })
                        .await
                        .map_err(BackoffError::transient)?;
                    Ok::<
                        (),
                        BackoffError<
                            sled_agent_client::Error<sled_agent_client::types::Error>,
                        >,
                    >(())
                };
                let log_failure = |error, _| {
                    warn!(self.log, "failed to initialize services"; "error" => ?error);
                };
                retry_notify(
                    internal_service_policy(),
                    services_put,
                    log_failure,
                ).await?;
                Ok::<(), SetupServiceError>(())
            })
        ).await.into_iter().collect::<Result<Vec<()>, SetupServiceError>>()?;

        // Finally, make sure the configuration is saved so we don't inject
        // the requests on the next iteration.
        tokio::fs::write(rss_config_path, config_str).await?;
        Ok(())
    }
}
