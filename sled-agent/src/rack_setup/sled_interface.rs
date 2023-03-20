// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Stubs for the sled agent interface.
//!
//! Defined to make testing easier.

use crate::params::{DatasetEnsureBody, ServiceZoneRequest};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use sled_agent_client::{
    types as SledAgentTypes, Client as SledAgentClient, Error as SledAgentError,
};
use slog::Logger;
use std::net::SocketAddrV6;
use uuid::Uuid;

// TODO(https://github.com/oxidecomputer/omicron/issues/732): Remove.
// when Nexus provisions Crucible.
const MINIMUM_U2_ZPOOL_COUNT: usize = 3;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error making HTTP request to Sled Agent: {0}")]
    SledApi(#[from] SledAgentError<SledAgentTypes::Error>),

    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Error initializing sled via sled-agent: {0}")]
    SledInitialization(String),
}

#[async_trait::async_trait]
pub(crate) trait SledInterface: Send + Sync + 'static {
    /// Gets zpool UUIDs from U.2 devices on the sled.
    async fn get_u2_zpools(
        &self,
        log: &Logger,
        address: SocketAddrV6,
    ) -> Result<Vec<Uuid>, Error>;

    async fn initialize_datasets(
        &self,
        log: &Logger,
        sled_address: SocketAddrV6,
        datasets: &Vec<DatasetEnsureBody>,
    ) -> Result<(), Error>;

    async fn initialize_services(
        &self,
        log: &Logger,
        sled_address: SocketAddrV6,
        services: &Vec<ServiceZoneRequest>,
    ) -> Result<(), Error>;
}

pub(crate) struct RealSledAccess {}

#[async_trait::async_trait]
impl SledInterface for RealSledAccess {
    async fn get_u2_zpools(
        &self,
        log: &Logger,
        address: SocketAddrV6,
    ) -> Result<Vec<Uuid>, Error> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .map_err(Error::HttpClient)?;
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", address),
            client,
            log.new(o!("SledAgentClient" => address.to_string())),
        );

        let get_u2_zpools = || async {
            let zpools: Vec<Uuid> = client
                .zpools_get()
                .await
                .map(|response| {
                    response
                        .into_inner()
                        .into_iter()
                        .filter_map(|zpool| match zpool.disk_type {
                            SledAgentTypes::DiskType::U2 => Some(zpool.id),
                            SledAgentTypes::DiskType::M2 => None,
                        })
                        .collect()
                })
                .map_err(|err| BackoffError::transient(Error::SledApi(err)))?;

            if zpools.len() < MINIMUM_U2_ZPOOL_COUNT {
                return Err(BackoffError::transient(
                    Error::SledInitialization("Awaiting zpools".to_string()),
                ));
            }

            Ok(zpools)
        };
        let log_failure = |error, _| {
            warn!(log, "failed to get zpools"; "error" => ?error);
        };
        let u2_zpools = retry_notify(
            retry_policy_internal_service_aggressive(),
            get_u2_zpools,
            log_failure,
        )
        .await?;

        Ok(u2_zpools)
    }

    async fn initialize_datasets(
        &self,
        log: &Logger,
        sled_address: SocketAddrV6,
        datasets: &Vec<DatasetEnsureBody>,
    ) -> Result<(), Error> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", sled_address),
            client,
            log.new(o!("SledAgentClient" => sled_address.to_string())),
        );

        info!(log, "sending dataset requests...");
        for dataset in datasets {
            let filesystem_put = || async {
                info!(log, "creating new filesystem: {:?}", dataset);
                client
                    .filesystem_put(&dataset.clone().into())
                    .await
                    .map_err(BackoffError::transient)?;
                Ok::<(), BackoffError<SledAgentError<SledAgentTypes::Error>>>(())
            };
            let log_failure = |error, _| {
                warn!(log, "failed to create filesystem"; "error" => ?error);
            };
            retry_notify(
                retry_policy_internal_service_aggressive(),
                filesystem_put,
                log_failure,
            )
            .await?;
        }
        Ok(())
    }

    async fn initialize_services(
        &self,
        log: &Logger,
        sled_address: SocketAddrV6,
        services: &Vec<ServiceZoneRequest>,
    ) -> Result<(), Error> {
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;
        let client = SledAgentClient::new_with_client(
            &format!("http://{}", sled_address),
            client,
            log.new(o!("SledAgentClient" => sled_address.to_string())),
        );

        info!(log, "sending service requests...");
        let services_put = || async {
            info!(log, "initializing sled services: {:?}", services);
            client
                .services_put(&SledAgentTypes::ServiceEnsureBody {
                    services: services
                        .iter()
                        .map(|s| s.clone().into())
                        .collect(),
                })
                .await
                .map_err(BackoffError::transient)?;
            Ok::<(), BackoffError<SledAgentError<SledAgentTypes::Error>>>(())
        };
        let log_failure = |error, _| {
            warn!(log, "failed to initialize services"; "error" => ?error);
        };
        retry_notify(
            retry_policy_internal_service_aggressive(),
            services_put,
            log_failure,
        )
        .await?;
        Ok(())
    }
}
