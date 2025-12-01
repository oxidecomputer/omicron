// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent implementation

use crate::nexus::NexusClient;
use crate::sim::simulatable::Simulatable;
use async_trait::async_trait;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::ResourceType;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerKind;
use oximeter_producer::LogConfig;
use oximeter_producer::Server as ProducerServer;
use sled_agent_types::disk::DiskStateRequested;
use std::net::{Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::common::disk::{Action as DiskAction, DiskStates};

// Oximeter timeseries names are derived based on the precise names of structs,
// so we shove this in a module to more liberally use arbitrary names (like
// "Read").
mod producers {
    use super::*;
    use oximeter::{
        Metric, Target,
        types::{Cumulative, Sample},
    };

    #[derive(Debug, Clone, Target)]
    pub struct CrucibleUpstairs {
        pub upstairs_uuid: Uuid,
    }

    // TODO: It would be a lot nicer if we could just depend on the Crucible
    // types here directly, rather than recreate them. However, in doing so,
    // we bump into issues with the "Metric" trait - the implementation of
    // oximeter::Producer claims that "Metric" is not implemented for the
    // Crucible-defined structure, even though it is derived.
    // I suspect this is due to version skew between Crucible vs Omicron's copy
    // of Oximeter.

    #[derive(Debug, Default, Copy, Clone, Metric)]
    struct Activated {
        /// Count of times this upstairs has activated.
        #[datum]
        count: Cumulative<i64>,
    }
    #[derive(Debug, Default, Copy, Clone, Metric)]
    struct Write {
        /// Count of region writes this upstairs has completed
        #[datum]
        count: Cumulative<i64>,
    }
    #[derive(Debug, Default, Copy, Clone, Metric)]
    struct WriteBytes {
        /// Count of bytes written
        #[datum]
        count: Cumulative<i64>,
    }
    #[derive(Debug, Default, Copy, Clone, Metric)]
    struct Read {
        /// Count of region reads this upstairs has completed
        #[datum]
        count: Cumulative<i64>,
    }
    #[derive(Debug, Default, Copy, Clone, Metric)]
    struct ReadBytes {
        /// Count of bytes read
        #[datum]
        count: Cumulative<i64>,
    }
    #[derive(Debug, Default, Copy, Clone, Metric)]
    struct Flush {
        /// Count of region flushes this upstairs has completed
        #[datum]
        count: Cumulative<i64>,
    }

    #[derive(Debug, Clone)]
    pub struct DiskProducer {
        target: CrucibleUpstairs,
        activated_count: Activated,
        write_count: Write,
        write_bytes: WriteBytes,
        read_count: Read,
        read_bytes: ReadBytes,
        flush_count: Flush,
    }

    impl DiskProducer {
        pub fn new(id: Uuid) -> Self {
            Self {
                target: CrucibleUpstairs { upstairs_uuid: id },
                activated_count: Default::default(),
                write_count: Default::default(),
                write_bytes: Default::default(),
                read_count: Default::default(),
                read_bytes: Default::default(),
                flush_count: Default::default(),
            }
        }
    }

    impl oximeter::Producer for DiskProducer {
        fn produce(
            &mut self,
        ) -> Result<
            Box<(dyn Iterator<Item = Sample> + 'static)>,
            oximeter::MetricsError,
        > {
            let samples = vec![
                Sample::new(&self.target, &self.activated_count)?,
                Sample::new(&self.target, &self.write_count)?,
                Sample::new(&self.target, &self.write_bytes)?,
                Sample::new(&self.target, &self.read_count)?,
                Sample::new(&self.target, &self.read_bytes)?,
                Sample::new(&self.target, &self.flush_count)?,
            ];

            *self.activated_count.datum_mut() += 1;
            *self.write_count.datum_mut() += 1;
            *self.write_bytes.datum_mut() += 1;
            *self.read_count.datum_mut() += 1;
            *self.read_bytes.datum_mut() += 1;
            *self.flush_count.datum_mut() += 1;

            Ok(Box::new(samples.into_iter()))
        }
    }
}

/// Simulated Disk (network block device), as created by the external Oxide API
///
/// See `Simulatable` for how this works.
pub struct SimDisk {
    state: DiskStates,
    producer: Option<ProducerServer>,
}

// "producer" doesn't implement Debug, so we can't derive it on SimDisk.
impl std::fmt::Debug for SimDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimDisk").field("state", &self.state).finish()
    }
}

impl SimDisk {
    pub async fn start_producer_server(
        &mut self,
        nexus_address: SocketAddr,
        id: Uuid,
    ) -> Result<(), String> {
        // Set up a producer server.
        //
        // This listens on any available port, and the server internally updates this to the actual
        // bound port of the Dropshot HTTP server.
        let producer_address = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0);
        let server_info = ProducerEndpoint {
            id,
            kind: ProducerKind::SledAgent,
            address: producer_address,
            interval: Duration::from_millis(200),
        };
        let config = oximeter_producer::Config {
            server_info,
            registration_address: Some(nexus_address),
            default_request_body_max_bytes: 2048,
            log: LogConfig::Config(ConfigLogging::StderrTerminal {
                level: ConfigLoggingLevel::Error,
            }),
        };
        let server =
            ProducerServer::start(&config).map_err(|e| e.to_string())?;
        let producer = producers::DiskProducer::new(id);
        server
            .registry()
            .register_producer(producer)
            .map_err(|e| e.to_string())?;
        self.producer.replace(server);
        Ok(())
    }
}

#[async_trait]
impl Simulatable for SimDisk {
    type CurrentState = DiskRuntimeState;
    type RequestedState = DiskStateRequested;
    type ProducerArgs = (std::net::SocketAddr, Uuid);
    type Action = DiskAction;

    fn new(current: DiskRuntimeState) -> Self {
        SimDisk { state: DiskStates::new(current), producer: None }
    }

    async fn set_producer(
        &mut self,
        args: Self::ProducerArgs,
    ) -> Result<(), Error> {
        self.start_producer_server(args.0, args.1).await.map_err(|e| {
            Error::internal_error(&format!("Setting producer server: {e}"))
        })?;
        Ok(())
    }

    fn request_transition(
        &mut self,
        target: &DiskStateRequested,
    ) -> Result<Option<DiskAction>, Error> {
        self.state.request_transition(target)
    }

    fn execute_desired_transition(&mut self) -> Option<DiskAction> {
        if let Some(desired) = self.state.desired() {
            let observed = match desired {
                DiskStateRequested::Attached(uuid) => {
                    DiskState::Attached(*uuid)
                }
                DiskStateRequested::Detached => DiskState::Detached,
                DiskStateRequested::Destroyed => DiskState::Destroyed,
                DiskStateRequested::Faulted => DiskState::Faulted,
            };
            self.state.observe_transition(&observed)
        } else {
            None
        }
    }

    fn generation(&self) -> Generation {
        self.state.current().generation
    }

    fn current(&self) -> Self::CurrentState {
        self.state.current().clone()
    }

    fn desired(&self) -> Option<Self::RequestedState> {
        self.state.desired().clone()
    }

    fn ready_to_destroy(&self) -> bool {
        DiskState::Destroyed == self.current().disk_state
    }

    async fn notify(
        nexus_client: &Arc<NexusClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), Error> {
        nexus_client
            .cpapi_disks_put(
                id,
                &nexus_client::types::DiskRuntimeState::from(current),
            )
            .await
            .map(|_| ())
            .map_err(Error::from)
    }

    fn resource_type() -> ResourceType {
        ResourceType::Disk
    }
}
