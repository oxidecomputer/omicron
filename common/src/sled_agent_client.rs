/*!
 * Interface for making API requests to a Sled Agent
 */

use async_trait::async_trait;
use std::convert::TryFrom;
use uuid::Uuid;

generate_logging_api!("../openapi/sled-agent.json");

impl From<anyhow::Error> for crate::api::external::Error {
    fn from(e: anyhow::Error) -> Self {
        // TODO this needs to be updated when progenitor gets better error types.
        if let Some(ee) = e.downcast_ref::<reqwest::Error>() {
            if let Some(s) = ee.status() {
                if s.is_client_error() {
                    return crate::api::external::Error::InvalidRequest {
                        message: e.to_string(),
                    };
                }
            }
        }
        crate::api::external::Error::internal_error(&e.to_string())
    }
}

impl From<crate::api::internal::nexus::InstanceRuntimeState>
    for types::InstanceRuntimeState
{
    fn from(s: crate::api::internal::nexus::InstanceRuntimeState) -> Self {
        Self {
            run_state: s.run_state.into(),
            sled_uuid: s.sled_uuid,
            ncpus: s.ncpus.into(),
            memory: s.memory.into(),
            hostname: s.hostname,
            gen: s.gen.into(),
            time_updated: s.time_updated,
        }
    }
}

impl From<crate::api::external::InstanceState> for types::InstanceState {
    fn from(s: crate::api::external::InstanceState) -> Self {
        match s {
            crate::api::external::InstanceState::Creating => Self::Creating,
            crate::api::external::InstanceState::Starting => Self::Starting,
            crate::api::external::InstanceState::Running => Self::Running,
            crate::api::external::InstanceState::Stopping => Self::Stopping,
            crate::api::external::InstanceState::Stopped => Self::Stopped,
            crate::api::external::InstanceState::Rebooting => Self::Rebooting,
            crate::api::external::InstanceState::Repairing => Self::Repairing,
            crate::api::external::InstanceState::Failed => Self::Failed,
            crate::api::external::InstanceState::Destroyed => Self::Destroyed,
        }
    }
}

impl From<crate::api::external::InstanceCpuCount> for types::InstanceCpuCount {
    fn from(s: crate::api::external::InstanceCpuCount) -> Self {
        Self(s.0)
    }
}

impl From<crate::api::external::ByteCount> for types::ByteCount {
    fn from(s: crate::api::external::ByteCount) -> Self {
        Self(s.to_bytes())
    }
}

impl From<crate::api::external::Generation> for types::Generation {
    fn from(s: crate::api::external::Generation) -> Self {
        Self(i64::from(&s) as u64)
    }
}

impl From<crate::api::internal::sled_agent::InstanceRuntimeStateRequested>
    for types::InstanceRuntimeStateRequested
{
    fn from(
        s: crate::api::internal::sled_agent::InstanceRuntimeStateRequested,
    ) -> Self {
        Self { run_state: s.run_state.into() }
    }
}

impl From<crate::api::internal::sled_agent::InstanceStateRequested>
    for types::InstanceStateRequested
{
    fn from(
        s: crate::api::internal::sled_agent::InstanceStateRequested,
    ) -> Self {
        match s {
            crate::api::internal::sled_agent::InstanceStateRequested::Running => Self::Running,
            crate::api::internal::sled_agent::InstanceStateRequested::Stopped => Self::Stopped,
            crate::api::internal::sled_agent::InstanceStateRequested::Reboot => Self::Reboot,
            crate::api::internal::sled_agent::InstanceStateRequested::Destroyed => Self::Destroyed,
        }
    }
}

impl From<types::InstanceRuntimeState>
    for crate::api::internal::nexus::InstanceRuntimeState
{
    fn from(s: types::InstanceRuntimeState) -> Self {
        Self {
            run_state: s.run_state.into(),
            sled_uuid: s.sled_uuid,
            ncpus: s.ncpus.into(),
            memory: s.memory.into(),
            hostname: s.hostname,
            gen: s.gen.into(),
            time_updated: s.time_updated,
        }
    }
}

impl From<types::InstanceState> for crate::api::external::InstanceState {
    fn from(s: types::InstanceState) -> Self {
        match s {
            types::InstanceState::Creating => Self::Creating,
            types::InstanceState::Starting => Self::Starting,
            types::InstanceState::Running => Self::Running,
            types::InstanceState::Stopping => Self::Stopping,
            types::InstanceState::Stopped => Self::Stopped,
            types::InstanceState::Rebooting => Self::Rebooting,
            types::InstanceState::Repairing => Self::Repairing,
            types::InstanceState::Failed => Self::Failed,
            types::InstanceState::Destroyed => Self::Destroyed,
        }
    }
}

impl From<types::InstanceCpuCount> for crate::api::external::InstanceCpuCount {
    fn from(s: types::InstanceCpuCount) -> Self {
        Self(s.0)
    }
}

impl From<types::ByteCount> for crate::api::external::ByteCount {
    fn from(s: types::ByteCount) -> Self {
        Self::try_from(s.0).unwrap()
    }
}

impl From<types::Generation> for crate::api::external::Generation {
    fn from(s: types::Generation) -> Self {
        Self::try_from(s.0 as i64).unwrap()
    }
}

impl From<crate::api::internal::nexus::DiskRuntimeState>
    for types::DiskRuntimeState
{
    fn from(s: crate::api::internal::nexus::DiskRuntimeState) -> Self {
        Self {
            disk_state: s.disk_state.into(),
            gen: s.gen.into(),
            time_updated: s.time_updated,
        }
    }
}

impl From<crate::api::external::DiskState> for types::DiskState {
    fn from(s: crate::api::external::DiskState) -> Self {
        match s {
            crate::api::external::DiskState::Creating => Self::Creating,
            crate::api::external::DiskState::Detached => Self::Detached,
            crate::api::external::DiskState::Attaching(u) => Self::Attaching(u),
            crate::api::external::DiskState::Attached(u) => Self::Attached(u),
            crate::api::external::DiskState::Detaching(u) => Self::Detaching(u),
            crate::api::external::DiskState::Destroyed => Self::Destroyed,
            crate::api::external::DiskState::Faulted => Self::Faulted,
        }
    }
}

impl From<types::DiskRuntimeState>
    for crate::api::internal::nexus::DiskRuntimeState
{
    fn from(s: types::DiskRuntimeState) -> Self {
        Self {
            disk_state: s.disk_state.into(),
            gen: s.gen.into(),
            time_updated: s.time_updated,
        }
    }
}

impl From<types::DiskState> for crate::api::external::DiskState {
    fn from(s: types::DiskState) -> Self {
        match s {
            types::DiskState::Creating => Self::Creating,
            types::DiskState::Detached => Self::Detached,
            types::DiskState::Attaching(u) => Self::Attaching(u),
            types::DiskState::Attached(u) => Self::Attached(u),
            types::DiskState::Detaching(u) => Self::Detaching(u),
            types::DiskState::Destroyed => Self::Destroyed,
            types::DiskState::Faulted => Self::Faulted,
        }
    }
}

impl From<crate::api::internal::sled_agent::DiskStateRequested>
    for types::DiskStateRequested
{
    fn from(s: crate::api::internal::sled_agent::DiskStateRequested) -> Self {
        match s {
            crate::api::internal::sled_agent::DiskStateRequested::Detached => {
                Self::Detached
            }
            crate::api::internal::sled_agent::DiskStateRequested::Attached(
                u,
            ) => Self::Attached(u),
            crate::api::internal::sled_agent::DiskStateRequested::Destroyed => {
                Self::Destroyed
            }
            crate::api::internal::sled_agent::DiskStateRequested::Faulted => {
                Self::Faulted
            }
        }
    }
}

impl From<crate::api::internal::sled_agent::InstanceHardware>
    for types::InstanceHardware
{
    fn from(s: crate::api::internal::sled_agent::InstanceHardware) -> Self {
        Self {
            nics: s.nics.iter().map(Into::into).collect(),
            runtime: s.runtime.into(),
        }
    }
}
impl From<&crate::api::external::NetworkInterface> for types::NetworkInterface {
    fn from(s: &crate::api::external::NetworkInterface) -> Self {
        Self {
            identity: (&s.identity).into(),
            ip: s.ip.to_string(),
            mac: s.mac.into(),
            subnet_id: s.subnet_id,
            vpc_id: s.vpc_id,
        }
    }
}

impl From<&crate::api::external::IdentityMetadata> for types::IdentityMetadata {
    fn from(s: &crate::api::external::IdentityMetadata) -> Self {
        Self {
            description: s.description.clone(),
            id: s.id,
            name: (&s.name).into(),
            time_created: s.time_created,
            time_modified: s.time_modified,
        }
    }
}

impl From<&crate::api::external::Name> for types::Name {
    fn from(s: &crate::api::external::Name) -> Self {
        Self(<&str>::from(s).to_string())
    }
}

impl From<crate::api::external::MacAddr> for types::MacAddr {
    fn from(s: crate::api::external::MacAddr) -> Self {
        Self(s.0.to_string())
    }
}

//impl From<DiskRuntimeState> for DiskRuntimeState {}

/*
/** Client for a sled agent */
pub struct Client {
    /**
     * sled agent's unique id (used by callers to track `Client`
     * objects)
     */
    pub id: Uuid,
    /** sled agent server address */
    pub service_address: SocketAddr,
    /** underlying HTTP client */
    client: HttpClient,
}

impl Client {
    /**
     * Create a new sled agent client to make requests to the sled agent running
     * at `server_addr`.
     */
    pub fn new(id: &Uuid, server_addr: SocketAddr, log: Logger) -> Client {
        Client {
            id: *id,
            service_address: server_addr,
            client: HttpClient::new("sled agent", server_addr, log),
        }
    }

    /**
     * Idempotently ensures that the given API Instance exists on this server in
     * the given runtime state (described by `target`).
     */
    pub async fn instance_ensure(
        self: &Arc<Self>,
        instance_id: Uuid,
        initial: InstanceHardware,
        target: InstanceRuntimeStateRequested,
    ) -> Result<InstanceRuntimeState, Error> {
        let path = format!("/instances/{}", instance_id);
        let body = Body::from(
            serde_json::to_string(&InstanceEnsureBody { initial, target })
                .unwrap(),
        );
        let mut response =
            self.client.request(Method::PUT, path.as_str(), body).await?;
        /* TODO-robustness handle 300-level? */
        assert!(response.status().is_success());
        let value = self
            .client
            .read_json::<InstanceRuntimeState>(
                &self.client.error_message_base(&Method::PUT, path.as_str()),
                &mut response,
            )
            .await?;
        Ok(value)
    }

    /**
     * Idempotently ensures that the given API Disk is attached (or not) as
     * specified.
     */
    pub async fn disk_ensure(
        self: &Arc<Self>,
        disk_id: Uuid,
        initial_runtime: DiskRuntimeState,
        target: DiskStateRequested,
    ) -> Result<DiskRuntimeState, Error> {
        let path = format!("/disks/{}", disk_id);
        let body = Body::from(
            serde_json::to_string(&DiskEnsureBody { initial_runtime, target })
                .unwrap(),
        );
        let mut response =
            self.client.request(Method::PUT, path.as_str(), body).await?;
        /* TODO-robustness handle 300-level? */
        assert!(response.status().is_success());
        let value = self
            .client
            .read_json::<DiskRuntimeState>(
                &self.client.error_message_base(&Method::PUT, path.as_str()),
                &mut response,
            )
            .await?;
        Ok(value)
    }
}
*/

/**
 * Exposes additional [`Client`] interfaces for use by the test suite. These
 * are bonus endpoints, not generated in the real client.
 */
#[async_trait]
pub trait TestInterfaces {
    async fn instance_finish_transition(&self, id: Uuid);
    async fn disk_finish_transition(&self, id: Uuid);
}

#[async_trait]
impl TestInterfaces for Client {
    async fn instance_finish_transition(&self, id: Uuid) {
        let baseurl = self.baseurl();
        let client = self.client();
        let url = format!("{}/instances/{}/poke", baseurl, id);
        client
            .post(url)
            .send()
            .await
            .expect("instance_finish_transition() failed unexpectedly");
    }

    async fn disk_finish_transition(&self, id: Uuid) {
        let baseurl = self.baseurl();
        let client = self.client();
        let url = format!("{}/disks/{}/poke", baseurl, id);
        client
            .post(url)
            .send()
            .await
            .expect("disk_finish_transition() failed unexpectedly");
    }
}
