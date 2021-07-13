/*!
 * Interface for making API requests to the Oxide control plane at large
 * from within the control plane
 *
 * This should be replaced with a client generated from the OpenAPI spec
 * generated by the server.
 */

use crate::api::DiskRuntimeState;
use crate::api::Error;
use crate::api::InstanceRuntimeState;
use crate::api::SledAgentStartupInfo;
use crate::http_client::HttpClient;
use http::Method;
use hyper::Body;
use slog::Logger;
use std::net::SocketAddr;
use uuid::Uuid;

/** Client for a nexus instance */
pub struct Client {
    /** underlying HTTP client */
    client: HttpClient,
}

impl Client {
    /**
     * Create a new nexus client to make requests to the Nexus instance at
     * `server_addr`.
     */
    pub fn new(server_addr: SocketAddr, log: Logger) -> Client {
        Client { client: HttpClient::new("nexus", server_addr, log) }
    }

    /**
     * Publish information about a sled agent's startup.
     */
    pub async fn notify_sled_agent_online(
        &self,
        id: Uuid,
        info: SledAgentStartupInfo,
    ) -> Result<(), Error> {
        let path = format!("/sled_agents/{}", id);
        let body = Body::from(serde_json::to_string(&info).unwrap());
        self.client.request(Method::POST, path.as_str(), body).await.map(|_| ())
    }

    /**
     * Publish an updated runtime state for an Instance.
     */
    pub async fn notify_instance_updated(
        &self,
        id: &Uuid,
        new_runtime_state: &InstanceRuntimeState,
    ) -> Result<(), Error> {
        let path = format!("/instances/{}", id);
        let body =
            Body::from(serde_json::to_string(new_runtime_state).unwrap());
        self.client.request(Method::PUT, path.as_str(), body).await.map(|_| ())
    }

    /**
     * Publish an updated runtime state for a Disk.
     */
    pub async fn notify_disk_updated(
        &self,
        id: &Uuid,
        new_state: &DiskRuntimeState,
    ) -> Result<(), Error> {
        let path = format!("/disks/{}", id);
        let body = Body::from(serde_json::to_string(new_state).unwrap());
        self.client.request(Method::PUT, path.as_str(), body).await.map(|_| ())
    }
}
