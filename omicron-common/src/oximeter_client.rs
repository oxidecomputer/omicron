//! Interface for API requests to an Oximeter metric collection server
// Copyright 2021 Oxide Computer Company

use std::net::SocketAddr;

use http::Method;
use hyper::Body;
use slog::Logger;
use uuid::Uuid;

use crate::api::ApiError;
use crate::api::ProducerEndpoint;
use crate::http_client::HttpClient;

/// Client of an oximeter server
pub struct Client {
    /// The oximeter server's unique ID
    pub id: Uuid,

    /// oximeter server's address
    pub address: SocketAddr,
    client: HttpClient,
}

impl Client {
    pub fn new(id: Uuid, address: SocketAddr, log: Logger) -> Client {
        Client {
            id,
            address,
            client: HttpClient::new("oximeter", address, log),
        }
    }

    /// Register the metric producer server described in `info` with oximeter.
    pub async fn register_producer(
        &self,
        info: &ProducerEndpoint,
    ) -> Result<(), ApiError> {
        self.client
            .request(
                Method::POST,
                "/producers",
                Body::from(serde_json::to_string(info).unwrap()),
            )
            .await?;
        Ok(())
    }
}
