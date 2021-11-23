//! Interface for making API requests to a Sled Agent's Bootstrap API.
//! This is not its own crate because the only intended consumer is other
//! bootstrap peers within the cluster.

use omicron_common::generate_logging_api;

generate_logging_api!("../openapi/bootstrap-agent.json");
