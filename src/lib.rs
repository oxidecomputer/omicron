/*!
 * Oxide control plane
 */

mod api_error;
pub mod api_model;
mod datastore;
mod http_client;
mod oxide_controller;
mod sled_agent;
mod test_util;

pub use oxide_controller::controller_run_openapi_external;
pub use oxide_controller::controller_run_server;
pub use oxide_controller::ConfigController;
pub use oxide_controller::ControllerClient;
pub use oxide_controller::ControllerServerContext;
pub use oxide_controller::OxideController;
pub use oxide_controller::OxideControllerServer;
pub use oxide_controller::OxideControllerTestInterfaces;

pub use sled_agent::sa_run_server;
pub use sled_agent::ConfigSledAgent;
pub use sled_agent::SimMode;
pub use sled_agent::SledAgentClient;
pub use sled_agent::SledAgentServer;
pub use sled_agent::SledAgentTestInterfaces;

#[macro_use]
extern crate slog;
