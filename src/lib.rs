/*!
 * Library interfaces for this crate, intended for use only by the automated
 * test suite.  This crate does not define a Rust library API that's intended to
 * be consumed from the outside.
 */

mod api_error;
pub mod api_model;
mod controller;
mod datastore;
mod http_client;
mod server_controller;
mod test_util;

pub use controller::controller_run_openapi_external;
pub use controller::controller_run_server;
pub use controller::populate_initial_data;
pub use controller::ControllerClient;
pub use controller::ControllerServerConfig;
pub use controller::ControllerServerContext;
pub use controller::OxideController;
pub use controller::OxideControllerServer;
pub use controller::OxideControllerTestInterfaces;

pub use api_model::ApiServerStartupInfo;
pub use server_controller::sc_run_server;
pub use server_controller::ConfigServerController;
pub use server_controller::ServerController;
pub use server_controller::ServerControllerClient;
pub use server_controller::ServerControllerServer;
pub use server_controller::ServerControllerTestInterfaces;
pub use server_controller::SimMode;

#[macro_use]
extern crate slog;
