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
mod server_controller_client;
mod test_util;

pub use controller::controller_external_api;
pub use controller::controller_internal_api;
pub use controller::controller_run_openapi_external;
pub use controller::controller_run_server;
pub use controller::populate_initial_data;
pub use controller::ControllerServerConfig;
pub use controller::ControllerServerContext;
pub use controller::OxideController;
pub use controller::OxideControllerTestInterfaces;

pub use api_model::ApiServerStartupInfo;
pub use server_controller::run_server_controller_api_server;
pub use server_controller::sc_dropshot_api;
pub use server_controller::ConfigServerController;
pub use server_controller::ControllerClient;
pub use server_controller::ServerController;
pub use server_controller::SimMode;
pub use server_controller_client::ServerControllerTestInterfaces;

#[macro_use]
extern crate slog;
