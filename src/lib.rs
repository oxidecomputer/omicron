/*!
 * Library interfaces for this crate, intended for use only by the automated
 * test suite.  This crate does not define a Rust library API that's intended to
 * be consumed from the outside.
 */

mod api_error;
pub mod api_model;
mod datastore;
mod http_client;
mod oxide_controller;
mod server_controller;
mod test_util;

pub use oxide_controller::controller_run_openapi_external;
pub use oxide_controller::controller_run_server;
pub use oxide_controller::ConfigController;
pub use oxide_controller::ControllerClient;
pub use oxide_controller::ControllerServerContext;
pub use oxide_controller::OxideController;
pub use oxide_controller::OxideControllerServer;
pub use oxide_controller::OxideControllerTestInterfaces;

pub use server_controller::sc_run_server;
pub use server_controller::ConfigServerController;
pub use server_controller::ServerControllerClient;
pub use server_controller::ServerControllerServer;
pub use server_controller::ServerControllerTestInterfaces;
pub use server_controller::SimMode;

#[macro_use]
extern crate slog;
