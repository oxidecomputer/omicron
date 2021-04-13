/*!
 * Library interface to the bootstrap agent
 */

mod bootstrap_agent;
mod bootstrap_agent_client;
mod config;
mod http_entrypoints;
mod server;

pub use config::Config;
pub use server::Server;

#[macro_use]
extern crate slog;
