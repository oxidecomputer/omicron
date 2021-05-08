/*!
* Library interface to the sled agent
 */

/*
 * We only use rustdoc for internal documentation, including private items, so
 * it's expected that we'll have links to private items in the docs.
 */
#![allow(private_intra_doc_links)]
/* Clippy's style lints are useful, but not worth running automatically. */
#![allow(clippy::style)]

pub mod sim;

mod config;
mod http_entrypoints;
mod server;
mod sled_agent;

pub use server::run_server;
pub use config::Config;

#[macro_use]
extern crate slog;
