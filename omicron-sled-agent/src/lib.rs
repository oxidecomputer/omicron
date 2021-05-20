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

pub mod common;
pub mod sim;

// TODO: Double check pub / exporting from submodules? Maybe limit visibility?

pub mod config;
mod http_entrypoints;
pub mod server;
mod sled_agent;

#[macro_use]
extern crate slog;
