//! Library interface to the sled agent

// We only use rustdoc for internal documentation, including private items, so
// it's expected that we'll have links to private items in the docs.
#![allow(rustdoc::private_intra_doc_links)]
// Clippy's style lints are useful, but not worth running automatically.
#![allow(clippy::style)]

// Module for executing the simulated sled agent.
pub mod sim;

// Modules shared by both simulated and non-simulated sled agents.
pub mod common;

// Modules for the non-simulated sled agent.
pub mod bootstrap;
pub mod config;
mod http_entrypoints;
mod illumos;
mod instance;
mod instance_manager;
mod params;
pub mod server;
mod sled_agent;

#[cfg(test)]
mod mocks;

#[macro_use]
extern crate slog;
