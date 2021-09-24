//! Tools for interacting with the timeseries database.
// Copyright 2021 Oxide Computer Company

mod client;
pub mod model;
pub mod query;

pub use client::Client;
