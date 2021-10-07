//! Utilities to assist with testing across Omicron crates.

// We only use rustdoc for internal documentation, including private items, so
// it's expected that we'll have links to private items in the docs.
#![allow(rustdoc::private_intra_doc_links)]

pub mod dev;

#[macro_use]
extern crate slog;
