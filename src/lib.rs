/*!
 * Library interfaces for this crate, intended for use only by the automated
 * test suite.  This crate does not define a Rust library API that's intended to
 * be consumed from the outside.
 *
 * TODO-cleanup is there a better way to do this?
 */

pub mod api_error;
pub mod api_handler;
mod api_http_entrypoints;
pub mod api_http_router;
pub mod api_http_util;
pub mod api_model;
pub mod api_server;
mod sim;
