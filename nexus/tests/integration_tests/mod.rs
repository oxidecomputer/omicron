//! Nexus integration tests
//!
//! See the driver in the parent directory for how and why this is structured
//! the way it is.

mod authn_http;
mod basic;
mod commands;
mod console_api;
mod datasets;
mod disks;
mod images;
mod instances;
mod organizations;
mod oximeter;
mod projects;
mod roles_builtin;
mod router_routes;
mod silos;
mod ssh_keys;
mod subnet_allocation;
mod timeseries;
mod unauthorized;
mod unauthorized_coverage;
mod updates;
mod users_builtin;
mod vpc_firewall;
mod vpc_routers;
mod vpc_subnets;
mod vpcs;
mod zpools;

// This module is used only for shared data, not test cases.
mod endpoints;
