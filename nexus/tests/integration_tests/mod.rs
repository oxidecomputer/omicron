//! Nexus integration tests
//!
//! See the driver in the parent directory for how and why this is structured
//! the way it is.

mod authn_http;
mod authz;
mod basic;
mod commands;
mod console_api;
mod device_auth;
mod disks;
mod images;
mod instances;
mod ip_pools;
mod metrics;
mod organizations;
mod oximeter;
mod password_login;
mod projects;
mod rack;
mod role_assignments;
mod roles_builtin;
mod router_routes;
mod saml;
mod silos;
mod snapshots;
mod ssh_keys;
mod subnet_allocation;
mod timeseries;
mod unauthorized;
mod unauthorized_coverage;
mod updates;
mod users_builtin;
mod volume_management;
mod vpc_firewall;
mod vpc_routers;
mod vpc_subnets;
mod vpcs;
mod zpools;

// This module is used only for shared data, not test cases.
mod endpoints;
