//! Nexus integration tests
//!
//! See the driver in the parent directory for how and why this is structured
//! the way it is.

mod address_lots;
mod allow_list;
mod authn_http;
mod authz;
mod basic;
mod certificates;
mod commands;
mod console_api;
mod crucible_replacements;
mod demo_saga;
mod device_auth;
mod disks;
mod external_ips;
mod host_phase1_updater;
mod images;
mod initialization;
mod instances;
mod internet_gateway;
mod ip_pools;
mod metrics;
mod oximeter;
mod pantry;
mod password_login;
mod probe;
mod projects;
mod quotas;
mod rack;
mod role_assignments;
mod roles_builtin;
mod rot_updater;
mod router_routes;
mod saml;
mod schema;
mod silo_users;
mod silos;
mod sleds;
mod snapshots;
mod sp_updater;
mod ssh_keys;
mod subnet_allocation;
mod switch_port;
mod unauthorized;
mod unauthorized_coverage;
mod updates;
mod users_builtin;
mod utilization;
mod volume_management;
mod vpc_firewall;
mod vpc_routers;
mod vpc_subnets;
mod vpcs;

// This module is used only for shared data, not test cases.
mod endpoints;
