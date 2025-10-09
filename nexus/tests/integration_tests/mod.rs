// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus integration tests
//!
//! See the driver in the parent directory for how and why this is structured
//! the way it is.

mod address_lots;
mod affinity;
mod allow_list;
mod audit_log;
mod authn_http;
mod authz;
mod basic;
mod certificates;
mod cockroach;
mod commands;
mod console_api;
mod crucible_replacements;
mod demo_saga;
mod device_auth;
mod disks;
mod external_ips;
mod images;
mod initialization;
mod instances;
mod internet_gateway;
mod ip_pools;
mod metrics;
mod metrics_querier;
mod oximeter;
mod pantry;
mod password_login;
mod probe;
mod projects;
mod quiesce;
mod quotas;
mod rack;
mod role_assignments;
mod router_routes;
mod saml;
mod schema;
mod scim;
mod silo_users;
mod silos;
mod sleds;
mod snapshots;
mod ssh_keys;
mod subnet_allocation;
mod support_bundles;
mod switch_port;
mod target_release;
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
mod webhooks;

// This module is used only for shared data, not test cases.
mod endpoints;
