// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

// The progenitor-generated API for dpd currently incorporates a type from
// oximeter, which includes a docstring that has a doc-test in it.
// That test passes for code that lives in omicron, but fails for code imported
// by omicron.
#![allow(rustdoc::broken_intra_doc_links)]

use std::net::IpAddr;

use slog::info;
use slog::Logger;

include!(concat!(env!("OUT_DIR"), "/dpd-client.rs"));

/// State maintained by a [`Client`].
#[derive(Clone, Debug)]
pub struct ClientState {
    /// An arbitrary tag used to identify a client, for controlling things like
    /// per-client settings.
    pub tag: String,
    /// Used for logging requests and responses.
    pub log: Logger,
}

impl Client {
    /// Ensure that a NAT entry exists, overwriting a previous conflicting entry if
    /// applicable.
    ///
    /// nat_ipv[46]_create are not idempotent (see oxidecomputer/dendrite#343),
    /// but this wrapper function is. Call this from sagas instead.
    #[allow(clippy::too_many_arguments)]
    pub async fn ensure_nat_entry(
        &self,
        log: &Logger,
        target_ip: IpAddr,
        target_mac: types::MacAddr,
        target_first_port: u16,
        target_last_port: u16,
        target_vni: u32,
        sled_ip_address: &std::net::Ipv6Addr,
    ) -> Result<(), progenitor_client::Error<types::Error>> {
        let existing_nat = match &target_ip {
            IpAddr::V4(ip) => self.nat_ipv4_get(ip, target_first_port).await,
            IpAddr::V6(ip) => self.nat_ipv6_get(ip, target_first_port).await,
        };

        // If a NAT entry already exists, but has the wrong internal
        // IP address, delete the old entry before continuing (the
        // DPD entry-creation API won't replace an existing entry).
        // If the entry exists and has the right internal IP, there's
        // no more work to do for this external IP.
        match existing_nat {
            Ok(existing) => {
                let existing = existing.into_inner();
                if existing.internal_ip != *sled_ip_address {
                    info!(log, "deleting old nat entry";
                      "target_ip" => ?target_ip);

                    match &target_ip {
                        IpAddr::V4(ip) => {
                            self.nat_ipv4_delete(ip, target_first_port).await
                        }
                        IpAddr::V6(ip) => {
                            self.nat_ipv6_delete(ip, target_first_port).await
                        }
                    }?;
                } else {
                    info!(log,
                      "nat entry with expected internal ip exists";
                      "target_ip" => ?target_ip,
                      "existing_entry" => ?existing);

                    return Ok(());
                }
            }
            Err(e) => {
                if e.status() == Some(http::StatusCode::NOT_FOUND) {
                    info!(log, "no nat entry found for: {target_ip:#?}");
                } else {
                    return Err(e);
                }
            }
        }

        info!(log, "creating nat entry for: {target_ip:#?}");
        let nat_target = crate::types::NatTarget {
            inner_mac: target_mac,
            internal_ip: *sled_ip_address,
            vni: target_vni.into(),
        };

        match &target_ip {
            IpAddr::V4(ip) => {
                self.nat_ipv4_create(
                    ip,
                    target_first_port,
                    target_last_port,
                    &nat_target,
                )
                .await
            }
            IpAddr::V6(ip) => {
                self.nat_ipv6_create(
                    ip,
                    target_first_port,
                    target_last_port,
                    &nat_target,
                )
                .await
            }
        }?;

        info!(log, "creation of nat entry successful for: {target_ip:#?}");

        Ok(())
    }

    /// Ensure that a loopback address is created.
    ///
    /// loopback_ipv[46]_create are not idempotent (see
    /// oxidecomputer/dendrite#343), but this wrapper function is. Call this
    /// from sagas instead.
    pub async fn ensure_loopback_created(
        &self,
        log: &Logger,
        address: IpAddr,
        tag: &str,
    ) -> Result<(), progenitor_client::Error<types::Error>> {
        let result = match &address {
            IpAddr::V4(a) => {
                self.loopback_ipv4_create(&types::Ipv4Entry {
                    addr: *a,
                    tag: tag.into(),
                })
                .await
            }
            IpAddr::V6(a) => {
                self.loopback_ipv6_create(&types::Ipv6Entry {
                    addr: *a,
                    tag: tag.into(),
                })
                .await
            }
        };

        match result {
            Ok(_) => {
                info!(log, "created loopback address"; "address" => ?address);
                Ok(())
            }

            Err(progenitor_client::Error::ErrorResponse(er)) => {
                match er.status() {
                    http::StatusCode::CONFLICT => {
                        info!(log, "loopback address already created"; "address" => ?address);

                        Ok(())
                    }

                    _ => Err(progenitor_client::Error::ErrorResponse(er)),
                }
            }

            Err(e) => Err(e),
        }
    }

    /// Ensure that a loopback address is deleted.
    ///
    /// loopback_ipv[46]_delete are not idempotent (see
    /// oxidecomputer/dendrite#343), but this wrapper function is. Call this
    /// from sagas instead.
    pub async fn ensure_loopback_deleted(
        &self,
        log: &Logger,
        address: IpAddr,
    ) -> Result<(), progenitor_client::Error<types::Error>> {
        let result = match &address {
            IpAddr::V4(a) => self.loopback_ipv4_delete(&a).await,
            IpAddr::V6(a) => self.loopback_ipv6_delete(&a).await,
        };

        match result {
            Ok(_) => {
                info!(log, "deleted loopback address"; "address" => ?address);
                Ok(())
            }

            Err(progenitor_client::Error::ErrorResponse(er)) => {
                match er.status() {
                    http::StatusCode::NOT_FOUND => {
                        info!(log, "loopback address already deleted"; "address" => ?address);
                        Ok(())
                    }

                    _ => Err(progenitor_client::Error::ErrorResponse(er)),
                }
            }

            Err(e) => Err(e),
        }
    }
}
