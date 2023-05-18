// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

#![allow(clippy::redundant_closure_call)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::match_single_binding)]
#![allow(clippy::clone_on_copy)]
#![allow(clippy::unnecessary_to_owned)]
// The progenitor-generated API for dpd currently incorporates a type from
// oximeter, which includes a docstring that has a doc-test in it.
// That test passes for code that lives in omicron, but fails for code imported
// by omicron.
#![allow(rustdoc::broken_intra_doc_links)]

use slog::Logger;
use slog::info;

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
    pub async fn ensure_nat_entry(
        &self,
        log: &Logger,
        target_ip: ipnetwork::IpNetwork,
        target_mac: types::MacAddr,
        target_first_port: u16,
        target_last_port: u16,
        target_vni: u32,
        sled_ip_address: &std::net::Ipv6Addr,
    ) -> Result<(), progenitor_client::Error<types::Error>> {
        let existing_nat = match target_ip {
            ipnetwork::IpNetwork::V4(network) => {
                self
                    .nat_ipv4_get(&network.ip(), target_first_port)
                    .await
            }
            ipnetwork::IpNetwork::V6(network) => {
                self
                    .nat_ipv6_get(&network.ip(), target_first_port)
                    .await
            }
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

                    match target_ip {
                        ipnetwork::IpNetwork::V4(network) => {
                            self
                                .nat_ipv4_delete(
                                    &network.ip(),
                                    target_first_port,
                                )
                                .await
                        }
                        ipnetwork::IpNetwork::V6(network) => {
                            self
                                .nat_ipv6_delete(
                                    &network.ip(),
                                    target_first_port,
                                )
                                .await
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

        match target_ip {
            ipnetwork::IpNetwork::V4(network) => {
                self
                    .nat_ipv4_create(
                        &network.ip(),
                        target_first_port,
                        target_last_port,
                        &nat_target,
                    )
                    .await
            }
            ipnetwork::IpNetwork::V6(network) => {
                self
                    .nat_ipv6_create(
                        &network.ip(),
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
}
