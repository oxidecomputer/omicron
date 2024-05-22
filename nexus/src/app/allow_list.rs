// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

//! Nexus methods for operating on source IP allowlists.

use nexus_db_queries::context::OpContext;
use nexus_types::external_api::params;
use nexus_types::external_api::views::AllowList;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use std::net::IpAddr;

impl super::Nexus {
    /// Fetch the allowlist of source IPs that can reach user-facing services.
    pub async fn allow_list_view(
        &self,
        opctx: &OpContext,
    ) -> Result<AllowList, Error> {
        self.db_datastore
            .allow_list_view(opctx)
            .await
            .and_then(AllowList::try_from)
    }

    /// Upsert the allowlist of source IPs that can reach user-facing services.
    pub async fn allow_list_upsert(
        &self,
        opctx: &OpContext,
        remote_addr: IpAddr,
        params: params::AllowListUpdate,
    ) -> Result<AllowList, Error> {
        if let external::AllowedSourceIps::List(list) = &params.allowed_ips {
            // Size limits on the allowlist.
            const MAX_ALLOWLIST_LENGTH: usize = 1000;
            if list.len() > MAX_ALLOWLIST_LENGTH {
                let message = format!(
                    "Source IP allowlist is limited to {} entries, found {}",
                    MAX_ALLOWLIST_LENGTH,
                    list.len(),
                );
                return Err(Error::invalid_request(message));
            }

            // Some basic sanity-checks on the addresses in the allowlist.
            //
            // The most important part here is checking that the source address
            // the request came from is on the allowlist. This is our only real
            // guardrail to prevent accidentally preventing any future access to
            // the rack!
            let mut contains_remote = false;
            for entry in list.iter() {
                contains_remote = entry.contains(remote_addr);
                if entry.addr().is_unspecified() {
                    return Err(Error::invalid_request(
                        "Source IP allowlist may not contain the \
                        unspecified address. Use \"any\" to allow \
                        any source to connect to user-facing services.",
                    ));
                }
                if entry.width() == 0 {
                    return Err(Error::invalid_request(
                        "Source IP allowlist entries may not have \
                        a netmask of /0.",
                    ));
                }
            }
            if !contains_remote {
                return Err(Error::invalid_request(
                    "The source IP allow list would prevent access \
                    from the current client! Ensure that the allowlist \
                    contains an entry that continues to allow access \
                    from this peer.",
                ));
            }
        };

        // Actually insert the new allowlist.
        let list = self
            .db_datastore
            .allow_list_upsert(opctx, params.allowed_ips.clone())
            .await
            .and_then(AllowList::try_from)?;

        // Notify the sled-agents of the updated firewall rules.
        //
        // Importantly, we need to use a different `opctx` from that we're
        // passed in here. This call requires access to Oxide-internal data
        // around our VPC, and so we must use a context that's authorized for
        // that.
        //
        // TODO-debugging: It's unfortunate that we're using this new logger,
        // since that means we lose things like the original actor and request
        // ID. It would be great if we could insert additional key-value pairs
        // into the logger itself here, or "merge" the two in some other way.
        info!(
            opctx.log,
            "updated user-facing services allow list, switching to \
            internal opcontext to plumb rules to sled-agents";
            "new_allowlist" => ?params.allowed_ips,
        );
        let new_opctx = self.opctx_for_internal_api();
        match nexus_networking::plumb_service_firewall_rules(
            self.datastore(),
            &new_opctx,
            &[],
            &new_opctx,
            &new_opctx.log,
        )
        .await
        {
            Ok(_) => {
                info!(self.log, "plumbed updated IP allowlist to sled-agents");
                Ok(list)
            }
            Err(e) => {
                error!(
                    self.log,
                    "failed to update sled-agents with new allowlist";
                    "error" => ?e
                );
                let message = "Failed to plumb allowlist as firewall rules \
                to relevant sled agents. The request must be retried for them \
                to take effect.";
                Err(Error::unavail(message))
            }
        }
    }

    /// Wait until we've applied the user-facing services allowlist.
    ///
    /// This will block until we've plumbed this allowlist and passed it to the
    /// sled-agents responsible. This should only be called from
    /// rack-initialization handling.
    pub(crate) async fn await_ip_allowlist_plumbing(&self) {
        let opctx = self.opctx_for_internal_api();
        loop {
            match nexus_networking::plumb_service_firewall_rules(
                self.datastore(),
                &opctx,
                &[],
                &opctx,
                &opctx.log,
            )
            .await
            {
                Ok(_) => {
                    info!(self.log, "plumbed initial IP allowlist");
                    return;
                }
                Err(e) => {
                    error!(
                        self.log,
                        "failed to plumb initial IP allowlist";
                        "error" => ?e
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
    }
}
