// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::authz;
use crate::external_api::params;
use internal_dns::resolver::Resolver;
use mg_admin_client::types::MessageHistoryRequest;
use nexus_db_model::{BgpAnnounceSet, BgpAnnouncement, BgpConfig};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    self, BgpImportedRouteIpv4, BgpMessageHistory, BgpPeerStatus, CreateResult,
    DeleteResult, ListResultVec, LookupResult, NameOrId, SwitchBgpHistory,
};
use slog::Logger;
use std::net::IpAddr;
use std::sync::Arc;

/// Application level operations for BGP
pub struct Bgp {
    log: Logger,
    datastore: Arc<db::DataStore>,
    internal_resolver: Resolver,
}

impl Bgp {
    pub fn new(
        log: Logger,
        datastore: Arc<db::DataStore>,
        internal_resolver: Resolver,
    ) -> Bgp {
        Bgp { log, datastore, internal_resolver }
    }
    pub async fn config_set(
        &self,
        opctx: &OpContext,
        config: &params::BgpConfigCreate,
    ) -> CreateResult<BgpConfig> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let result = self.datastore.bgp_config_set(opctx, config).await?;
        Ok(result)
    }

    pub async fn config_get(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
    ) -> LookupResult<BgpConfig> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.datastore.bgp_config_get(opctx, &name_or_id).await
    }

    pub async fn config_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<BgpConfig> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.datastore.bgp_config_list(opctx, pagparams).await
    }

    pub async fn config_delete(
        &self,
        opctx: &OpContext,
        sel: &params::BgpConfigSelector,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let result = self.datastore.bgp_config_delete(opctx, sel).await?;
        Ok(result)
    }

    pub async fn create_announce_set(
        &self,
        opctx: &OpContext,
        announce: &params::BgpAnnounceSetCreate,
    ) -> CreateResult<(BgpAnnounceSet, Vec<BgpAnnouncement>)> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let result =
            self.datastore.bgp_create_announce_set(opctx, announce).await?;
        Ok(result)
    }

    pub async fn announce_list(
        &self,
        opctx: &OpContext,
        sel: &params::BgpAnnounceSetSelector,
    ) -> ListResultVec<BgpAnnouncement> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.datastore.bgp_announce_list(opctx, sel).await
    }

    pub async fn delete_announce_set(
        &self,
        opctx: &OpContext,
        sel: &params::BgpAnnounceSetSelector,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let result = self.datastore.bgp_delete_announce_set(opctx, sel).await?;
        Ok(result)
    }

    pub async fn peer_status(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<BgpPeerStatus> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let mut result = Vec::new();
        for (switch, client) in
            &super::mg_clients(&self.internal_resolver, &self.log)
                .await
                .map_err(|e| {
                    external::Error::internal_error(&format!(
                        "failed to get mg clients: {e}"
                    ))
                })?
        {
            let router_info = match client.read_routers().await {
                Ok(result) => result.into_inner(),
                Err(e) => {
                    error!(
                        self.log,
                        "failed to get routers from {switch}: {e}"
                    );
                    continue;
                }
            };

            for r in &router_info {
                let asn = r.asn;

                let peers = match client.get_neighbors(asn).await {
                    Ok(result) => result.into_inner(),
                    Err(e) => {
                        error!(
                        self.log,
                        "failed to get peers for asn {asn} from {switch}: {e}"
                    );
                        continue;
                    }
                };
                for (host, info) in peers {
                    let Ok(host) = host.parse() else {
                        error!(
                            self.log,
                            "failed to parse peer host address {host}",
                        );
                        continue;
                    };
                    result.push(BgpPeerStatus {
                        switch: *switch,
                        addr: host,
                        local_asn: r.asn,
                        remote_asn: info.asn.unwrap_or(0),
                        state: info.state.into(),
                        state_duration_millis: info.duration_millis,
                    });
                }
            }
        }
        Ok(result)
    }

    pub async fn message_history(
        &self,
        opctx: &OpContext,
        sel: &params::BgpRouteSelector,
    ) -> ListResultVec<SwitchBgpHistory> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        let mut result = Vec::new();
        for (switch, client) in
            &super::mg_clients(&self.internal_resolver, &self.log)
                .await
                .map_err(|e| {
                    external::Error::internal_error(&format!(
                        "failed to get mg clients: {e}"
                    ))
                })?
        {
            let history = match client
                .message_history(&MessageHistoryRequest { asn: sel.asn })
                .await
            {
                Ok(result) => result.into_inner().by_peer.clone(),
                Err(e) => {
                    error!(
                        self.log,
                        "failed to get bgp history from {switch}: {e}"
                    );
                    continue;
                }
            };

            result.push(SwitchBgpHistory {
                switch: *switch,
                history: history
                    .into_iter()
                    .map(|(k, v)| (k, BgpMessageHistory::new(v)))
                    .collect(),
            });
        }

        Ok(result)
    }

    pub async fn imported_routes_ipv4(
        &self,
        opctx: &OpContext,
        sel: &params::BgpRouteSelector,
    ) -> ListResultVec<BgpImportedRouteIpv4> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let mut result = Vec::new();
        for (switch, client) in
            &super::mg_clients(&self.internal_resolver, &self.log)
                .await
                .map_err(|e| {
                    external::Error::internal_error(&format!(
                        "failed to get mg clients: {e}"
                    ))
                })?
        {
            let mut imported: Vec<BgpImportedRouteIpv4> = Vec::new();
            match client
                .get_imported(&mg_admin_client::types::AsnSelector {
                    asn: sel.asn,
                })
                .await
            {
                Ok(result) => {
                    for (prefix, paths) in result.into_inner().iter() {
                        let ipnet = match prefix.parse() {
                            Ok(p) => p,
                            Err(e) => {
                                error!(
                                    self.log,
                                    "failed to parse prefix {prefix}: {e}"
                                );
                                continue;
                            }
                        };
                        for p in paths.iter() {
                            let nexthop = match p.nexthop {
                                IpAddr::V4(addr) => addr,
                                IpAddr::V6(_) => continue,
                            };
                            let x = BgpImportedRouteIpv4 {
                                switch: *switch,
                                prefix: ipnet,
                                id: p
                                    .bgp
                                    .as_ref()
                                    .map(|bgp| bgp.id)
                                    .unwrap_or(0),
                                nexthop,
                            };
                            imported.push(x);
                        }
                    }
                }
                Err(e) => {
                    error!(
                        self.log,
                        "failed to get BGP imported from {switch}: {e}"
                    );
                    continue;
                }
            };

            result.extend_from_slice(&imported);
        }
        Ok(result)
    }
}
