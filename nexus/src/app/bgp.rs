// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::authz;
use crate::external_api::params;
use mg_admin_client::types::MessageHistoryRequest;
use nexus_db_model::{BgpAnnounceSet, BgpAnnouncement, BgpConfig};
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    self, BgpExported, BgpImported, BgpMessageHistory, BgpPeerStatus,
    CreateResult, DeleteResult, ListResultVec, LookupResult, NameOrId,
    SwitchBgpHistory,
};

impl super::Nexus {
    pub async fn bgp_config_create(
        &self,
        opctx: &OpContext,
        config: &params::BgpConfigCreate,
    ) -> CreateResult<BgpConfig> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let result = self.db_datastore.bgp_config_create(opctx, config).await?;
        Ok(result)
    }

    pub async fn bgp_config_get(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
    ) -> LookupResult<BgpConfig> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.bgp_config_get(opctx, &name_or_id).await
    }

    pub async fn bgp_config_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<BgpConfig> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.bgp_config_list(opctx, pagparams).await
    }

    pub async fn bgp_config_delete(
        &self,
        opctx: &OpContext,
        sel: &params::BgpConfigSelector,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let result = self.db_datastore.bgp_config_delete(opctx, sel).await?;
        Ok(result)
    }

    pub async fn bgp_update_announce_set(
        &self,
        opctx: &OpContext,
        announce: &params::BgpAnnounceSetCreate,
    ) -> CreateResult<(BgpAnnounceSet, Vec<BgpAnnouncement>)> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let result =
            self.db_datastore.bgp_update_announce_set(opctx, announce).await?;

        // eagerly propagate changes via rpw
        self.background_tasks
            .activate(&self.background_tasks.task_switch_port_settings_manager);
        Ok(result)
    }

    pub async fn bgp_announce_set_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<BgpAnnounceSet> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.bgp_announce_set_list(opctx, pagparams).await
    }

    pub async fn bgp_delete_announce_set(
        &self,
        opctx: &OpContext,
        sel: &params::BgpAnnounceSetSelector,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let result =
            self.db_datastore.bgp_delete_announce_set(opctx, sel).await?;
        Ok(result)
    }

    pub async fn bgp_announcement_list(
        &self,
        opctx: &OpContext,
        sel: &params::BgpAnnounceSetSelector,
    ) -> ListResultVec<BgpAnnouncement> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.bgp_announcement_list(opctx, sel).await
    }

    pub async fn bgp_peer_status(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<BgpPeerStatus> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let mut result = Vec::new();
        for (switch, client) in &self.mg_clients().await.map_err(|e| {
            external::Error::internal_error(&format!(
                "failed to get mg clients: {e}"
            ))
        })? {
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
                let peers = match client.get_neighbors_v4(asn).await {
                    Ok(result) => result.into_inner(),
                    Err(e) => {
                        error!(
                            self.log,
                            "failed to get peers for asn {asn} from {switch}: {e}"
                        );
                        continue;
                    }
                };
                for (peer_id, info) in peers {
                    result.push(BgpPeerStatus {
                        switch: *switch,
                        peer_id: peer_id.clone(),
                        addr: info.remote_ip,
                        local_asn: r.asn,
                        remote_asn: info.asn.unwrap_or(0),
                        state: info.fsm_state.into(),
                        state_duration_millis: u64::try_from(
                            info.fsm_state_duration.as_millis(),
                        )
                        .unwrap_or(u64::MAX),
                    });
                }
            }
        }
        Ok(result)
    }

    pub async fn bgp_exported(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<BgpExported> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let mut result = BgpExported::default();
        for (switch, client) in &self.mg_clients().await.map_err(|e| {
            external::Error::internal_error(&format!(
                "failed to get mg clients: {e}"
            ))
        })? {
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
                let selector = mg_admin_client::types::ExportedSelector {
                    afi: None,
                    asn,
                    peer: None,
                };

                let exported = match client.get_exported_v2(&selector).await {
                    Ok(result) => result.into_inner(),
                    Err(e) => {
                        error!(
                            self.log,
                            "failed to get exports for asn {asn} from {switch}: {e}"
                        );
                        continue;
                    }
                };

                for (addr, exports) in exported {
                    let mut xps = Vec::new();
                    for ex in exports.iter() {
                        let net = match ex {
                            rdb_types::Prefix::V4(v4) => {
                                oxnet::IpNet::V4(oxnet::Ipv4Net::new_unchecked(
                                    v4.value, v4.length,
                                ))
                            }
                            rdb_types::Prefix::V6(v6) => {
                                oxnet::IpNet::V6(oxnet::Ipv6Net::new_unchecked(
                                    v6.value, v6.length,
                                ))
                            }
                        };
                        xps.push(net);
                    }
                    result.exports.insert(addr.to_string(), xps);
                }
            }
        }
        Ok(result)
    }

    pub async fn bgp_message_history(
        &self,
        opctx: &OpContext,
        sel: &params::BgpRouteSelector,
    ) -> ListResultVec<SwitchBgpHistory> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        let mut result = Vec::new();
        for (switch, client) in &self.mg_clients().await.map_err(|e| {
            external::Error::internal_error(&format!(
                "failed to get mg clients: {e}"
            ))
        })? {
            let history = match client
                .message_history_v3(&MessageHistoryRequest {
                    asn: sel.asn,
                    direction: None,
                    peer: None,
                })
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

    pub async fn bgp_imported_routes(
        &self,
        opctx: &OpContext,
        _sel: &params::BgpRouteSelector,
    ) -> ListResultVec<BgpImported> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let mut result = Vec::new();
        for (switch, client) in &self.mg_clients().await.map_err(|e| {
            external::Error::internal_error(&format!(
                "failed to get mg clients: {e}"
            ))
        })? {
            let mut imported: Vec<BgpImported> = Vec::new();
            match client.get_rib_imported_v2(None, None).await {
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
                            let x = BgpImported {
                                switch: *switch,
                                prefix: ipnet,
                                id: p
                                    .bgp
                                    .as_ref()
                                    .map(|bgp| bgp.id)
                                    .unwrap_or(0),
                                nexthop: p.nexthop,
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
