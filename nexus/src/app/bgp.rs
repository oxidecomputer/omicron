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
    self, BgpImportedRouteIpv4, BgpMessageHistory, BgpPeerStatus, CreateResult,
    DeleteResult, Ipv4Net, ListResultVec, LookupResult, NameOrId,
    SwitchBgpHistory,
};

impl super::Nexus {
    pub async fn bgp_config_set(
        &self,
        opctx: &OpContext,
        config: &params::BgpConfigCreate,
    ) -> CreateResult<BgpConfig> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let result = self.db_datastore.bgp_config_set(opctx, config).await?;
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

    pub async fn bgp_create_announce_set(
        &self,
        opctx: &OpContext,
        announce: &params::BgpAnnounceSetCreate,
    ) -> CreateResult<(BgpAnnounceSet, Vec<BgpAnnouncement>)> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let result =
            self.db_datastore.bgp_create_announce_set(opctx, announce).await?;
        Ok(result)
    }

    pub async fn bgp_announce_list(
        &self,
        opctx: &OpContext,
        sel: &params::BgpAnnounceSetSelector,
    ) -> ListResultVec<BgpAnnouncement> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.bgp_announce_list(opctx, sel).await
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
            let router_info = match client.get_routers().await {
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
                for (addr, info) in &r.peers {
                    let Ok(addr) = addr.parse() else {
                        continue;
                    };
                    result.push(BgpPeerStatus {
                        switch: *switch,
                        addr,
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

    pub async fn bgp_imported_routes_ipv4(
        &self,
        opctx: &OpContext,
        sel: &params::BgpRouteSelector,
    ) -> ListResultVec<BgpImportedRouteIpv4> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let mut result = Vec::new();
        for (switch, client) in &self.mg_clients().await.map_err(|e| {
            external::Error::internal_error(&format!(
                "failed to get mg clients: {e}"
            ))
        })? {
            let imported: Vec<BgpImportedRouteIpv4> = match client
                .get_imported4(&mg_admin_client::types::GetImported4Request {
                    asn: sel.asn,
                })
                .await
            {
                Ok(result) => result
                    .into_inner()
                    .into_iter()
                    .map(|x| BgpImportedRouteIpv4 {
                        switch: *switch,
                        prefix: Ipv4Net(
                            ipnetwork::Ipv4Network::new(
                                x.prefix.value,
                                x.prefix.length,
                            )
                            .unwrap(),
                        ),
                        nexthop: x.nexthop,
                        id: x.id,
                    })
                    .collect(),
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
