// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use crate::external_api::params;
use mg_admin_client::types::{AddBfdPeerRequest, PeerState};
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::shared::{BfdState, BfdStatus};
use omicron_common::api::{
    external::{Error, Name},
    internal::shared::{ParseSwitchLocationError, SwitchLocation},
};

impl super::Nexus {
    fn mg_client_for_switch_name(
        &self,
        switch: &Name,
    ) -> Result<Arc<mg_admin_client::Client>, Error> {
        let switch_location: SwitchLocation = switch.as_str().parse().map_err(
            |_: ParseSwitchLocationError| {
                Error::invalid_value("switch", "must be switch0 or switch 1")
            },
        )?;

        self.mg_client_for_switch_location(switch_location)
    }

    fn mg_client_for_switch_location(
        &self,
        switch: SwitchLocation,
    ) -> Result<Arc<mg_admin_client::Client>, Error> {
        let mg_client: Arc<mg_admin_client::Client> = self
            .mg_clients
            .get(&switch)
            .ok_or_else(|| {
                Error::not_found_by_name(
                    omicron_common::api::external::ResourceType::Switch,
                    &switch.to_string().parse().unwrap(),
                )
            })?
            .clone();

        Ok(mg_client)
    }

    pub async fn bfd_enable(
        &self,
        _opctx: &OpContext,
        session: params::BfdSessionEnable,
    ) -> Result<(), Error> {
        let mg_client = self.mg_client_for_switch_name(&session.switch)?;

        mg_client
            .inner
            .add_bfd_peer(&AddBfdPeerRequest {
                detection_threshold: session.detection_threshold,
                listen: session.local.unwrap_or(Ipv4Addr::UNSPECIFIED.into()),
                peer: session.remote,
                required_rx: session.required_rx,
            })
            .await
            .map_err(|e| {
                Error::internal_error(&format!("maghemite bfd enable: {e}"))
            })?;

        Ok(())
    }

    pub async fn bfd_disable(
        &self,
        _opctx: &OpContext,
        session: params::BfdSessionDisable,
    ) -> Result<(), Error> {
        let mg_client = self.mg_client_for_switch_name(&session.switch)?;

        mg_client.inner.remove_bfd_peer(&session.remote).await.map_err(
            |e| Error::internal_error(&format!("maghemite bfd disable: {e}")),
        )?;

        Ok(())
    }

    pub async fn bfd_status(
        &self,
        _opctx: &OpContext,
    ) -> Result<Vec<BfdStatus>, Error> {
        let mut result = Vec::new();
        for s in &[SwitchLocation::Switch0, SwitchLocation::Switch1] {
            let mg_client = self.mg_client_for_switch_location(*s)?;
            let status = mg_client
                .inner
                .get_bfd_peers()
                .await
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "maghemite get bfd peers: {e}"
                    ))
                })?
                .into_inner();

            for (addr, state) in status.iter() {
                let addr: IpAddr = addr.parse().unwrap();
                result.push(BfdStatus {
                    peer: addr,
                    state: match state {
                        PeerState::Up => BfdState::Up,
                        PeerState::Down => BfdState::Down,
                        PeerState::Init => BfdState::Init,
                        PeerState::AdminDown => BfdState::AdminDown,
                    },
                    switch: s.to_string().parse().unwrap(),
                })
            }
        }
        Ok(result)
    }
}
