// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for managing switch bidirectional forwarding detection
//! (BFD) sessions.

use crate::app::{
    background::tasks::networking::build_mgd_clients,
    switch_zone_address_mappings,
};

use crate::app::background::BackgroundTask;
use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use mg_admin_client::ClientInfo;
use mg_admin_client::types::{BfdPeerConfig, SessionMode};
use nexus_db_model::{BfdMode, BfdSession};
use nexus_db_queries::{context::OpContext, db::DataStore};
use omicron_common::api::external::{DataPageParams, SwitchLocation};
use serde_json::json;
use std::{
    collections::HashSet,
    hash::Hash,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

pub struct BfdManager {
    datastore: Arc<DataStore>,
    resolver: Resolver,
}

impl BfdManager {
    pub fn new(datastore: Arc<DataStore>, resolver: Resolver) -> Self {
        Self { datastore, resolver }
    }
}

struct BfdSessionKey {
    switch: SwitchLocation,
    local: Option<IpAddr>,
    remote: IpAddr,
    detection_threshold: u8,
    required_rx: u64,
    mode: BfdMode,
}

impl BfdSessionKey {
    fn needs_update(&self, target: &BfdSessionKey) -> bool {
        self.detection_threshold != target.detection_threshold
            || self.required_rx != target.required_rx
            || self.mode != target.mode
    }
}

impl Hash for BfdSessionKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.switch.to_string().hash(state);
        self.remote.hash(state);
    }
}

impl PartialEq for BfdSessionKey {
    fn eq(&self, other: &Self) -> bool {
        self.switch.eq(&other.switch) && self.remote.eq(&other.remote)
    }
}

impl Eq for BfdSessionKey {}

impl From<BfdSession> for BfdSessionKey {
    fn from(value: BfdSession) -> Self {
        Self {
            switch: value.switch.parse().unwrap(), //TODO unwrap
            remote: value.remote.ip(),
            local: value.local.map(|x| x.ip()),
            detection_threshold: value
                .detection_threshold
                .0
                .try_into()
                .unwrap(), //TODO unwrap
            required_rx: value.required_rx.0.into(),
            mode: value.mode,
        }
    }
}

impl BackgroundTask for BfdManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;

            let target: HashSet<BfdSessionKey> = match self
                .datastore
                .bfd_session_list(opctx, &DataPageParams::max_page())
                .await
            {
                Ok(sessions) => sessions.into_iter().map(Into::into).collect(),
                Err(e) => {
                    error!(&log, "failed to get bfd sessions from db";
                        "error" => e.to_string()
                    );
                    return json!({
                        "error":
                            format!(
                                "failed to get bfd sessions from db: \
                                {:#}",
                                e
                            )
                    });
                }
            };

            let mut current: HashSet<BfdSessionKey> = HashSet::new();

            let mappings = match switch_zone_address_mappings(&self.resolver, log).await {
                Ok(mappings) => mappings,
                Err(e) => {
                    error!(log, "failed to resolve addresses for Dendrite services"; "error" => %e);
                    return json!({
                        "error":
                            format!(
                                "failed to resolve addresses for Dendrite services: {:#}",
                                e
                            )
                    });
                },
            };

            let mgd_clients = build_mgd_clients(mappings, log);

            for (location, c) in &mgd_clients {
                let client_current = match c.get_bfd_peers().await {
                    Ok(x) => x.into_inner(),
                    Err(e) => {
                        error!(&log, "failed to get bfd sessions from mgd: {}",
                            c.baseurl();
                            "error" => e.to_string()
                        );
                        continue;
                    }
                };
                for info in &client_current {
                    current.insert(BfdSessionKey {
                        local: Some(info.config.listen),
                        remote: info.config.peer,
                        detection_threshold: info.config.detection_threshold,
                        required_rx: info.config.required_rx,
                        switch: *location,
                        mode: match info.config.mode {
                            SessionMode::SingleHop => BfdMode::SingleHop,
                            SessionMode::MultiHop => BfdMode::MultiHop,
                        },
                    });
                }
            }

            let to_add: HashSet<&BfdSessionKey> =
                target.difference(&current).collect();

            let to_del: HashSet<&BfdSessionKey> =
                current.difference(&target).collect();

            let to_check: HashSet<&BfdSessionKey> =
                target.intersection(&current).collect();

            let mut to_update: HashSet<&BfdSessionKey> = HashSet::new();
            for x in &to_check {
                let c = current.get(x).unwrap();
                let t = target.get(x).unwrap();
                if c.needs_update(&t) {
                    to_update.insert(t);
                }
            }

            for x in &to_add {
                let mg = match mgd_clients.get(&x.switch) {
                    Some(mg) => mg,
                    None => {
                        error!(&log, "failed to get mg client";
                            "switch" => x.switch.to_string(),
                        );
                        continue;
                    }
                };
                if let Err(e) = mg
                    .add_bfd_peer(&BfdPeerConfig {
                        peer: x.remote,
                        detection_threshold: x.detection_threshold,
                        listen: x.local.unwrap_or(Ipv4Addr::UNSPECIFIED.into()),
                        mode: match x.mode {
                            BfdMode::SingleHop => SessionMode::SingleHop,
                            BfdMode::MultiHop => SessionMode::MultiHop,
                        },
                        required_rx: x.required_rx,
                    })
                    .await
                {
                    error!(&log, "failed to add bfd peer to switch daemon";
                        "error" => e.to_string(),
                        "switch" => x.switch.to_string(),
                    );
                }
            }

            for x in &to_del {
                let mg = match mgd_clients.get(&x.switch) {
                    Some(mg) => mg,
                    None => {
                        error!(&log, "failed to get mg client";
                            "switch" => x.switch.to_string(),
                        );
                        continue;
                    }
                };
                if let Err(e) = mg.remove_bfd_peer(&x.remote).await {
                    error!(&log, "failed to remove bfd peer from switch daemon";
                        "error" => e.to_string(),
                        "switch" => x.switch.to_string(),
                    );
                }
            }

            // TODO parameter updates
            // https://github.com/oxidecomputer/omicron/issues/4921

            json!({})
        }
        .boxed()
    }
}
