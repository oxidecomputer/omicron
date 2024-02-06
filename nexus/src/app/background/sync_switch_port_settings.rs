// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for managing switch port settings

use crate::app::sagas::switch_port_settings_common::api_to_dpd_port_settings;

use super::common::BackgroundTask;
use dpd_client::types::PortId;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::{
    context::OpContext,
    db::{datastore::SwitchPortSettingsCombinedResult, DataStore},
};
use nexus_types::identity::Resource;
use omicron_common::api::external::{DataPageParams, SwitchLocation};
use omicron_common::OMICRON_DPD_TAG;
use serde_json::json;
use std::{collections::HashMap, str::FromStr, sync::Arc};

const DPD_TAG: Option<&'static str> = Some(OMICRON_DPD_TAG);

pub struct SwitchPortSettingsManager {
    datastore: Arc<DataStore>,
    dpd_clients: HashMap<SwitchLocation, Arc<dpd_client::Client>>,
}

impl SwitchPortSettingsManager {
    pub fn new(
        datastore: Arc<DataStore>,
        dpd_clients: HashMap<SwitchLocation, Arc<dpd_client::Client>>,
    ) -> Self {
        Self { datastore, dpd_clients }
    }
}

enum PortSettingsChange {
    Apply(Box<SwitchPortSettingsCombinedResult>),
    Clear,
}

impl BackgroundTask for SwitchPortSettingsManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;

            let port_list = match self
                .datastore
                .switch_port_list(opctx, &DataPageParams::max_page())
                .await
            {
                Ok(port_list) => port_list,
                Err(e) => {
                    error!(
                        &log,
                        "failed to enumerate switch ports";
                        "error" => format!("{:#}", e)
                    );
                    return json!({
                        "error":
                            format!(
                                "failed enumerate switch ports: \
                                {:#}",
                                e
                            )
                    });
                }
            };

            let mut changes = Vec::new();

            for p in port_list {
                let id = match p.port_settings_id {
                    Some(id) => id,
                    _ => {
                        changes.push((p, PortSettingsChange::Clear));
                        continue;
                    }
                };

                let settings = match self
                    .datastore
                    .switch_port_settings_get(opctx, &id.into())
                    .await
                {
                    Ok(settings) => settings,
                    Err(e) => {
                        error!(
                            &log,
                            "failed to get switch port settings";
                            "switch_port_settings_id" => ?id,
                            "error" => format!("{:#}", e)
                        );
                        return json!({
                            "error":
                                format!(
                                    "failed to get switch port settings: \
                                    {:#}",
                                    e
                                )
                        });
                    }
                };

                changes.push((p, PortSettingsChange::Apply(Box::new(settings))));
            }

            for (switch_port, change) in changes {
                let location: SwitchLocation = match switch_port
                    .switch_location
                    .clone()
                    .parse()
                {
                    Ok(location) => location,
                    Err(e) => {
                        error!(
                            &log,
                            "failed to parse switch location";
                            "switch_location" => ?switch_port.switch_location,
                            "error" => ?e
                        );
                        continue;
                    }
                };

                let client = match self.dpd_clients.get(&location) {
                    Some(client) => client,
                    None => {
                        error!(
                            &log,
                            "no DPD client for switch location";
                            "switch_location" => ?location
                        );
                        continue;
                    }
                };

                let port_name = switch_port.port_name.clone();

                let dpd_port_id = match PortId::from_str(port_name.as_str()) {
                    Ok(port_id) => port_id,
                    Err(e) => {
                        error!(
                            &log,
                            "failed to parse switch port id";
                            "db_switch_port_name" => ?switch_port.port_name,
                            "switch_location" => ?location,
                            "error" => format!("{:#}", e)
                        );
                        continue;
                    }
                };

                match change {
                    PortSettingsChange::Apply(settings) => {
                        let dpd_port_settings = match api_to_dpd_port_settings(
                            &settings,
                        ) {
                            Ok(settings) => settings,
                            Err(e) => {
                                error!(
                                    &log,
                                    "failed to convert switch port settings";
                                    "switch_port_id" => ?port_name,
                                    "switch_location" => ?location,
                                    "switch_port_settings_id" => ?settings.settings.id(),
                                    "error" => format!("{:#}", e)
                                );
                                continue;
                            }
                        };

                        // apply settings via dpd client
                        match client
                            .port_settings_apply(
                                &dpd_port_id,
                                DPD_TAG,
                                &dpd_port_settings,
                            )
                            .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!(
                                    &log,
                                    "failed to apply switch port settings";
                                    "switch_port_id" => ?port_name,
                                    "switch_location" => ?location,
                                    "error" => format!("{:#}", e)
                                );
                            }
                        }
                    }
                    PortSettingsChange::Clear => {
                        // clear settings via dpd client
                        match client.port_settings_clear(&dpd_port_id, DPD_TAG).await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!(
                                    &log,
                                    "failed to clear switch port settings";
                                    "switch_port_id" => ?port_name,
                                    "switch_location" => ?location,
                                    "error" => format!("{:#}", e)
                                );
                            }
                        }
                    }
                }
            }
            json!({})
        }
        .boxed()
    }
}
