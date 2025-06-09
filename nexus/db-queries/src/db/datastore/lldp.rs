// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`LldpLinkConfig`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db::model::LldpLinkConfig;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use ipnetwork::IpNetwork;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::Name;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

// The LLDP configuration has been defined as a leaf of the switch-port-settings
// tree, and is identified in the database with a UUID stored in that tree.
// Using the uuid as the target argument for the config operations would be
// reasonable, and similar in spirit to the link configuration operations.
//
// On the other hand, the neighbors are discovered on a configured link, but the
// data is otherwise completely independent of the configuration.  Furthermore,
// the questions answered by the neighbor information have to do with the
// physical connections between the Oxide rack and the upstream, datacenter
// switch.  Accordingly, it seems more appropriate to use the physical
// rack/switch/port triple to identify the port of interest for the neighbors
// query.
//
// For consistency across the lldp operations, all use rack/switch/port rather
// than the uuid.
impl DataStore {
    /// Look up the settings id for this port in the switch_port table by its
    /// rack/switch/port triple, and then use that id to look up the lldp
    /// config id in the switch_port_settings_link_config table.
    async fn lldp_config_id_get(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        port_name: Name,
    ) -> LookupResult<Uuid> {
        use nexus_db_schema::schema::switch_port;
        use nexus_db_schema::schema::switch_port::dsl as switch_port_dsl;
        use nexus_db_schema::schema::switch_port_settings_link_config;
        use nexus_db_schema::schema::switch_port_settings_link_config::dsl as config_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let port_settings_id: Uuid = switch_port_dsl::switch_port
            .filter(switch_port::rack_id.eq(rack_id))
            .filter(
                switch_port::switch_location.eq(switch_location.to_string()),
            )
            .filter(switch_port::port_name.eq(port_name.to_string()))
            .select(switch_port::port_settings_id)
            .limit(1)
            .first_async::<Option<Uuid>>(&*conn)
            .await
            .map_err(|_| {
                Error::not_found_by_name(ResourceType::SwitchPort, &port_name)
            })?
            .ok_or(Error::invalid_value(
                "settings",
                "switch port not yet configured".to_string(),
            ))?;

        let lldp_id: Uuid = config_dsl::switch_port_settings_link_config
            .filter(
                switch_port_settings_link_config::port_settings_id
                    .eq(port_settings_id),
            )
            .select(switch_port_settings_link_config::lldp_link_config_id)
            .limit(1)
            .first_async::<Option<Uuid>>(&*conn)
            .await
            .map_err(|_| {
                Error::not_found_by_id(
                    ResourceType::SwitchPortSettings,
                    &port_settings_id,
                )
            })?
            .ok_or(Error::invalid_value(
                "settings",
                "lldp not configured for this port".to_string(),
            ))?;
        Ok(lldp_id)
    }

    /// Fetch the current LLDP configuration settings for the link identified
    /// using the rack/switch/port triple.
    pub async fn lldp_config_get(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        port_name: Name,
    ) -> LookupResult<external::LldpLinkConfig> {
        use nexus_db_schema::schema::lldp_link_config;
        use nexus_db_schema::schema::lldp_link_config::dsl;

        let id = self
            .lldp_config_id_get(opctx, rack_id, switch_location, port_name)
            .await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        dsl::lldp_link_config
            .filter(lldp_link_config::id.eq(id))
            .select(LldpLinkConfig::as_select())
            .limit(1)
            .first_async::<LldpLinkConfig>(&*conn)
            .await
            .map(|config| config.into())
            .map_err(|e| {
                let msg = "failed to lookup lldp config by id";
                error!(opctx.log, "{msg}"; "error" => ?e);

                match e {
                    diesel::result::Error::NotFound => Error::not_found_by_id(
                        ResourceType::LldpLinkConfig,
                        &id,
                    ),
                    _ => Error::internal_error(msg),
                }
            })
    }

    /// Update the current LLDP configuration settings for the link identified
    /// using the rack/switch/port triple.  n.b.: each link is given an empty
    /// configuration structure at link creation time, so there are no
    /// lldp config create/delete operations.
    pub async fn lldp_config_update(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        port_name: Name,
        config: external::LldpLinkConfig,
    ) -> UpdateResult<()> {
        use nexus_db_schema::schema::lldp_link_config::dsl;

        let id = self
            .lldp_config_id_get(opctx, rack_id, switch_location, port_name)
            .await?;
        if id != config.id {
            return Err(external::Error::invalid_request(&format!(
                "id ({}) doesn't match provided config ({})",
                id, config.id
            )));
        }

        diesel::update(dsl::lldp_link_config)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(id))
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::enabled.eq(config.enabled),
                dsl::link_name.eq(config.link_name.map(|n| n.to_string())),
                dsl::link_description.eq(config.link_description.clone()),
                dsl::chassis_id.eq(config.chassis_id.clone()),
                dsl::system_name.eq(config.system_name.clone()),
                dsl::system_description.eq(config.system_description.clone()),
                dsl::management_ip.eq(config.management_ip.map(|a| IpNetwork::from(a)))))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|err| {
                error!(opctx.log, "lldp link config update failed"; "error" => ?err);
                public_error_from_diesel(err, ErrorHandler::Server)
            })?;
        Ok(())
    }
}
