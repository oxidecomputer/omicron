use crate::app::authz;
use lldpd_client::types::ChassisId;
use lldpd_client::types::PortId;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::Error;
use omicron_common::api::external::LldpLinkConfig;
use omicron_common::api::external::LldpNeighbor;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::Name;
use omicron_common::api::external::SwitchLocation;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl super::Nexus {
    /// Lookup and return the LLDP config associated with the link identified
    /// using a rack/switch/port triple.
    pub(crate) async fn lldp_config_get(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        port: Name,
    ) -> LookupResult<LldpLinkConfig> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore
            .lldp_config_get(opctx, rack_id, switch_location, port)
            .await
    }

    /// Lookup the LLDP config associated with the link identified using a
    /// rack/switch/port triple, and update all fields in the database to match
    /// those in the provided struct.
    pub async fn lldp_config_update(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        port: Name,
        config: LldpLinkConfig,
    ) -> UpdateResult<()> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        self.db_datastore
            .lldp_config_update(opctx, rack_id, switch_location, port, config)
            .await?;

        // eagerly propagate changes via rpw
        self.background_tasks
            .activate(&self.background_tasks.task_switch_port_settings_manager);
        Ok(())
    }

    /// Query the LLDP daemon running on this rack/switch about all neighbors
    /// that have been identified on the specified port.
    pub async fn lldp_neighbors_get(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        port: Name,
    ) -> Result<Vec<LldpNeighbor>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        let loc: SwitchLocation =
            switch_location.as_str().parse().map_err(|e| {
                Error::invalid_request(&format!(
                    "invalid switch name {switch_location}: {e}"
                ))
            })?;

        let lldpd_clients = self.lldpd_clients(rack_id).await.map_err(|e| {
            Error::internal_error(&format!("lldpd clients get: {e}"))
        })?;

        let lldpd =
            lldpd_clients.get(&loc).ok_or(Error::internal_error(&format!(
                "no lldpd client for rack: {rack_id} switch {switch_location}"
            )))?;

        let neighbors = lldpd
            .get_neighbors()
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "failed to get neighbor list for {loc}/{port}: {e}"
                ))
            })?
            .into_inner();

        // The RFC defines several possible data classes for the port_id and
        // chassis_id TLVs.  There is no real semantic meaning associated with
        // the different types, other than describing how the binary payload
        // should be parsed.  The lldp client interface passes those values to
        // us in a type-specific enum.  Since there seems to be little value in
        // passing that complexity on to consumers of our API, we flatten these
        // fields into strings.
        Ok(neighbors
            .iter()
            .map(|n| LldpNeighbor {
                local_port: n.port.to_string(),
                first_seen: n.first_seen,
                last_seen: n.last_seen,
                link_name: match &n.system_info.port_id {
                    PortId::InterfaceAlias(s) => s.to_string(),
                    PortId::MacAddress(mac) => format!(
                        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                        mac.a[0],
                        mac.a[1],
                        mac.a[2],
                        mac.a[3],
                        mac.a[4],
                        mac.a[5]
                    ),
                    PortId::NetworkAddress(ip) => ip.to_string(),
                    PortId::InterfaceName(s) => s.to_string(),
                    PortId::PortComponent(s) => s.to_string(),
                    PortId::AgentCircuitId(s) => s.to_string(),
                    PortId::LocallyAssigned(s) => s.to_string(),
                },
                link_description: n.system_info.port_description.clone(),
                chassis_id: match &n.system_info.chassis_id {
                    ChassisId::ChassisComponent(s) => s.to_string(),
                    ChassisId::InterfaceAlias(s) => s.to_string(),
                    ChassisId::PortComponent(s) => s.to_string(),
                    ChassisId::MacAddress(mac) => format!(
                        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                        mac.a[0],
                        mac.a[1],
                        mac.a[2],
                        mac.a[3],
                        mac.a[4],
                        mac.a[5]
                    ),
                    ChassisId::NetworkAddress(ip) => ip.to_string(),
                    ChassisId::InterfaceName(s) => s.to_string(),
                    ChassisId::LocallyAssigned(s) => s.to_string(),
                },
                system_name: n.system_info.system_name.clone(),
                system_description: n.system_info.system_description.clone(),
                management_ip: n
                    .system_info
                    .management_addresses
                    .iter()
                    .map(|a| oxnet::IpNet::host_net(a.addr))
                    .collect(),
            })
            .collect())
    }
}
