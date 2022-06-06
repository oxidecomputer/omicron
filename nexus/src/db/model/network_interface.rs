// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{MacAddr, VpcSubnet};
use crate::db::identity::Resource;
use crate::db::schema::network_interface;
use db_macros::Resource;
use omicron_common::api::external;
use uuid::Uuid;

#[derive(Selectable, Queryable, Insertable, Clone, Debug, Resource)]
#[diesel(table_name = network_interface)]
pub struct NetworkInterface {
    #[diesel(embed)]
    pub identity: NetworkInterfaceIdentity,

    pub instance_id: Uuid,
    pub vpc_id: Uuid,
    pub subnet_id: Uuid,
    pub mac: MacAddr,
    // TODO-correctness: We need to split this into an optional V4 and optional V6 address, at
    // least one of which will always be specified.
    //
    // If user requests an address of either kind, give exactly that and not the other.
    // If neither is specified, auto-assign one of each?
    pub ip: ipnetwork::IpNetwork,
    pub slot: i16,
    #[diesel(column_name = is_primary)]
    pub primary: bool,
}

impl From<NetworkInterface> for external::NetworkInterface {
    fn from(iface: NetworkInterface) -> Self {
        Self {
            identity: iface.identity(),
            instance_id: iface.instance_id,
            vpc_id: iface.vpc_id,
            subnet_id: iface.subnet_id,
            ip: iface.ip.ip(),
            mac: *iface.mac,
            primary: iface.primary,
        }
    }
}

/// A not fully constructed NetworkInterface. It may not yet have an IP
/// address allocated.
#[derive(Clone, Debug)]
pub struct IncompleteNetworkInterface {
    pub identity: NetworkInterfaceIdentity,
    pub instance_id: Uuid,
    pub vpc_id: Uuid,
    pub subnet: VpcSubnet,
    pub ip: Option<std::net::IpAddr>,
}

impl IncompleteNetworkInterface {
    pub fn new(
        interface_id: Uuid,
        instance_id: Uuid,
        vpc_id: Uuid,
        subnet: VpcSubnet,
        identity: external::IdentityMetadataCreateParams,
        ip: Option<std::net::IpAddr>,
    ) -> Result<Self, external::Error> {
        if let Some(ip) = ip {
            subnet.check_requestable_addr(ip)?;
        };
        let identity = NetworkInterfaceIdentity::new(interface_id, identity);
        Ok(Self { identity, instance_id, subnet, vpc_id, ip })
    }
}
