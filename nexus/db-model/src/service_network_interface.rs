// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::MacAddr;
use crate::schema::service_network_interface;
use db_macros::Resource;
use uuid::Uuid;

#[derive(Selectable, Queryable, Insertable, Clone, Debug, Resource)]
#[diesel(table_name = service_network_interface)]
pub struct ServiceNetworkInterface {
    #[diesel(embed)]
    pub identity: ServiceNetworkInterfaceIdentity,

    pub service_id: Uuid,
    pub vpc_id: Uuid,
    pub subnet_id: Uuid,
    pub mac: MacAddr,
    pub ip: ipnetwork::IpNetwork,
    pub slot: i16,
}
