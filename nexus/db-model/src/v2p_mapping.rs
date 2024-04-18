use crate::schema::v2p_mapping_view;
use crate::{MacAddr, SqlU32};
use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Queryable, Selectable, Clone, Debug, Serialize, Deserialize)]
#[diesel(table_name = v2p_mapping_view)]
pub struct V2PMappingView {
    pub nic_id: Uuid,
    pub sled_id: Uuid,
    pub sled_ip: IpNetwork,
    pub vni: SqlU32,
    pub mac: MacAddr,
    pub ip: IpNetwork,
}
