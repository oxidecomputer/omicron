use crate::{Ipv6Addr, MacAddr, Vni};
use ipnetwork::IpNetwork;
use omicron_uuid_kinds::SledUuid;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// This is not backed by an actual database view,
/// but it is still essentially a "view" in the sense
/// that it is a read-only data model derived from
/// multiple db tables
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct V2PMappingView {
    pub nic_id: Uuid,
    pub sled_id: SledUuid,
    pub sled_ip: Ipv6Addr,
    pub vni: Vni,
    pub mac: MacAddr,
    pub ip: IpNetwork,
}
