use super::Generation;
use crate::schema::{
    internet_gateway, internet_gateway_addr, internet_gateway_pool,
};
use db_macros::Resource;
use ipnetwork::IpNetwork;
use uuid::Uuid;

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = internet_gateway)]
pub struct InternetGateway {
    #[diesel(embed)]
    identity: InternetGatewayIdentity,

    pub vpc_id: Uuid,
    pub rcgen: Generation,
    pub resolved_version: i64,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = internet_gateway_pool)]
pub struct InternetGatewayPool {
    pub gateway_id: Uuid,
    pub ip_pool_id: Uuid,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = internet_gateway_addr)]
pub struct InternetGatewayAddr {
    pub gateway_id: Uuid,
    pub addr: IpNetwork,
}
