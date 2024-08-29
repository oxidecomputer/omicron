use super::Generation;
use crate::schema::{
    internet_gateway, internet_gateway_ip_address, internet_gateway_ip_pool,
};
use db_macros::Resource;
use ipnetwork::IpNetwork;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
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

impl From<InternetGateway> for views::InternetGateway {
    fn from(value: InternetGateway) -> Self {
        Self { identity: value.identity(), vpc_id: value.vpc_id }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = internet_gateway_ip_pool)]
pub struct InternetGatewayIpPool {
    #[diesel(embed)]
    pub identity: InternetGatewayIpPoolIdentity,
    pub internet_gateway_id: Uuid,
    pub ip_pool_id: Uuid,
}

impl From<InternetGatewayIpPool> for views::InternetGatewayIpPool {
    fn from(value: InternetGatewayIpPool) -> Self {
        Self {
            identity: value.identity(),
            internet_gateway_id: value.internet_gateway_id,
            ip_pool_id: value.ip_pool_id,
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = internet_gateway_ip_address)]
pub struct InternetGatewayIpAddress {
    #[diesel(embed)]
    pub identity: InternetGatewayIpAddressIdentity,
    pub internet_gateway_id: Uuid,
    pub address: IpNetwork,
}

impl From<InternetGatewayIpAddress> for views::InternetGatewayIpAddress {
    fn from(value: InternetGatewayIpAddress) -> Self {
        Self {
            identity: value.identity(),
            internet_gateway_id: value.internet_gateway_id,
            address: value.address.ip(),
        }
    }
}
