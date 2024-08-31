use super::Generation;
use crate::schema::{
    internet_gateway, internet_gateway_ip_address, internet_gateway_ip_pool,
};
use crate::DatastoreCollectionConfig;
use db_macros::Resource;
use ipnetwork::IpNetwork;
use nexus_types::external_api::{params, views};
use nexus_types::identity::Resource;
use omicron_common::api::external::IdentityMetadataCreateParams;
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

impl InternetGateway {
    pub fn new(
        gateway_id: Uuid,
        vpc_id: Uuid,
        params: params::InternetGatewayCreate,
    ) -> Self {
        let identity =
            InternetGatewayIdentity::new(gateway_id, params.identity);
        Self { identity, vpc_id, rcgen: Generation::new(), resolved_version: 0 }
    }
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
    identity: InternetGatewayIpPoolIdentity,

    pub internet_gateway_id: Uuid,
    pub ip_pool_id: Uuid,
}

impl InternetGatewayIpPool {
    pub fn new(
        id: Uuid,
        ip_pool_id: Uuid,
        internet_gateway_id: Uuid,
        identity: IdentityMetadataCreateParams,
    ) -> Self {
        let identity = InternetGatewayIpPoolIdentity::new(id, identity);
        //InternetGatewayIpPoolIdentity::new(pool_id, params.identity);
        Self { identity, internet_gateway_id, ip_pool_id }
    }
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
    identity: InternetGatewayIpAddressIdentity,

    pub internet_gateway_id: Uuid,
    pub address: IpNetwork,
}

impl InternetGatewayIpAddress {
    pub fn new(
        pool_id: Uuid,
        internet_gateway_id: Uuid,
        params: params::InternetGatewayIpAddressCreate,
    ) -> Self {
        let identity =
            InternetGatewayIpAddressIdentity::new(pool_id, params.identity);
        Self {
            identity,
            internet_gateway_id,
            address: IpNetwork::from(params.address),
        }
    }
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

impl DatastoreCollectionConfig<InternetGatewayIpPool> for InternetGateway {
    type CollectionId = Uuid;
    type GenerationNumberColumn = internet_gateway::dsl::rcgen;
    type CollectionTimeDeletedColumn = internet_gateway::dsl::time_deleted;
    type CollectionIdColumn =
        internet_gateway_ip_pool::dsl::internet_gateway_id;
}

impl DatastoreCollectionConfig<InternetGatewayIpAddress> for InternetGateway {
    type CollectionId = Uuid;
    type GenerationNumberColumn = internet_gateway::dsl::rcgen;
    type CollectionTimeDeletedColumn = internet_gateway::dsl::time_deleted;
    type CollectionIdColumn =
        internet_gateway_ip_address::dsl::internet_gateway_id;
}
