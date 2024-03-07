use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Name;
use crate::db::model::SiloUtilization;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::BoolExpressionMethods;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use nexus_db_model::IpPoolRange;
use omicron_common::address::IpRange;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use ref_cast::RefCast;

impl DataStore {
    pub async fn silo_utilization_view(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
    ) -> Result<SiloUtilization, Error> {
        opctx.authorize(authz::Action::Read, authz_silo).await?;
        let silo_id = authz_silo.id();

        use db::schema::silo_utilization::dsl;
        dsl::silo_utilization
            .filter(dsl::silo_id.eq(silo_id))
            .select(SiloUtilization::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn silo_utilization_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<SiloUtilization> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::silo_utilization::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::silo_utilization, dsl::silo_id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::silo_utilization,
                dsl::silo_name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .select(SiloUtilization::as_select())
        .filter(
            dsl::silo_discoverable
                .eq(true)
                .or(dsl::cpus_allocated.gt(0))
                .or(dsl::memory_allocated.gt(0))
                .or(dsl::storage_allocated.gt(0)),
        )
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn ip_pool_allocated_count(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> Result<i64, Error> {
        opctx.authorize(authz::Action::Read, authz_pool).await?;

        use db::schema::external_ip;

        external_ip::table
            .filter(external_ip::ip_pool_id.eq(authz_pool.id()))
            .filter(external_ip::time_deleted.is_null())
            .select(external_ip::id)
            .count()
            // TODO: how much do I have to worry about this being bigger than
            // u32? it seems impossible, but do I have to handle it?
            .first_async::<i64>(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    // TODO: should this just return the vec of ranges and live in ip_pool.rs?
    // TODO: generally we never retrieve all of anything, how bad is that
    pub async fn ip_pool_total_capacity(
        &self,
        opctx: &OpContext,
        authz_pool: &authz::IpPool,
    ) -> Result<u128, Error> {
        opctx.authorize(authz::Action::Read, authz_pool).await?;
        opctx.authorize(authz::Action::ListChildren, authz_pool).await?;

        use db::schema::ip_pool_range;

        let ranges: Vec<IpPoolRange> = ip_pool_range::table
            .filter(ip_pool_range::ip_pool_id.eq(authz_pool.id()))
            .filter(ip_pool_range::time_deleted.is_null())
            .select(IpPoolRange::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_pool),
                )
            })?;

        Ok(ranges.iter().fold(0, |acc, r| acc + IpRange::from(r).len()))
    }
}

#[cfg(test)]
mod test {
    use crate::authz;
    use crate::db::datastore::test_utils::datastore_test;
    use nexus_db_model::{IpPool, IpPoolResource, IpPoolResourceType, Project};
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::external_api::params;
    use nexus_types::identity::Resource;
    use omicron_common::address::{IpRange, Ipv4Range, Ipv6Range};
    use omicron_common::api::external::{
        IdentityMetadataCreateParams, LookupType,
    };
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_ip_utilization() {
        let logctx = dev::test_setup_log("test_ip_utilization");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let authz_silo = opctx.authn.silo_required().unwrap();
        let project = Project::new(
            authz_silo.id(),
            params::ProjectCreate {
                identity: IdentityMetadataCreateParams {
                    name: "my-project".parse().unwrap(),
                    description: "".to_string(),
                },
            },
        );
        let (.., project) =
            datastore.project_create(&opctx, project).await.unwrap();

        // create an IP pool for the silo, add a range to it, and link it to the silo
        let identity = IdentityMetadataCreateParams {
            name: "my-pool".parse().unwrap(),
            description: "".to_string(),
        };
        let pool = datastore
            .ip_pool_create(&opctx, IpPool::new(&identity))
            .await
            .expect("Failed to create IP pool");
        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            pool.id(),
            LookupType::ById(pool.id()),
        );

        // capacity of zero because there are no ranges
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 0);

        let range = IpRange::V4(
            Ipv4Range::new(
                std::net::Ipv4Addr::new(10, 0, 0, 1),
                std::net::Ipv4Addr::new(10, 0, 0, 5),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &range)
            .await
            .expect("Could not add range");

        // now it has a capacity of 5 because we added the range
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 5);

        let link = IpPoolResource {
            ip_pool_id: pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: authz_silo.id(),
            is_default: true,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Could not link pool to silo");

        let ip_count = datastore
            .ip_pool_allocated_count(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(ip_count, 0);

        let identity = IdentityMetadataCreateParams {
            name: "my-ip".parse().unwrap(),
            description: "".to_string(),
        };
        let ip = datastore
            .allocate_floating_ip(&opctx, project.id(), identity, None, None)
            .await
            .expect("Could not allocate floating IP");
        assert_eq!(ip.ip.to_string(), "10.0.0.1/32");

        let ip_count = datastore
            .ip_pool_allocated_count(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(ip_count, 1);

        // allocating one has nothing to do with total capacity
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 5);

        let ipv6_range = IpRange::V6(
            Ipv6Range::new(
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 10),
                std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 1, 20),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ipv6_range)
            .await
            .expect("Could not add range");

        // now test with additional v6 range
        let max_ips = datastore
            .ip_pool_total_capacity(&opctx, &authz_pool)
            .await
            .unwrap();
        assert_eq!(max_ips, 5 + 11 + 65536);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
