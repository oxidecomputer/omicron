// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Service`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::identity::Asset;
use crate::db::model::Service;
use crate::db::model::ServiceKind;
use crate::db::model::Sled;
use crate::db::pool::DbConnection;
use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl};
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_types::identity::Resource;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::ReservedRackSubnet;
use omicron_common::address::DNS_REDUNDANCY;
use omicron_common::address::RACK_PREFIX;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use std::net::Ipv6Addr;
use uuid::Uuid;

impl DataStore {
    /// Stores a new service in the database.
    pub async fn service_upsert(
        &self,
        opctx: &OpContext,
        service: Service,
    ) -> CreateResult<Service> {
        use db::schema::service::dsl;

        let sled_id = service.sled_id;
        Sled::insert_resource(
            sled_id,
            diesel::insert_into(dsl::service)
                .values(service.clone())
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::sled_id.eq(excluded(dsl::sled_id)),
                    dsl::ip.eq(excluded(dsl::ip)),
                    dsl::kind.eq(excluded(dsl::kind)),
                )),
        )
        .insert_and_get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Sled,
                lookup_type: LookupType::ById(sled_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Service,
                        &service.id().to_string(),
                    ),
                )
            }
        })
    }

    async fn service_upsert_on_connection(
        conn: &async_bb8_diesel::Connection<crate::db::pool::DbConnection>,
        service: Service,
    ) -> Result<Service, Error> {
        use db::schema::service::dsl;

        let sled_id = service.sled_id;
        Sled::insert_resource(
            sled_id,
            diesel::insert_into(dsl::service)
                .values(service.clone())
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::sled_id.eq(excluded(dsl::sled_id)),
                    dsl::ip.eq(excluded(dsl::ip)),
                    dsl::kind.eq(excluded(dsl::kind)),
                )),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Sled,
                lookup_type: LookupType::ById(sled_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Service,
                        &service.id().to_string(),
                    ),
                )
            }
        })
    }

    async fn sled_list_with_limit_on_connection(
        conn: &async_bb8_diesel::Connection<crate::db::pool::DbConnection>,
        limit: u32,
    ) -> Result<Vec<Sled>, async_bb8_diesel::ConnectionError> {
        use db::schema::sled::dsl;
        dsl::sled
            .filter(dsl::time_deleted.is_null())
            .limit(limit as i64)
            .select(Sled::as_select())
            .load_async(conn)
            .await
    }

    pub async fn service_list(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
    ) -> Result<Vec<Service>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::service::dsl;
        dsl::service
            .filter(dsl::sled_id.eq(sled_id))
            .select(Service::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    // List all sleds on a rack, with info about provisioned services of a
    // particular type.
    async fn sled_and_service_list(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: Uuid,
        kind: ServiceKind,
    ) -> Result<Vec<(Sled, Option<Service>)>, async_bb8_diesel::ConnectionError>
    {
        use db::schema::service::dsl as svc_dsl;
        use db::schema::sled::dsl as sled_dsl;

        db::schema::sled::table
            .filter(sled_dsl::time_deleted.is_null())
            .filter(sled_dsl::rack_id.eq(rack_id))
            .left_outer_join(db::schema::service::table.on(
                svc_dsl::sled_id.eq(sled_dsl::id).and(svc_dsl::kind.eq(kind)),
            ))
            .select(<(Sled, Option<Service>)>::as_select())
            .get_results_async(conn)
            .await
    }

    /// Ensures that all Scrimlets in `rack_id` have the `kind` service
    /// provisioned.
    ///
    /// TODO: Returns what?
    pub async fn ensure_scrimlet_service(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        kind: ServiceKind,
    ) -> Result<Vec<Service>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        type TxnError = TransactionError<Error>;

        self.pool()
            .transaction_async(|conn| async move {
                let sleds_and_maybe_svcs =
                    Self::sled_and_service_list(&conn, rack_id, kind).await?;

                // Split the set of returned sleds into "those with" and "those
                // without" the requested service.
                let (sleds_with_svc, sleds_without_svc): (Vec<_>, Vec<_>) =
                    sleds_and_maybe_svcs
                        .into_iter()
                        .partition(|(_, maybe_svc)| maybe_svc.is_some());

                // Identify sleds without services (targets for future
                // allocation).
                let sleds_without_svc =
                    sleds_without_svc.into_iter().map(|(sled, _)| sled);

                // Identify sleds with services (part of output).
                let mut svcs: Vec<_> = sleds_with_svc
                    .into_iter()
                    .map(|(_, maybe_svc)| {
                        maybe_svc.expect(
                            "Should have filtered by sleds with the service",
                        )
                    })
                    .collect();

                // Add this service to all scrimlets without it.
                for sled in sleds_without_svc {
                    if sled.is_scrimlet() {
                        let svc_id = Uuid::new_v4();
                        let address = Self::next_ipv6_address_on_connection(
                            &conn,
                            sled.id(),
                        )
                        .await
                        .map_err(|e| TxnError::CustomError(e))?;

                        let service = db::model::Service::new(
                            svc_id,
                            sled.id(),
                            address,
                            kind,
                        );

                        let svc =
                            Self::service_upsert_on_connection(&conn, service)
                                .await
                                .map_err(|e| TxnError::CustomError(e))?;
                        svcs.push(svc);
                    }
                }

                return Ok(svcs);
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(e) => e,
                TxnError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })
    }

    /// Ensures that `redundancy` sleds within `rack_id` have the `kind` service
    /// provisioned.
    ///
    /// Returns all services which have been allocated within the rack.
    pub async fn ensure_rack_service(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        kind: ServiceKind,
        redundancy: u32,
    ) -> Result<Vec<Service>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;

        #[derive(Debug)]
        enum ServiceError {
            NotEnoughSleds,
            Other(Error),
        }
        type TxnError = TransactionError<ServiceError>;

        // NOTE: We could also make parts of this a saga?
        //
        // - TODO: DON'T mark/unmark as rebalancing!!!!!!
        //     - Use rcgen!!! It's what it's for - optimistic concurrency control.
        //     - Basically, create a new rcgen for Nexus to use, bail if
        //     somone else increments past us? *That* can be stored on the rack
        //     table.
        //   TODO: alternatively, this whole thing is happening in the DB.
        //   We *could* issue a CTE.
        //
        //
        // - List sleds + services, return sleds with/without services
        // - Pick sleds that are targets probably all up-front
        // - FOR EACH
        //  - Provision IPv6
        //  - Upsert service record
        //  - IF NEXUS
        //      - Provision external IP
        //      - Find cert
        //      - Upsert nexus service record

        // NOTE: It's probably possible to do this without the transaction.
        //
        // Something like this - heavily inspired by the external IP allocation
        // CTE:
        //
        // WITH
        // existing_count AS (
        //     SELECT COUNT(1) FROM services WHERE allocated AND not deleted
        // ),
        // new_count AS (
        //     -- Use "GREATEST" to avoid underflow if we've somehow
        //     -- over-allocated services beyond the redundancy.
        //     GREATEST(<redundancy>, existing_count) - existing_count
        // ),
        // candidate_sleds AS (
        //     SELECT all sleds in the allocation scope (in the rack?)
        //     LEFT OUTER JOIN with allocated services
        //     ON service_type
        //     WHERE service_type IS NULL (svc not allocated to the sled)
        //     LIMIT new_count
        // ),
        // new_internal_ips AS (
        //     UPDATE sled
        //     SET
        //          last_used_address = last_used_address + 1
        //     WHERE
        //          sled_id IN candidate_sleds
        //     RETURNING
        //          last_used_address
        // ),
        // new_external_ips AS (
        //     (need to insert the external IP allocation CTE here somehow)
        // ),
        // candidate_services AS (
        //     JOIN all the sleds with the IPs they need
        // ),
        // new_services AS (
        //     INSERT INTO services
        //     SELECT * FROM candidate_services
        //     ON CONFLICT (id)
        //     --- This doesn't actually work for the 'already exists' case, fyi
        //     DO NOTHING
        //     RETURNING *
        // ),
        self.pool()
            .transaction_async(|conn| async move {
                let sleds_and_maybe_svcs =
                    Self::sled_and_service_list(&conn, rack_id, kind)
                        .await
                        .map_err(|e| TxnError::Pool(e.into()))?;

                // Split the set of returned sleds into "those with" and "those
                // without" the requested service.
                let (sleds_with_svc, sleds_without_svc): (Vec<_>, Vec<_>) =
                    sleds_and_maybe_svcs
                        .into_iter()
                        .partition(|(_, maybe_svc)| maybe_svc.is_some());

                // Identify sleds without services (targets for future
                // allocation).
                let mut sleds_without_svc =
                    sleds_without_svc.into_iter().map(|(sled, _)| sled);

                // Identify sleds with services (part of output).
                let mut svcs: Vec<_> = sleds_with_svc
                    .into_iter()
                    .map(|(_, maybe_svc)| {
                        maybe_svc.expect(
                            "Should have filtered by sleds with the service",
                        )
                    })
                    .collect();

                // Add services to sleds, in-order, until we've met a
                // number sufficient for our redundancy.
                //
                // The selection of "which sleds run this service" is completely
                // arbitrary.
                while svcs.len() < (redundancy as usize) {
                    let sled = sleds_without_svc.next().ok_or_else(|| {
                        TxnError::CustomError(ServiceError::NotEnoughSleds)
                    })?;
                    let svc_id = Uuid::new_v4();

                    // TODO: With some work, you can get rid of the
                    // "...on_connection" versions of functions.
                    //
                    // See: https://github.com/oxidecomputer/omicron/pull/1621#discussion_r949796959
                    //
                    // TODO: I *strongly* believe this means Connection vs Pool
                    // error unification in async_bb8_diesel. *always* return
                    // the pool error; keep it simple.

                    // Always allocate an internal IP address to this service.
                    let address =
                        Self::next_ipv6_address_on_connection(&conn, sled.id())
                            .await
                            .map_err(|e| {
                                TxnError::CustomError(ServiceError::Other(
                                    e.into(),
                                ))
                            })?;

                    // If requested, allocate an external IP address for this
                    // service too.
                    let external_ip = if matches!(kind, ServiceKind::Nexus) {
                        let (.., pool) =
                            Self::ip_pools_lookup_by_rack_id_on_connection(
                                &conn, rack_id,
                            )
                            .await
                            .map_err(|e| {
                                TxnError::CustomError(ServiceError::Other(
                                    e.into(),
                                ))
                            })?;

                        let external_ip =
                            Self::allocate_service_ip_on_connection(
                                &conn,
                                Uuid::new_v4(),
                                pool.id(),
                            )
                            .await
                            .map_err(|e| {
                                TxnError::CustomError(ServiceError::Other(
                                    e.into(),
                                ))
                            })?;

                        Some(external_ip)
                    } else {
                        None
                    };

                    // TODO: We actually have to *use* the external_ip
                    // - Use the NexusCertificate table (look up by UUID)
                    // - Create a NexusService table (reference service, ip,
                    // certs)

                    let service = db::model::Service::new(
                        svc_id,
                        sled.id(),
                        address,
                        kind,
                    );

                    let svc =
                        Self::service_upsert_on_connection(&conn, service)
                            .await
                            .map_err(|e| {
                                TxnError::CustomError(ServiceError::Other(
                                    e.into(),
                                ))
                            })?;
                    svcs.push(svc);
                }

                return Ok(svcs);
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(ServiceError::NotEnoughSleds) => {
                    Error::unavail("Not enough sleds for service allocation")
                }
                TxnError::CustomError(ServiceError::Other(e)) => e,
                TxnError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })
    }

    /// Ensures that `redundancy` sleds within the `rack_subnet` have a DNS
    /// service provisioned.
    ///
    /// TODO: Returns what?
    pub async fn ensure_dns_service(
        &self,
        opctx: &OpContext,
        rack_subnet: Ipv6Subnet<RACK_PREFIX>,
        redundancy: u32,
    ) -> Result<Vec<Service>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        #[derive(Debug)]
        enum ServiceError {
            NotEnoughSleds,
            NotEnoughIps,
            Other(Error),
        }
        type TxnError = TransactionError<ServiceError>;

        self.pool()
            .transaction_async(|conn| async move {
                let mut svcs = Self::dns_service_list(&conn).await?;

                // Get all subnets not allocated to existing services.
                let mut usable_dns_subnets = ReservedRackSubnet(rack_subnet)
                    .get_dns_subnets()
                    .into_iter()
                    .filter(|subnet| {
                        // If any existing services are using this address,
                        // skip it.
                        !svcs.iter().any(|svc| {
                            Ipv6Addr::from(svc.ip) == subnet.dns_address().ip()
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter();

                // Get all sleds which aren't already running DNS services.
                let mut target_sleds =
                    Self::sled_list_with_limit_on_connection(&conn, redundancy)
                        .await?
                        .into_iter()
                        .filter(|sled| {
                            // The target sleds are only considered if they aren't already
                            // running a DNS service.
                            svcs.iter().all(|svc| svc.sled_id != sled.id())
                        })
                        .collect::<Vec<_>>()
                        .into_iter();

                while svcs.len() < (redundancy as usize) {
                    let sled = target_sleds.next().ok_or_else(|| {
                        TxnError::CustomError(ServiceError::NotEnoughSleds)
                    })?;
                    let svc_id = Uuid::new_v4();
                    let dns_subnet =
                        usable_dns_subnets.next().ok_or_else(|| {
                            TxnError::CustomError(ServiceError::NotEnoughIps)
                        })?;
                    let address = dns_subnet.dns_address().ip();
                    let service = db::model::Service::new(
                        svc_id,
                        sled.id(),
                        address,
                        ServiceKind::InternalDNS,
                    );

                    let svc =
                        Self::service_upsert_on_connection(&conn, service)
                            .await
                            .map_err(|e| {
                                TxnError::CustomError(ServiceError::Other(e))
                            })?;

                    svcs.push(svc);
                }
                return Ok(svcs);
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(ServiceError::NotEnoughSleds) => {
                    Error::unavail("Not enough sleds for service allocation")
                }
                TxnError::CustomError(ServiceError::NotEnoughIps) => {
                    Error::unavail(
                        "Not enough IP addresses for service allocation",
                    )
                }
                TxnError::CustomError(ServiceError::Other(e)) => e,
                TxnError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })
    }

    async fn dns_service_list(
        conn: &async_bb8_diesel::Connection<crate::db::pool::DbConnection>,
    ) -> Result<Vec<Service>, async_bb8_diesel::ConnectionError> {
        use db::schema::service::dsl as svc;

        svc::service
            .filter(svc::kind.eq(ServiceKind::InternalDNS))
            .limit(DNS_REDUNDANCY.into())
            .select(Service::as_select())
            .get_results_async(conn)
            .await
    }
}
