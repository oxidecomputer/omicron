// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::context::OpContext;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::model::ApplySledFilterExt;
use crate::db::model::V2PMappingView;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel::{JoinOnDsl, NullableExpressionMethods};
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_model::{NetworkInterface, NetworkInterfaceKind, Sled, Vpc};
use nexus_types::deployment::SledFilter;
use omicron_common::api::external::ListResultVec;

impl DataStore {
    pub async fn v2p_mappings(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<V2PMappingView> {
        use nexus_db_schema::schema::instance::dsl as instance_dsl;
        use nexus_db_schema::schema::network_interface::dsl as network_interface_dsl;
        use nexus_db_schema::schema::probe::dsl as probe_dsl;
        use nexus_db_schema::schema::sled::dsl as sled_dsl;
        use nexus_db_schema::schema::vmm::dsl as vmm_dsl;
        use nexus_db_schema::schema::vpc::dsl as vpc_dsl;
        use nexus_db_schema::schema::vpc_subnet::dsl as vpc_subnet_dsl;

        use nexus_db_schema::schema::network_interface;

        opctx.check_complex_operations_allowed()?;

        // Query for instance v2p mappings
        let mut mappings = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch: Vec<_> =
                paginated(
                    network_interface_dsl::network_interface,
                    network_interface_dsl::id,
                    &p.current_pagparams(),
                )
                .inner_join(
                    instance_dsl::instance
                        .on(network_interface_dsl::parent_id
                            .eq(instance_dsl::id)),
                )
                .inner_join(vmm_dsl::vmm.on(
                    vmm_dsl::id.nullable().eq(instance_dsl::active_propolis_id),
                ))
                .inner_join(vpc_subnet_dsl::vpc_subnet.on(
                    vpc_subnet_dsl::id.eq(network_interface_dsl::subnet_id),
                ))
                .inner_join(
                    vpc_dsl::vpc
                        .on(vpc_dsl::id.eq(network_interface_dsl::vpc_id)),
                )
                .inner_join(
                    sled_dsl::sled.on(sled_dsl::id.eq(vmm_dsl::sled_id)),
                )
                .filter(network_interface::time_deleted.is_null())
                .filter(
                    network_interface::kind.eq(NetworkInterfaceKind::Instance),
                )
                .sled_filter(SledFilter::VpcRouting)
                .select((
                    NetworkInterface::as_select(),
                    Sled::as_select(),
                    Vpc::as_select(),
                ))
                .load_async(&*self.pool_connection_authorized(opctx).await?)
                .await
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
                .into_iter()
                .map(|(nic, sled, vpc): (NetworkInterface, Sled, Vpc)| {
                    V2PMappingView {
                        nic_id: nic.identity.id,
                        sled_id: sled.identity.id.into(),
                        sled_ip: sled.ip,
                        vni: vpc.vni,
                        mac: nic.mac,
                        ip: nic.ip,
                    }
                })
                .collect();

            paginator = p.found_batch(&batch, &|mapping| mapping.nic_id);
            mappings.extend(batch);
        }

        // Query for probe v2p mappings
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch: Vec<_> =
                paginated(
                    network_interface_dsl::network_interface,
                    network_interface_dsl::id,
                    &p.current_pagparams(),
                )
                .inner_join(
                    probe_dsl::probe
                        .on(probe_dsl::id.eq(network_interface_dsl::parent_id)),
                )
                .inner_join(vpc_subnet_dsl::vpc_subnet.on(
                    vpc_subnet_dsl::id.eq(network_interface_dsl::subnet_id),
                ))
                .inner_join(
                    vpc_dsl::vpc
                        .on(vpc_dsl::id.eq(network_interface_dsl::vpc_id)),
                )
                .inner_join(sled_dsl::sled.on(sled_dsl::id.eq(probe_dsl::sled)))
                .filter(network_interface::time_deleted.is_null())
                .filter(network_interface::kind.eq(NetworkInterfaceKind::Probe))
                .sled_filter(SledFilter::VpcRouting)
                .select((
                    NetworkInterface::as_select(),
                    Sled::as_select(),
                    Vpc::as_select(),
                ))
                .load_async(&*self.pool_connection_authorized(opctx).await?)
                .await
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
                .into_iter()
                .map(|(nic, sled, vpc): (NetworkInterface, Sled, Vpc)| {
                    V2PMappingView {
                        nic_id: nic.identity.id,
                        sled_id: sled.identity.id.into(),
                        sled_ip: sled.ip,
                        vni: vpc.vni,
                        mac: nic.mac,
                        ip: nic.ip,
                    }
                })
                .collect();

            paginator = p.found_batch(&batch, &|mapping| mapping.nic_id);
            mappings.extend(batch);
        }

        Ok(mappings)
    }
}
