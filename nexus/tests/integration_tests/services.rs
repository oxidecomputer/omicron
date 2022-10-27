// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;
use omicron_nexus::context::OpContext;
use omicron_nexus::db::identity::Asset;
use omicron_nexus::internal_api::params::{ServiceKind, ServicePutRequest};
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use uuid::Uuid;

use nexus_test_utils::{ControlPlaneTestContext, SLED_AGENT_UUID};
use nexus_test_utils_macros::nexus_test;

#[nexus_test]
async fn test_service_put_success(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.internal_client;

    let all_variants = [
        (ServiceKind::InternalDNS, Uuid::new_v4()),
        (ServiceKind::Nexus, Uuid::new_v4()),
        (ServiceKind::Oximeter, Uuid::new_v4()),
        (ServiceKind::Dendrite, Uuid::new_v4()),
        (ServiceKind::Tfport, Uuid::new_v4()),
    ];

    let mut rng = StdRng::from_entropy();

    for (service_kind, service_id) in &all_variants {
        let rand: u128 = rng.gen();
        let request = ServicePutRequest {
            service_id: *service_id,
            sled_id: SLED_AGENT_UUID.parse().unwrap(),
            address: if matches!(service_kind, ServiceKind::Tfport) {
                None
            } else {
                Some(rand.into())
            },
            kind: *service_kind,
        };
        client
            .make_request(
                Method::PUT,
                "/service",
                Some(request),
                StatusCode::NO_CONTENT,
            )
            .await
            .unwrap();
    }

    // Validate that the services exist in the DB
    let nexus = &cptestctx.server.apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let sled_services = datastore
        .sled_services(&opctx, SLED_AGENT_UUID.parse().unwrap())
        .await
        .unwrap();

    for (_, service_id) in &all_variants {
        assert!(sled_services
            .iter()
            .any(|service| service.id() == *service_id));
    }
}
