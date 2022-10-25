// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;
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
        ServiceKind::InternalDNS,
        ServiceKind::Nexus,
        ServiceKind::Oximeter,
        ServiceKind::Dendrite,
        ServiceKind::Tfport,
    ];

    let mut rng = StdRng::from_entropy();

    for service_kind in all_variants {
        let rand: u128 = rng.gen();
        let request = ServicePutRequest {
            service_id: Uuid::new_v4(),
            sled_id: SLED_AGENT_UUID.parse().unwrap(),
            address: if matches!(service_kind, ServiceKind::Tfport) {
                None
            } else {
                Some(rand.into())
            },
            kind: service_kind,
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
}
