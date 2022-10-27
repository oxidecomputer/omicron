// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;

use http::method::Method;
use http::StatusCode;
use omicron_nexus::context::OpContext;
use omicron_nexus::db::identity::Asset;
use omicron_nexus::db::model::Service;
use omicron_nexus::internal_api::params::{ServiceKind, ServicePutRequest};
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use uuid::Uuid;

use nexus_test_utils::{ControlPlaneTestContext, SLED_AGENT_UUID};
use nexus_test_utils_macros::nexus_test;

#[nexus_test]
async fn test_service_put_success(cptestctx: &ControlPlaneTestContext) {
    let mut rng = StdRng::from_entropy();
    let client = &cptestctx.internal_client;

    let internal_dns_service_id = Uuid::new_v4();
    let all_variants = [
        (
            ServiceKind::InternalDNS,
            internal_dns_service_id,
            Some(rng.gen::<u128>().into()),
            Some(rng.gen::<u16>()),
        ),
        (
            ServiceKind::Nexus,
            Uuid::new_v4(),
            Some(rng.gen::<u128>().into()),
            Some(rng.gen::<u16>()),
        ),
        (
            ServiceKind::Oximeter,
            Uuid::new_v4(),
            Some(rng.gen::<u128>().into()),
            Some(rng.gen::<u16>()),
        ),
        (
            ServiceKind::Dendrite,
            Uuid::new_v4(),
            Some(rng.gen::<u128>().into()),
            Some(rng.gen::<u16>()),
        ),
        (ServiceKind::Tfport, Uuid::new_v4(), None, None),
    ];

    for (service_kind, service_id, address, port) in &all_variants {
        let request = ServicePutRequest {
            service_id: *service_id,
            sled_id: SLED_AGENT_UUID.parse().unwrap(),
            address: *address,
            port: *port,
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

    for (_, service_id, _, _) in &all_variants {
        assert!(sled_services
            .iter()
            .any(|service| service.id() == *service_id));
    }

    // Validate the records were written in successfully
    let mut service_map: HashMap<Uuid, Service> = HashMap::default();
    for sled_service in &sled_services {
        service_map.insert(sled_service.id(), sled_service.clone());
    }

    for (service_kind, service_id, address, port) in &all_variants {
        let sled_service = service_map.get(service_id).unwrap();
        assert_eq!(sled_service.kind, (*service_kind).into());
        assert_eq!(sled_service.ip.map(|x| x.into()), *address);
        assert_eq!(sled_service.port.map(|x| x.into()), *port);
    }

    // Validate that a service can be updated
    let original_dns = service_map.get(&internal_dns_service_id).unwrap();

    let (new_ip, new_port) = loop {
        let new_ip = rng.gen::<u128>();
        let new_port = rng.gen::<u16>();

        let original_ip: std::net::Ipv6Addr = original_dns.ip.unwrap().into();

        if new_ip != original_ip.into() {
            break (new_ip, new_port);
        }
    };

    let request = ServicePutRequest {
        service_id: internal_dns_service_id,
        sled_id: SLED_AGENT_UUID.parse().unwrap(),
        address: Some(new_ip.into()),
        port: Some(new_port),
        kind: ServiceKind::InternalDNS,
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

    let updated_services = datastore
        .sled_services(&opctx, SLED_AGENT_UUID.parse().unwrap())
        .await
        .unwrap();

    let updated_dns = updated_services
        .iter()
        .filter(|service| service.id() == internal_dns_service_id)
        .collect::<Vec<&Service>>()[0];

    assert_ne!(original_dns.address(), updated_dns.address());
}
