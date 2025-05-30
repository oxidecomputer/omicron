// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2025 Oxide Computer Company

use dropshot::test_util;
use gateway_messages::SpPort;
use gateway_test_utils::current_simulator_state;
use gateway_test_utils::setup;
use gateway_types::component::SpType;
use omicron_uuid_kinds::GenericUuid;
use std::sync::LazyLock;
use uuid::Uuid;

struct EreportRequest {
    sled: usize,
    restart_id: Uuid,
    start_ena: u64,
    committed_ena: Option<u64>,
    limit: usize,
}

impl EreportRequest {
    async fn response(
        self,
        client: &test_util::ClientTestContext,
    ) -> ereport_types::Ereports {
        let Self { sled, restart_id, start_ena, committed_ena, limit } = self;
        use std::fmt::Write;

        let base = client.url("/sp/sled");
        let mut url = format!(
            "{base}/{sled}/ereports?restart_id={restart_id}&start_at={start_ena}&limit={limit}"
        );
        if let Some(committed) = committed_ena {
            write!(&mut url, "&committed={committed}").unwrap();
        }
        // N.B. that we must use `ClientTestContext::make_request` rather than one
        // of the higher level helpers like `objects_post`, as our combination of
        // method and status code is a bit weird.
        let mut response = client
            .make_request::<()>(
                http::Method::POST,
                &url,
                None,
                http::StatusCode::OK,
            )
            .await
            .unwrap();
        test_util::read_json::<ereport_types::Ereports>(&mut response).await
    }
}

type JsonObject = serde_json::map::Map<String, serde_json::Value>;
macro_rules! def_ereport {
    ($NAME:ident: $($json:tt)+) => {
        pub(super) static $NAME: LazyLock<JsonObject> =
            LazyLock::new(|| match serde_json::json!($($json)+) {
                serde_json::Value::Object(obj) => obj,
                it => panic!(
                    "expected {} to be a JSON object, but found: {it:?}",
                    stringify!($NAME)
                ),
            });
    };
}

mod sled0 {
    use super::*;
    pub(super) static RESTART_0: LazyLock<Uuid> = LazyLock::new(|| {
        "af1ebf85-36ba-4c31-bbec-b9825d6d9d8b".parse().expect("is a valid UUID")
    });

    def_ereport! {
        EREPORT_1: {
            "chassis_model": "SimGimletSp",
            "chassis_serial": "SimGimlet00",
            "hubris_archive_id": "ffffffff",
            "hubris_version": "0.0.2",
            "hubris_task_name": "task_apollo_server",
            "hubris_task_gen": 13,
            "hubris_uptime_ms": 1233,
            "ereport_message_version": 0,
            "class": "gov.nasa.apollo.o2_tanks.stir.begin",
            "message": "stirring the tanks",
        }
    }
    def_ereport! {
        EREPORT_2: {
            "chassis_model": "SimGimletSp",
            "chassis_serial": "SimGimlet00",
            "hubris_archive_id": "ffffffff",
            "hubris_version": "0.0.2",
            "hubris_task_name": "drv_ae35_server",
            "hubris_task_gen": 1,
            "hubris_uptime_ms": 1234,
            "ereport_message_version": 0,
            "class": "io.discovery.ae35.fault",
            "message": "i've just picked up a fault in the AE-35 unit",
            "de": {
                "scheme": "fmd",
                "mod-name": "ae35-diagnosis",
                "authority": {
                    "product-id": "HAL-9000-series computer",
                    "server-id": "HAL 9000",
                },
            },
            "hours_to_failure": 72,
        }
    }
    def_ereport! {
        EREPORT_3: {
            "chassis_model": "SimGimletSp",
            "chassis_serial": "SimGimlet00",
            "hubris_archive_id": "ffffffff",
            "hubris_version": "0.0.2",
            "hubris_task_name": "task_apollo_server",
            "hubris_task_gen": 13,
            "hubris_uptime_ms": 1237,
            "ereport_message_version": 0,
            "class": "gov.nasa.apollo.fault",
            "message": "houston, we have a problem",
            "crew": [
                "Lovell",
                "Swigert",
                "Haise",
            ],
        }
    }

    def_ereport! {
        EREPORT_4: {
            "chassis_model": "SimGimletSp",
            "chassis_serial": "SimGimlet00",
            "hubris_archive_id": "ffffffff",
            "hubris_version": "0.0.2",
            "hubris_task_name": "drv_thingy_server",
            "hubris_task_gen": 2,
            "hubris_uptime_ms": 1240,
            "ereport_message_version": 0,
            "class": "flagrant_error",
            "computer": false,
        }
    }

    def_ereport! {
        EREPORT_5: {
            "chassis_model": "SimGimletSp",
            "chassis_serial": "SimGimlet00",
            "hubris_archive_id": "ffffffff",
            "hubris_version": "0.0.2",
            "hubris_task_name": "task_latex_server",
            "hubris_task_gen": 1,
            "hubris_uptime_ms": 1245,
            "ereport_message_version": 0,
            "class": "overfull_hbox",
            "badness": 10000,
        }
    }
}

mod sled1 {
    use super::*;
    pub(super) static RESTART_0: LazyLock<Uuid> = LazyLock::new(|| {
        "55e30cc7-a109-492f-aca9-735ed725df3c".parse().expect("is a valid UUID")
    });

    def_ereport! {
        EREPORT_1: {
            "chassis_model": "SimGimletSp",
            "chassis_serial": "SimGimlet01",
            "hubris_archive_id": "ffffffff",
            "hubris_version": "0.0.2",
            "hubris_task_name": "task_thermal_server",
            "hubris_task_gen": 1,
            "hubris_uptime_ms": 1233,
            "ereport_message_version": 0,
            "class": "computer.oxide.gimlet.chassis_integrity.fault",
            "nosub_class": "chassis_integrity.cat_hair_detected",
            "message": "cat hair detected inside gimlet",
            "de": {
                "scheme": "fmd",
                "mod-name": "hubris-thermal-diagnosis",
                "mod-version": "1.0",
                "authority": {
                    "product-id": "oxide",
                    "server-id": "SimGimlet1",
                }
            },
            "certainty": 0x64,
            "cat_hair_amount": 10000,
        }
    }
}

/// Double check that we have at least 1 sidecar and 2 gimlets and that all
/// SPs are enabled.
async fn check_sim_state(
    simrack: &sp_sim::SimRack,
) -> Vec<gateway_test_utils::SpInfo> {
    let sim_state = current_simulator_state(simrack).await;
    assert!(
        sim_state
            .iter()
            .filter(|sp| sp.ignition.id.typ == SpType::Sled)
            .count()
            >= 2
    );
    assert!(sim_state.iter().any(|sp| sp.ignition.id.typ == SpType::Switch));
    assert!(sim_state.iter().all(|sp| sp.state.is_ok()));
    sim_state
}

#[tokio::test]
async fn ereports_basic() {
    let testctx = setup::test_setup("ereports_basic", SpPort::One).await;
    let client = &testctx.client;
    let simrack = &testctx.simrack;
    check_sim_state(simrack).await;

    let ereport_types::Ereports { restart_id, reports } = dbg!(
        EreportRequest {
            sled: 1,
            restart_id: Uuid::new_v4(),
            start_ena: 0,
            committed_ena: None,
            limit: 100
        }
        .response(client)
        .await
    );

    assert_eq!(restart_id.as_untyped_uuid(), &*sled1::RESTART_0);
    let reports = reports.items;
    assert_eq!(reports.len(), 1, "expected 1 ereport, found: {:#?}", reports);
    let report = &reports[0];
    assert_eq!(report.ena, ereport_types::Ena(1));
    assert_eq!(report.data, *sled1::EREPORT_1);

    testctx.teardown().await;
}

#[tokio::test]
async fn ereports_limit() {
    let testctx = setup::test_setup("ereports_limit", SpPort::One).await;
    let client = &testctx.client;
    let simrack = &testctx.simrack;
    check_sim_state(simrack).await;

    let ereport_types::Ereports { restart_id, reports } = dbg!(
        EreportRequest {
            sled: 0,
            restart_id: Uuid::new_v4(),
            start_ena: 0,
            committed_ena: None,
            limit: 2
        }
        .response(client)
        .await
    );

    assert_eq!(restart_id.as_untyped_uuid(), &*sled0::RESTART_0);
    let reports = reports.items;
    assert_eq!(reports.len(), 2, "expected 2 ereports, found: {:#?}", reports);
    let report = &reports[0];
    assert_eq!(report.ena, ereport_types::Ena(1));
    assert_eq!(report.data, *sled0::EREPORT_1);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(2));
    assert_eq!(report.data, *sled0::EREPORT_2);

    let ereport_types::Ereports { restart_id, reports } = dbg!(
        EreportRequest {
            sled: 0,
            restart_id: restart_id.into_untyped_uuid(),
            start_ena: 3,
            committed_ena: None,
            limit: 2
        }
        .response(client)
        .await
    );

    assert_eq!(restart_id.as_untyped_uuid(), &*sled0::RESTART_0);
    let reports = reports.items;
    assert_eq!(reports.len(), 2, "expected 2 ereports, found: {:#?}", reports);
    let report = &reports[0];
    assert_eq!(report.ena, ereport_types::Ena(3));
    assert_eq!(report.data, *sled0::EREPORT_3);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(4));
    assert_eq!(report.data, *sled0::EREPORT_4);

    testctx.teardown().await;
}

#[tokio::test]
async fn ereports_commit() {
    let testctx = setup::test_setup("ereports_limit", SpPort::One).await;
    let client = &testctx.client;
    let simrack = &testctx.simrack;
    check_sim_state(simrack).await;

    let ereport_types::Ereports { restart_id, reports } = dbg!(
        EreportRequest {
            sled: 0,
            restart_id: Uuid::new_v4(),
            start_ena: 3,
            committed_ena: Some(2),
            limit: 2
        }
        .response(client)
        .await
    );

    // No ereports should be discarded by a request with a non-matching restart
    // ID.
    assert_eq!(restart_id.as_untyped_uuid(), &*sled0::RESTART_0);
    let reports = reports.items;
    assert_eq!(reports.len(), 2, "expected 2 ereports, found: {:#?}", reports);
    let report = &reports[0];
    assert_eq!(report.ena, ereport_types::Ena(1));
    assert_eq!(report.data, *sled0::EREPORT_1);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(2));
    assert_eq!(report.data, *sled0::EREPORT_2);

    // Now, send a request with a committed ENA *and* a matching restart ID.
    let ereport_types::Ereports { restart_id, reports } = dbg!(
        EreportRequest {
            sled: 0,
            restart_id: restart_id.into_untyped_uuid(),
            start_ena: 0,
            committed_ena: Some(2),
            limit: 2
        }
        .response(client)
        .await
    );

    assert_eq!(restart_id.as_untyped_uuid(), &*sled0::RESTART_0);
    let reports = reports.items;
    assert_eq!(reports.len(), 2, "expected 2 ereports, found: {:#?}", reports);
    let report = &reports[0];
    assert_eq!(report.ena, ereport_types::Ena(3));
    assert_eq!(report.data, *sled0::EREPORT_3);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(4));
    assert_eq!(report.data, *sled0::EREPORT_4);

    // Even if the start ENA of a subsequent request is 0, we shouldn't see any
    // ereports with ENAs lower than the committed ENA.
    let ereport_types::Ereports { restart_id, reports } = dbg!(
        EreportRequest {
            sled: 0,
            restart_id: restart_id.into_untyped_uuid(),
            start_ena: 0,
            committed_ena: None,
            limit: 100
        }
        .response(client)
        .await
    );

    assert_eq!(restart_id.as_untyped_uuid(), &*sled0::RESTART_0);
    let reports = reports.items;
    assert_eq!(reports.len(), 3, "expected 3 ereports, found: {:#?}", reports);
    let report = &reports[0];
    assert_eq!(report.ena, ereport_types::Ena(3));
    assert_eq!(report.data, *sled0::EREPORT_3);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(4));
    assert_eq!(report.data, *sled0::EREPORT_4);

    let report = &reports[2];
    assert_eq!(report.ena, ereport_types::Ena(5));
    assert_eq!(report.data, *sled0::EREPORT_5);

    testctx.teardown().await;
}
