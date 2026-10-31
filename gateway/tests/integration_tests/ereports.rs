// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use ereport_types::Ena;
use ereport_types::EreporterRestartUuid;
use gateway_ereport_messages::Request;
use gateway_ereport_messages::RequestIdV0;
use gateway_ereport_messages::RequestV0;
use gateway_ereport_messages::ResponseHeader;
use gateway_ereport_messages::ResponseHeaderV0;
use gateway_ereport_messages::RestartId;
use gateway_messages::SpPort;
use gateway_test_utils::current_simulator_state;
use gateway_test_utils::setup;
use gateway_types::component::SpType;
use omicron_uuid_kinds::GenericUuid;
use sp_sim::SimulatedSp;
use std::net::Ipv6Addr;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::net::UdpSocket;
use uuid::Uuid;
use zerocopy::IntoBytes;
use zerocopy::TryFromBytes;

struct EreportRequest {
    sled: u16,
    restart_id: EreporterRestartUuid,
    start_ena: Ena,
    committed_ena: Option<Ena>,
    limit: u32,
}

impl EreportRequest {
    async fn response(
        self,
        client: &gateway_client::Client,
    ) -> ereport_types::Ereports {
        let Self { sled, restart_id, start_ena, committed_ena, limit } = self;
        client
            .sp_ereports_ingest(
                &SpType::Sled,
                sled,
                committed_ena.as_ref(),
                limit.try_into().unwrap(),
                &restart_id,
                Some(&start_ena),
            )
            .await
            .unwrap()
            .into_inner()
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
        LOSS: {
            "baseboard_part_number": sp_sim::FAKE_GIMLET_MODEL,
            "baseboard_serial_number": "SimGimlet00",
            "hubris_caboose": {
                "board": sp_sim::SIM_GIMLET_BOARD,
                "commit": "ffffffff",
                "version": "0.0.2",
            },
            "hubris_task_name": "packrat",
            "hubris_task_gen": 0,
            "hubris_uptime_ms": 666,
            "ereport_message_version": 0,
            "lost": null,
        }
    }

    def_ereport! {
        EREPORT_1: {
            "baseboard_part_number": sp_sim::FAKE_GIMLET_MODEL,
            "baseboard_serial_number": "SimGimlet00",
            "hubris_caboose": {
                "board": sp_sim::SIM_GIMLET_BOARD,
                "commit": "ffffffff",
                "version": "0.0.2",
            },
            "hubris_task_name": "task_apollo_server",
            "hubris_task_gen": 13,
            "hubris_uptime_ms": 1233,
            "ereport_message_version": 0,
            "k": "gov.nasa.apollo.o2_tanks.stir.begin",
            "message": "stirring the tanks",
        }
    }
    def_ereport! {
        EREPORT_2: {
            "baseboard_part_number": sp_sim::FAKE_GIMLET_MODEL,
            "baseboard_serial_number": "SimGimlet00",
            "hubris_caboose": {
                "board": sp_sim::SIM_GIMLET_BOARD,
                "commit": "ffffffff",
                "version": "0.0.2",
            },
            "hubris_task_name": "drv_ae35_server",
            "hubris_task_gen": 1,
            "hubris_uptime_ms": 1234,
            "ereport_message_version": 0,
            "k": "io.discovery.ae35.fault",
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
            "baseboard_part_number": sp_sim::FAKE_GIMLET_MODEL,
            "baseboard_serial_number": "SimGimlet00",
            "hubris_caboose": {
                "board": sp_sim::SIM_GIMLET_BOARD,
                "commit": "ffffffff",
                "version": "0.0.2",
            },
            "hubris_task_name": "task_apollo_server",
            "hubris_task_gen": 13,
            "hubris_uptime_ms": 1237,
            "ereport_message_version": 0,
            "k": "gov.nasa.apollo.fault",
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
            "baseboard_part_number": sp_sim::FAKE_GIMLET_MODEL,
            "baseboard_serial_number": "SimGimlet00",
            "hubris_caboose": {
                "board": sp_sim::SIM_GIMLET_BOARD,
                "commit": "ffffffff",
                "version": "0.0.2",
            },
            "hubris_task_name": "drv_thingy_server",
            "hubris_task_gen": 2,
            "hubris_uptime_ms": 1240,
            "ereport_message_version": 0,
            "k": "flagrant_error",
            "computer": false,
        }
    }

    def_ereport! {
        EREPORT_5: {
            "baseboard_part_number": sp_sim::FAKE_GIMLET_MODEL,
            "baseboard_serial_number": "SimGimlet00",
            "hubris_caboose": {
                "board": sp_sim::SIM_GIMLET_BOARD,
                "commit": "ffffffff",
                "version": "0.0.2",
            },
            "hubris_task_name": "task_latex_server",
            "hubris_task_gen": 1,
            "hubris_uptime_ms": 1245,
            "ereport_message_version": 0,
            "k": "overfull_hbox",
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
        LOSS: {
            "baseboard_part_number": sp_sim::FAKE_GIMLET_MODEL,
            "baseboard_serial_number": "SimGimlet01",
            "hubris_caboose": {
                "board": sp_sim::SIM_GIMLET_BOARD,
                "commit": "ffffffff",
                "version": "0.0.2",
            },
            "hubris_task_name": "packrat",
            "hubris_task_gen": 0,
            "hubris_uptime_ms": 666,
            "ereport_message_version": 0,
            "lost": null,
        }
    }

    def_ereport! {
        EREPORT_1: {
            "baseboard_part_number": sp_sim::FAKE_GIMLET_MODEL,
            "baseboard_serial_number": "SimGimlet01",
            "hubris_caboose": {
                "board": sp_sim::SIM_GIMLET_BOARD,
                "commit": "ffffffff",
                "version": "0.0.2",
            },
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
            restart_id: EreporterRestartUuid::new_v4(),
            start_ena: Ena(0),
            committed_ena: None,
            limit: 100
        }
        .response(client)
        .await
    );

    assert_eq!(restart_id.as_untyped_uuid(), &*sled1::RESTART_0);
    let reports = reports.items;
    assert_eq!(reports.len(), 2, "expected 2 ereports, found: {:#?}", reports);

    let report = &reports[0];
    assert_eq!(report.ena, ereport_types::Ena(1));
    assert_eq!(report.data, *sled1::LOSS);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(2));
    assert_eq!(report.data, *sled1::EREPORT_1);

    testctx.teardown().await;
}

#[tokio::test]
async fn ereports_survive_malformed_requests() {
    let testctx =
        setup::test_setup("ereports_survive_malformed_requests", SpPort::One)
            .await;
    let client = &testctx.client;
    let simrack = &testctx.simrack;
    check_sim_state(simrack).await;

    let ereport_addr = simrack.gimlets[1]
        .local_ereport_addr(SpPort::One)
        .expect("simulated gimlet 1 has an ereport address");
    let sock = UdpSocket::bind((Ipv6Addr::LOCALHOST, 0))
        .await
        .expect("bound UDP socket on localhost");
    // Prevent accidental interference from other tests.
    //
    // UDP connect() causes datagrams from unrelated senders to be discarded.
    // From
    // https://pubs.opengroup.org/onlinepubs/9799919799/functions/connect.html:
    //
    // > If the initiating socket is not connection-mode, then connect() shall
    // > set the socket's peer address, and no connection is made. For SOCK_DGRAM
    // > sockets, the peer address identifies where all datagrams are sent on
    // > subsequent send() functions, and limits the remote sender for subsequent
    // > recv() functions.
    sock.connect(ereport_addr)
        .await
        .expect("connected UDP socket to the sim SP's ereport address");

    let ereport_request = |request_id| {
        Request::V0(RequestV0::new(
            RestartId::ZERO,
            request_id,
            gateway_ereport_messages::Ena::NONE,
            1,
            None,
        ))
    };

    // The probe has the correct, valid request ID.
    let probe_id = RequestIdV0(1);
    let probe = ereport_request(probe_id);

    // We use this as a base for constructing malformed packets. The request ID
    // deliberately differs from the probe's so that if a simulated SP wrongly
    // accepts a broken datagram, the header check below will fail (1).
    let corruption_template = ereport_request(RequestIdV0(2));
    let corruption_template = corruption_template.as_bytes();

    let mut too_long = corruption_template.to_vec();
    too_long.push(0);
    let mut bad_version = corruption_template.to_vec();
    bad_version[0] = 0xff;
    let malformed: &[(&str, &[u8])] = &[
        ("empty", &[]),
        ("truncated", &corruption_template[..corruption_template.len() - 1]),
        ("too long", &too_long),
        ("unknown version", &bad_version),
    ];

    // Send out the malformed packets.
    for &(description, packet) in malformed {
        sock.send(packet)
            .await
            .unwrap_or_else(|e| panic!("sent {description} packet: {e}"));
    }

    // Lastly, send out the probe (valid packet).
    //
    // Normally, UDP datagrams aren't guaranteed to be delivered in order, but
    // on a loopback address it would be strange not to do that. Therefore, if
    // we get a response to the probe, we can reasonably believe that the
    // simulated SP was able to handle the malformed datagrams without its UDP
    // task exiting.
    //
    // What if the datagrams were reordered, so that the probe was responded to,
    // but then one of the malformed packets caused the UDP task to exit? In
    // that case, the ereport fetch via MGS below would still catch a dead UDP
    // task (2).
    sock.send(probe.as_bytes()).await.expect("sent valid ereport request");
    let mut buf = [0u8; gateway_messages::MAX_SERIALIZED_SIZE];

    // This is the header check referred to in comment (1) above.
    let len =
        tokio::time::timeout(Duration::from_secs(60), sock.recv(&mut buf))
            .await
            .expect("sim SP responded to valid ereport request within 60s")
            .expect("received ereport response");
    let (header, _) = ResponseHeader::try_ref_from_prefix(&buf[..len])
        .expect("ereport response starts with a valid header");
    let ResponseHeader::V0(header) = header;
    assert_eq!(
        *header,
        ResponseHeaderV0 {
            request_id: probe_id,
            restart_id: RestartId::new(sled1::RESTART_0.as_u128()),
            start_ena: gateway_ereport_messages::Ena::new(1),
        },
    );

    // This is the ereport fetch via MGS referred to in comment (2) above.
    let ereport_types::Ereports { restart_id, reports } = EreportRequest {
        sled: 1,
        restart_id: EreporterRestartUuid::new_v4(),
        start_ena: Ena(0),
        committed_ena: None,
        limit: 100,
    }
    .response(client)
    .await;
    assert_eq!(restart_id.as_untyped_uuid(), &*sled1::RESTART_0);
    let reports = reports.items;
    assert_eq!(reports.len(), 2, "expected 2 ereports, found: {:#?}", reports);

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
            restart_id: EreporterRestartUuid::new_v4(),
            start_ena: Ena(0),
            committed_ena: None,
            limit: 3
        }
        .response(client)
        .await
    );

    assert_eq!(restart_id.as_untyped_uuid(), &*sled0::RESTART_0);
    let reports = reports.items;
    assert_eq!(reports.len(), 3, "expected 3 ereports, found: {:#?}", reports);

    let report = &reports[0];
    assert_eq!(report.ena, ereport_types::Ena(1));
    assert_eq!(report.data, *sled0::LOSS);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(2));
    assert_eq!(report.data, *sled0::EREPORT_1);

    let report = &reports[2];
    assert_eq!(report.ena, ereport_types::Ena(3));
    assert_eq!(report.data, *sled0::EREPORT_2);

    let ereport_types::Ereports { restart_id, reports } = dbg!(
        EreportRequest {
            sled: 0,
            restart_id,
            start_ena: Ena(3),
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
    assert_eq!(report.data, *sled0::EREPORT_2);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(4));
    assert_eq!(report.data, *sled0::EREPORT_3);

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
            restart_id: EreporterRestartUuid::new_v4(),
            start_ena: Ena(3),
            committed_ena: Some(Ena(2)),
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
    assert_eq!(report.data, *sled0::LOSS);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(2));
    assert_eq!(report.data, *sled0::EREPORT_1);

    // Now, send a request with a committed ENA *and* a matching restart ID.
    let ereport_types::Ereports { restart_id, reports } = dbg!(
        EreportRequest {
            sled: 0,
            restart_id,
            start_ena: Ena(0),
            committed_ena: Some(Ena(2)),
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
    assert_eq!(report.data, *sled0::EREPORT_2);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(4));
    assert_eq!(report.data, *sled0::EREPORT_3);

    // Even if the start ENA of a subsequent request is 0, we shouldn't see any
    // ereports with ENAs lower than the committed ENA.
    let ereport_types::Ereports { restart_id, reports } = dbg!(
        EreportRequest {
            sled: 0,
            restart_id,
            start_ena: Ena(0),
            committed_ena: None,
            limit: 100
        }
        .response(client)
        .await
    );

    assert_eq!(restart_id.as_untyped_uuid(), &*sled0::RESTART_0);
    let reports = reports.items;
    assert_eq!(reports.len(), 4, "expected 3 ereports, found: {:#?}", reports);
    let report = &reports[0];
    assert_eq!(report.ena, ereport_types::Ena(3));
    assert_eq!(report.data, *sled0::EREPORT_2);

    let report = &reports[1];
    assert_eq!(report.ena, ereport_types::Ena(4));
    assert_eq!(report.data, *sled0::EREPORT_3);

    let report = &reports[2];
    assert_eq!(report.ena, ereport_types::Ena(5));
    assert_eq!(report.data, *sled0::EREPORT_4);

    let report = &reports[3];
    assert_eq!(report.ena, ereport_types::Ena(6));
    assert_eq!(report.data, *sled0::EREPORT_5);

    testctx.teardown().await;
}
