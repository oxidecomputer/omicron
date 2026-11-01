// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_ereport_messages::Ena;
use gateway_ereport_messages::Request;
use gateway_ereport_messages::RequestIdV0;
use gateway_ereport_messages::RequestV0;
use gateway_ereport_messages::ResponseHeader;
use gateway_ereport_messages::ResponseHeaderV0;
use gateway_ereport_messages::RestartId;
use gateway_messages::SpPort;
use omicron_test_utils::dev::test_setup_log;
use sp_sim::SimRack;
use sp_sim::SimulatedSp;
use std::net::Ipv6Addr;
use std::time::Duration;
use tokio::net::UdpSocket;
use zerocopy::IntoBytes;
use zerocopy::TryFromBytes;

/// Path to the sp-sim configuration.
///
/// Thie is shared with gateway's integration tests.
const SP_SIM_CONFIG_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../gateway-test-utils/configs/sp_sim_config.test.toml"
);

fn ereport_request(request_id: RequestIdV0) -> Request {
    Request::V0(RequestV0::new(RestartId::ZERO, request_id, Ena::NONE, 1, None))
}

async fn send_probe_and_check_response(
    sock: &UdpSocket,
    probe_id: RequestIdV0,
    expected_restart_id: RestartId,
) {
    sock.send(ereport_request(probe_id).as_bytes())
        .await
        .expect("sent valid ereport request");

    let mut buf = [0u8; gateway_messages::MAX_SERIALIZED_SIZE];
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
            restart_id: expected_restart_id,
            start_ena: Ena::new(1),
        },
    );
}

#[tokio::test]
async fn ereports_survive_malformed_requests() {
    let logctx = test_setup_log("ereports_survive_malformed_requests");
    let config = sp_sim::Config::from_file(SP_SIM_CONFIG_PATH)
        .expect("loaded sp-sim test config");
    let simrack = SimRack::start(&config, &logctx.log)
        .await
        .expect("started simulated rack");

    let expected_restart_id = RestartId::new(
        config.simulated_sps.gimlet[1]
            .common
            .ereport_config
            .restart
            .restart_id
            .as_u128(),
    );
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

    // We use this as a base for constructing malformed packets. The request ID
    // deliberately differs from the probes' so that if a simulated SP wrongly
    // accepts a broken datagram, the send_probe_and_check_response calls below
    // will fail.
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
    // What if the datagrams _are_ reordered? To manage that unlikely situation,
    // we send a second probe out after the first response arrives. This probe
    // is guaranteed to occur after all the datagrams sent earlier have been
    // processed.
    send_probe_and_check_response(&sock, RequestIdV0(1), expected_restart_id)
        .await;
    send_probe_and_check_response(&sock, RequestIdV0(3), expected_restart_id)
        .await;

    logctx.cleanup_successful();
}
