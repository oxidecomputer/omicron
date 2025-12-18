// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use dropshot::HttpErrorResponseBody;
use futures::prelude::*;
use gateway_messages::SpPort;
use gateway_test_utils::current_simulator_state;
use gateway_test_utils::setup;
use gateway_test_utils::sim_sp_serial_console;
use gateway_types::component::SpType;
use http::StatusCode;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::Role;

#[tokio::test]
async fn serial_console_communication() {
    let testctx =
        setup::test_setup("serial_console_communication", SpPort::One).await;
    let client = &testctx.client;
    let simrack = &testctx.simrack;

    // sanity check: we have at least 1 gimlet, and all SPs are enabled
    let sim_state = current_simulator_state(simrack).await;
    assert!(sim_state.iter().any(|sp| sp.ignition.id.typ == SpType::Sled));
    assert!(sim_state.iter().all(|sp| sp.state.is_ok()));

    // connect to sled 0's serial console
    let (console_write, mut console_read) =
        sim_sp_serial_console(&simrack.gimlets[0]).await;

    // connect to the MGS websocket for this gimlet
    let upgraded = client
        .sp_component_serial_console_attach(&SpType::Sled, 0, "sp3-host-cpu")
        .await
        .unwrap()
        .into_inner();
    let mut ws =
        WebSocketStream::from_raw_socket(upgraded, Role::Client, None).await;

    for i in 0..8 {
        let msg_from_mgs = format!("hello from MGS {}", i).into_bytes();
        let msg_from_sp = format!("hello from SP {}", i).into_bytes();

        // confirm messages sent to the websocket are received on the console
        // TCP connection
        ws.send(Message::Binary(msg_from_mgs.clone())).await.unwrap();
        assert_eq!(console_read.recv().await.unwrap(), msg_from_mgs);

        // confirm messages sent to the console TCP connection are received by
        // the websocket
        console_write.send(msg_from_sp.clone()).await.unwrap();
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::Binary(msg_from_sp)
        );
    }

    testctx.teardown().await;
}

#[tokio::test]
async fn serial_console_detach() {
    let testctx = setup::test_setup("serial_console_detach", SpPort::One).await;
    let client = &testctx.client;
    let simrack = &testctx.simrack;

    // sanity check: we have at least 1 gimlet, and all SPs are enabled
    let sim_state = current_simulator_state(simrack).await;
    assert!(sim_state.iter().any(|sp| sp.ignition.id.typ == SpType::Sled));
    assert!(sim_state.iter().all(|sp| sp.state.is_ok()));

    // connect to sled 0's serial console
    let (console_write, mut console_read) =
        sim_sp_serial_console(&simrack.gimlets[0]).await;

    // connect to the MGS websocket for this gimlet
    let upgraded = client
        .sp_component_serial_console_attach(&SpType::Sled, 0, "sp3-host-cpu")
        .await
        .unwrap()
        .into_inner();
    let mut ws =
        WebSocketStream::from_raw_socket(upgraded, Role::Client, None).await;

    // attempting to connect while the first connection is still open should
    // fail
    let err = client
        .sp_component_serial_console_attach(&SpType::Sled, 0, "sp3-host-cpu")
        .await
        .unwrap_err();
    let gateway_client::Error::UnexpectedResponse(response) = err else {
        panic!("unexpected error");
    };
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let err: HttpErrorResponseBody = response.json().await.unwrap();
    assert!(err.message.contains("serial console already attached"));

    // the original websocket should still work
    ws.send(Message::Binary(b"hello".to_vec())).await.unwrap();
    assert_eq!(console_read.recv().await.unwrap(), b"hello");
    console_write.send(b"world".to_vec()).await.unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap(),
        Message::Binary(b"world".to_vec())
    );

    // hit the detach endpoint, which should disconnect `ws`
    client
        .sp_component_serial_console_detach(&SpType::Sled, 0, "sp3-host-cpu")
        .await
        .unwrap();
    match ws.next().await {
        Some(Ok(Message::Close(Some(frame)))) => {
            assert_eq!(frame.reason, "serial console was detached");
        }
        other => panic!("unexpected websocket message {:?}", other),
    }

    // we should now be able to rettach
    let upgraded = client
        .sp_component_serial_console_attach(&SpType::Sled, 0, "sp3-host-cpu")
        .await
        .unwrap()
        .into_inner();
    let mut ws =
        WebSocketStream::from_raw_socket(upgraded, Role::Client, None).await;
    ws.send(Message::Binary(b"hello".to_vec())).await.unwrap();
    assert_eq!(console_read.recv().await.unwrap(), b"hello");
    console_write.send(b"world".to_vec()).await.unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap(),
        Message::Binary(b"world".to_vec())
    );

    testctx.teardown().await;
}
