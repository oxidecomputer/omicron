// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

use dropshot::test_util::ClientTestContext;
use gateway_test_utils::setup::GatewayTestContext;

pub struct WicketdTestContext {
    pub wicketd_addr: SocketAddrV6,
    pub wicketd_client: wicketd_client::Client,
    // This is not currently used but is kept here because it's easier to debug
    // this way.
    pub wicketd_raw_client: ClientTestContext,
    pub artifact_addr: SocketAddrV6,
    pub artifact_client: installinator_artifact_client::Client,
    pub server: wicketd::Server,
    pub gateway: GatewayTestContext,
}

impl WicketdTestContext {
    pub async fn setup(gateway: GatewayTestContext) -> Self {
        // Can't be `const` because `SocketAddrV6::new()` isn't const yet
        let localhost_port_0 = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);

        // Reuse the log from the gateway context.
        let log = &gateway.logctx.log;

        let mgs_address = assert_ipv6(
            gateway
                .server
                .dropshot_server_for_address(localhost_port_0)
                .unwrap()
                .local_addr(),
        );
        let args = wicketd::Args {
            address: localhost_port_0,
            artifact_address: localhost_port_0,
            mgs_address,
            nexus_proxy_address: localhost_port_0,
            baseboard: None,
            rack_subnet: None,
        };

        let server = wicketd::Server::start(log.clone(), args)
            .await
            .expect("error starting wicketd");

        let wicketd_addr = assert_ipv6(server.wicketd_server.local_addr());
        let wicketd_client = {
            let endpoint = format!(
                "http://[{}]:{}",
                wicketd_addr.ip(),
                wicketd_addr.port()
            );
            wicketd_client::Client::new(
                &endpoint,
                log.new(slog::o!("component" => "wicketd test client")),
            )
        };

        let artifact_addr = assert_ipv6(server.artifact_server.local_addr());
        let artifact_client = {
            let endpoint = format!(
                "http://[{}]:{}",
                artifact_addr.ip(),
                artifact_addr.port()
            );
            installinator_artifact_client::Client::new(
                &endpoint,
                log.new(slog::o!("component" => "artifact test client")),
            )
        };

        let wicketd_raw_client = ClientTestContext::new(
            server.wicketd_server.local_addr(),
            log.new(slog::o!("component" => "wicketd test, raw client")),
        );

        Self {
            wicketd_addr,
            wicketd_client,
            wicketd_raw_client,
            artifact_addr,
            artifact_client,
            server,
            gateway,
        }
    }

    pub fn log(&self) -> &slog::Logger {
        &self.gateway.logctx.log
    }

    pub async fn teardown(self) {
        self.server.close().await.unwrap();
        self.gateway.teardown().await;
    }
}

fn assert_ipv6(addr: SocketAddr) -> SocketAddrV6 {
    match addr {
        SocketAddr::V6(addr) => addr,
        SocketAddr::V4(addr) => {
            panic!("expected v6 address, got v4: {addr}")
        }
    }
}
