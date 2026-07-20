// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;

use dropshot::test_util::ClientTestContext;
use gateway_test_utils::setup::GatewayTestContext;
use http::StatusCode;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use wicketd_commission_client::Error;
use wicketd_commission_types_versions::latest::inventory::{
    SpIdentifier, SpType,
};
use wicketd_commission_types_versions::latest::update::SpUpdateProgress;

pub struct WicketdTestContext {
    pub wicketd_addr: SocketAddrV6,
    pub wicketd_client: wicketd_client::Client,
    // This is not currently used but is kept here because it's easier to debug
    // this way.
    #[allow(dead_code)]
    pub wicketd_raw_client: ClientTestContext,
    pub artifact_addr: SocketAddrV6,
    // This is not currently used but is kept here because it's easier to debug
    // this way.
    #[allow(dead_code)]
    pub artifact_client: installinator_client::Client,
    pub commission_addr: SocketAddrV6,
    pub commission_client: wicketd_commission_client::Client,
    pub server: wicketd::Server,
    pub gateway: GatewayTestContext,
}

impl WicketdTestContext {
    pub async fn setup(gateway: GatewayTestContext) -> Self {
        const LOCALHOST_PORT_0: SocketAddrV6 =
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);

        // Reuse the log from the gateway context.
        let log = &gateway.logctx.log;

        let mgs_address = assert_ipv6(
            gateway
                .server
                .dropshot_server_for_address(LOCALHOST_PORT_0)
                .unwrap()
                .local_addr(),
        );

        let args = wicketd::Args {
            address: LOCALHOST_PORT_0,
            artifact_address: LOCALHOST_PORT_0,
            commission_address: LOCALHOST_PORT_0,
            mgs_address,
            nexus_proxy_address: LOCALHOST_PORT_0,
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

        let artifact_addr =
            assert_ipv6(server.installinator_server.local_addr());
        let artifact_client = {
            let endpoint = format!(
                "http://[{}]:{}",
                artifact_addr.ip(),
                artifact_addr.port()
            );
            installinator_client::Client::new(
                &endpoint,
                log.new(slog::o!("component" => "artifact test client")),
            )
        };

        let wicketd_raw_client = ClientTestContext::new(
            server.wicketd_server.local_addr(),
            log.new(slog::o!("component" => "wicketd test, raw client")),
        );

        let commission_addr =
            assert_ipv6(server.commission_server.local_addr());
        let commission_client = {
            let endpoint = format!(
                "http://[{}]:{}",
                commission_addr.ip(),
                commission_addr.port()
            );
            wicketd_commission_client::Client::new(
                &endpoint,
                log.new(slog::o!("component" => "commission test client")),
            )
        };

        Self {
            wicketd_addr,
            wicketd_client,
            wicketd_raw_client,
            artifact_addr,
            artifact_client,
            commission_addr,
            commission_client,
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

pub type CommissionClientError = Error<wicketd_commission_client::types::Error>;
pub type Cond<T> = Result<T, CondCheckError<CommissionClientError>>;

/// Poll get_update_progress until sled 0's entry satisfies `reached`.
///
/// Panics on failure with `expect_msg`.
pub async fn wait_for_sled0_progress(
    ctx: &WicketdTestContext,
    expect_msg: &str,
    reached: impl Fn(&SpUpdateProgress) -> bool,
) -> SpUpdateProgress {
    wait_for_condition(
        || async {
            let result: Cond<SpUpdateProgress> =
                match ctx.commission_client.get_update_progress().await {
                    Ok(resp) => {
                        match resp
                            .into_inner()
                            .get(&SpIdentifier { typ: SpType::Sled, slot: 0 })
                        {
                            Some(entry) if reached(entry) => Ok(entry.clone()),
                            _ => Err(CondCheckError::NotYet { status: None }),
                        }
                    }
                    Err(err) => Err(CondCheckError::Failed(err)),
                };
            result
        },
        &Duration::from_millis(50),
        &Duration::from_secs(30),
    )
    .await
    .expect(expect_msg)
}

pub fn assert_client_error(err: &CommissionClientError, expected: StatusCode) {
    match err {
        Error::ErrorResponse(rv) => {
            assert_eq!(rv.status(), expected, "unexpected status: {err:?}");
            assert!(!rv.message.is_empty(), "error carries a message: {err:?}");
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

pub fn assert_client_error_message(
    err: &CommissionClientError,
    expected: StatusCode,
    needle: &str,
) {
    match err {
        Error::ErrorResponse(rv) => {
            assert_eq!(rv.status(), expected, "unexpected status: {err:?}");
            assert!(
                rv.message.contains(needle),
                "error message {:?} should contain {needle:?}",
                rv.message,
            );
        }
        other => panic!("expected an error response, got {other:?}"),
    }
}
