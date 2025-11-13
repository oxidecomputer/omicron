// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use camino_tempfile::Utf8TempDir;
use dns_service_client::Client;
use dropshot::{HandlerTaskMode, test_util::LogContext};
use internal_dns_types::{
    config::{DnsConfigParams, DnsConfigZone, DnsRecord},
    names::ZONE_APEX_NAME,
};
use omicron_test_utils::dev::test_setup_log;
use slog::o;
use std::{collections::HashMap, net::Ipv6Addr};

const TEST_ZONE: &'static str = "oxide.internal";

// In this test we both need the latest DNS client from `dns-service-client`,
// and an older client to check compatibility against. While this gives us
// confidence that newer DNS servers' HTTP APIs work as expected with older
// clients, this does not check that old DNS servers handle new DNS clients
// well.
mod v1_client {
    use anyhow::Context;
    use internal_dns_types::v1;

    use std::collections::HashMap;

    progenitor::generate_api!(
        spec = "../openapi/dns-server/dns-server-1.0.0-49359e.json",
        interface = Positional,
        inner_type = slog::Logger,
        derives = [schemars::JsonSchema, Clone, Eq, PartialEq],
        pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
            slog::debug!(log, "client request";
                "method" => %request.method(),
                "uri" => %request.url(),
                "body" => ?&request.body(),
            );
        }),
        post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
            slog::debug!(log, "client response"; "result" => ?result);
        }),
        replace = {
            DnsConfig = v1::config::DnsConfig,
            DnsConfigParams = v1::config::DnsConfigParams,
            DnsConfigZone = v1::config::DnsConfigZone,
            DnsRecord = v1::config::DnsRecord,
            Srv = v1::config::Srv,
        }
    );

    pub async fn dns_records_create(
        client: &Client,
        zone_name: &str,
        records: HashMap<String, Vec<v1::config::DnsRecord>>,
    ) -> anyhow::Result<()> {
        let before = client
            .dns_config_get()
            .await
            .context("fetch current generation")?
            .into_inner();

        let (our_zones, other_zones) = before
            .zones
            .into_iter()
            .partition::<Vec<_>, _>(|z| z.zone_name == zone_name);

        assert!(our_zones.len() <= 1);
        let zone_records = if let Some(our_zone) = our_zones.into_iter().next()
        {
            our_zone.records.into_iter().chain(records.into_iter()).collect()
        } else {
            records
        };

        let new_zone = v1::config::DnsConfigZone {
            zone_name: zone_name.to_owned(),
            records: zone_records,
        };

        let zones =
            other_zones.into_iter().chain(std::iter::once(new_zone)).collect();
        let after = v1::config::DnsConfigParams {
            generation: before.generation.next(),
            zones,
            time_created: chrono::Utc::now(),
        };
        client.dns_config_put(&after).await.context("updating generation")?;
        Ok(())
    }

    pub async fn dns_records_list(
        client: &Client,
        zone_name: &str,
    ) -> anyhow::Result<HashMap<String, Vec<v1::config::DnsRecord>>> {
        Ok(client
            .dns_config_get()
            .await
            .context("fetch current generation")?
            .into_inner()
            .zones
            .into_iter()
            .find(|z| z.zone_name == zone_name)
            .map(|z| z.records)
            .unwrap_or_else(HashMap::new))
    }
}

// A V2 server can productively handle requests from a V1 client, and a V1
// client *can* provide records to a V2 server (though this really shouldn't
// ever happen). A V1 client will get an error trying to list records that its
// API doesn't support.
#[tokio::test]
pub async fn cross_version_works() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("cross_version_works").await?;

    use internal_dns_types::v1::config::DnsRecord as V1DnsRecord;
    use internal_dns_types::v2::config::DnsRecord as V2DnsRecord;

    let ns1_addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let ns1_name = format!("ns1.{TEST_ZONE}.");
    let service_addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x2);

    let mut records = HashMap::new();
    records.insert("ns1".to_string(), vec![V1DnsRecord::Aaaa(ns1_addr)]);

    // A V1 client can create records that V2 clients can read.
    v1_client::dns_records_create(&test_ctx.v1_client, TEST_ZONE, records)
        .await
        .expect("can create zone");

    let v1_records =
        v1_client::dns_records_list(&test_ctx.v1_client, TEST_ZONE)
            .await
            .expect("zone exists");
    let v2_records = dns_records_list(&test_ctx.latest_client, TEST_ZONE)
        .await
        .expect("zone exists");

    // V1 and V2 APIs return the same content, when both get content.
    let v1_as_v2: HashMap<String, Vec<V2DnsRecord>> = v1_records
        .into_iter()
        .map(|(k, v)| (k, v.into_iter().map(Into::into).collect()))
        .collect();
    assert_eq!(v2_records, v1_as_v2);

    // A V2 client can create records including the new NS type.
    let mut records = HashMap::new();
    records
        .insert("service".to_string(), vec![V2DnsRecord::Aaaa(service_addr)]);
    records.insert("ns1".to_string(), vec![V2DnsRecord::Aaaa(ns1_addr)]);
    records.insert(
        ZONE_APEX_NAME.to_string(),
        vec![V2DnsRecord::Ns(ns1_name.clone())],
    );
    dns_records_create(&test_ctx.latest_client, TEST_ZONE, records.clone())
        .await
        .expect("can create zone");

    match test_ctx.v1_client.dns_config_get().await {
        Err(dns_service_client::Error::ErrorResponse(rv)) => {
            assert_eq!(
                rv.message,
                dns_service_client::ERROR_CODE_INCOMPATIBLE_RECORD
            );
        }
        o => {
            panic!(
                "expected V1 config get to fail with an ErrorResponse, got {:?}",
                o
            );
        }
    }

    let records = dns_records_list(&test_ctx.latest_client, TEST_ZONE)
        .await
        .expect("zone exists");

    // The V2 records are what we PUT.
    assert_eq!(records.len(), 3);
    assert_eq!(records["service"], vec![V2DnsRecord::Aaaa(service_addr)]);
    assert_eq!(records[ZONE_APEX_NAME], vec![V2DnsRecord::Ns(ns1_name)]);

    test_ctx.cleanup().await;

    Ok(())
}

struct TestContext {
    v1_client: v1_client::Client,
    latest_client: Client,
    dns_server: dns_server::dns::server::ServerHandle,
    dropshot_server: dropshot::HttpServer<dns_server::http_server::Context>,
    tmp: Utf8TempDir,
    logctx: LogContext,
}

impl TestContext {
    async fn cleanup(self) {
        drop(self.dns_server);
        self.dropshot_server.close().await.expect("Failed to clean up server");
        self.tmp.close().expect("Failed to clean up tmp directory");
        self.logctx.cleanup_successful();
    }
}

async fn init_client_server(
    test_name: &str,
) -> Result<TestContext, anyhow::Error> {
    // initialize dns server config
    let (tmp, config_storage, config_dropshot, logctx) =
        test_config(test_name)?;
    let log = logctx.log.clone();

    // initialize dns server db
    let store = dns_server::storage::Store::new(
        log.new(o!("component" => "store")),
        &config_storage,
    )
    .context("initializing storage")?;
    assert!(store.is_new());

    // launch a dns server
    let dns_server_config = dns_server::dns::server::Config {
        bind_address: "[::1]:0".parse().unwrap(),
        ..Default::default()
    };
    let (dns_server, dropshot_server) = dns_server::start_servers(
        log.clone(),
        store,
        &dns_server_config,
        &config_dropshot,
    )
    .await?;

    let v1_client = v1_client::Client::new(
        &format!("http://{}", dropshot_server.local_addr()),
        log.clone(),
    );
    let latest_client =
        Client::new(&format!("http://{}", dropshot_server.local_addr()), log);

    Ok(TestContext {
        v1_client,
        latest_client,
        dns_server,
        dropshot_server,
        tmp,
        logctx,
    })
}

fn test_config(
    test_name: &str,
) -> Result<
    (
        Utf8TempDir,
        dns_server::storage::Config,
        dropshot::ConfigDropshot,
        LogContext,
    ),
    anyhow::Error,
> {
    let logctx = test_setup_log(test_name);
    let tmp_dir = Utf8TempDir::with_prefix("dns-server-test")?;
    let mut storage_path = tmp_dir.path().to_path_buf();
    storage_path.push("test");
    let config_storage =
        dns_server::storage::Config { storage_path, keep_old_generations: 3 };
    let config_dropshot = dropshot::ConfigDropshot {
        bind_address: "[::1]:0".to_string().parse().unwrap(),
        default_request_body_max_bytes: 1024,
        default_handler_task_mode: HandlerTaskMode::Detached,
        log_headers: vec![],
    };

    Ok((tmp_dir, config_storage, config_dropshot, logctx))
}

async fn dns_records_create(
    client: &Client,
    zone_name: &str,
    records: HashMap<String, Vec<DnsRecord>>,
) -> anyhow::Result<()> {
    let before = client
        .dns_config_get()
        .await
        .context("fetch current generation")?
        .into_inner();

    let (our_zones, other_zones) = before
        .zones
        .into_iter()
        .partition::<Vec<_>, _>(|z| z.zone_name == zone_name);

    assert!(our_zones.len() <= 1);
    let zone_records = if let Some(our_zone) = our_zones.into_iter().next() {
        our_zone.records.into_iter().chain(records.into_iter()).collect()
    } else {
        records
    };

    let new_zone = DnsConfigZone {
        zone_name: zone_name.to_owned(),
        records: zone_records,
    };

    let zones =
        other_zones.into_iter().chain(std::iter::once(new_zone)).collect();
    let after = DnsConfigParams {
        generation: before.generation.next(),
        serial: before.serial.checked_add(1).expect("can increment serial"),
        zones,
        time_created: chrono::Utc::now(),
    };
    client.dns_config_put(&after).await.context("updating generation")?;
    Ok(())
}

async fn dns_records_list(
    client: &Client,
    zone_name: &str,
) -> anyhow::Result<HashMap<String, Vec<DnsRecord>>> {
    Ok(client
        .dns_config_get()
        .await
        .context("fetch current generation")?
        .into_inner()
        .zones
        .into_iter()
        .find(|z| z.zone_name == zone_name)
        .map(|z| z.records)
        .unwrap_or_else(HashMap::new))
}
