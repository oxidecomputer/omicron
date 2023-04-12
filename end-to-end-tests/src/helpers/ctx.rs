use crate::helpers::generate_name;
use anyhow::{Context as _, Result};
use omicron_sled_agent::rack_setup::config::SetupServiceConfig;
use oxide_client::types::{Name, ProjectCreate};
use oxide_client::{Client, ClientProjectsExt, ClientVpcsExt};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::Url;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub project_name: Name,
}

impl Context {
    pub async fn new() -> Result<Context> {
        Context::from_client(build_client()?).await
    }

    pub async fn from_client(client: Client) -> Result<Context> {
        let project_name = client
            .project_create()
            .body(ProjectCreate {
                name: generate_name("proj")?,
                description: String::new(),
            })
            .send()
            .await?
            .name
            .clone();

        Ok(Context { client, project_name })
    }

    pub async fn cleanup(self) -> Result<()> {
        self.client
            .vpc_subnet_delete()
            .project(self.project_name.clone())
            .vpc("default")
            .subnet("default")
            .send()
            .await?;
        self.client
            .vpc_delete()
            .project(self.project_name.clone())
            .vpc("default")
            .send()
            .await?;
        self.client.project_delete().project(self.project_name).send().await?;
        Ok(())
    }
}

pub fn build_client() -> Result<oxide_client::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static(
            "Bearer oxide-spoof-001de000-05e4-4000-8000-000000004007",
        ),
    );

    let client = reqwest::ClientBuilder::new()
        .default_headers(headers)
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(60))
        .build()?;
    Ok(Client::new_with_client(&get_base_url(), client))
}

pub fn nexus_addr() -> SocketAddr {
    // Check $OXIDE_HOST first.
    if let Ok(host) =
        std::env::var("OXIDE_HOST").map_err(anyhow::Error::from).and_then(|s| {
            Ok(Url::parse(&s)?
                .host_str()
                .context("no host in OXIDE_HOST url")?
                .parse()?)
        })
    {
        return host;
    }

    // If we can find config-rss.toml, grab the first address from the
    // configured services IP pool.
    let rss_config_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../smf/sled-agent/non-gimlet/config-rss.toml");
    if rss_config_path.exists() {
        if let Ok(config) = SetupServiceConfig::from_file(rss_config_path) {
            if let Some(addr) = config
                .internal_services_ip_pool_ranges
                .iter()
                .flat_map(|range| range.iter())
                .next()
            {
                return (addr, 80).into();
            }
        }
    }

    ([192, 168, 1, 20], 80).into()
}

fn get_base_url() -> String {
    // Check $OXIDE_HOST first.
    if let Ok(host) = std::env::var("OXIDE_HOST") {
        return host;
    }

    format!("http://{}", nexus_addr())
}
