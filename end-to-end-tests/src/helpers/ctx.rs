use crate::helpers::generate_name;
use anyhow::{Context as _, Result};
use omicron_sled_agent::rack_setup::config::SetupServiceConfig;
use oxide_client::types::{Name, OrganizationCreate, ProjectCreate};
use oxide_client::{Client, ClientOrganizationsExt, ClientProjectsExt};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::Url;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub org_name: Name,
    pub project_name: Name,
}

impl Context {
    pub async fn new() -> Result<Context> {
        Context::from_client(build_client()?).await
    }

    pub async fn from_client(client: Client) -> Result<Context> {
        let org_name = client
            .organization_create()
            .body(OrganizationCreate {
                name: generate_name("org")?,
                description: String::new(),
            })
            .send()
            .await?
            .name
            .clone();

        let project_name = client
            .project_create()
            .organization_name(org_name.clone())
            .body(ProjectCreate {
                name: generate_name("proj")?,
                description: String::new(),
            })
            .send()
            .await?
            .name
            .clone();

        Ok(Context { client, org_name, project_name })
    }

    pub async fn cleanup(self) -> Result<()> {
        self.client
            .project_delete()
            .organization_name(self.org_name.clone())
            .project_name(self.project_name)
            .send()
            .await?;
        self.client
            .organization_delete()
            .organization_name(self.org_name)
            .send()
            .await?;
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

    // If we can find config-rss.toml, look for an external_address.
    let rss_config_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../smf/sled-agent/config-rss.toml");
    if rss_config_path.exists() {
        if let Ok(config) = SetupServiceConfig::from_file(rss_config_path) {
            return (config.nexus_external_address, 80).into();
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
