use crate::helpers::generate_name;
use anyhow::{Context as _, Result, anyhow};
use chrono::Utc;
use hickory_resolver::ResolveErrorKind;
use hickory_resolver::proto::ProtoErrorKind;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use oxide_client::CustomDnsResolver;
use oxide_client::types::{Name, ProjectCreate};
use oxide_client::{Client, ClientImagesExt, ClientProjectsExt, ClientVpcsExt};
use reqwest::Url;
use reqwest::dns::Resolve;
use reqwest::header::{HeaderMap, HeaderValue};
use sled_agent_types::rack_init::RackInitializeRequest;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const RSS_CONFIG_STR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../smf/sled-agent/non-gimlet/config-rss.toml"
));

// Environment variable containing the path to a cert that we should trust.
const E2E_TLS_CERT_ENV: &str = "E2E_TLS_CERT";

#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub project_name: Name,
}

impl Context {
    pub async fn new() -> Result<Context> {
        Context::from_client(ClientParams::new()?.build_client().await?).await
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

    pub async fn get_silo_image_id(&self, name: &str) -> Result<Uuid> {
        Ok(self.client.image_view().image(name).send().await?.id)
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

pub fn rss_config() -> Result<RackInitializeRequest> {
    let path = "/opt/oxide/sled-agent/pkg/config-rss.toml";
    let content =
        std::fs::read_to_string(&path).unwrap_or(RSS_CONFIG_STR.to_string());
    toml::from_str(&content)
        .with_context(|| "parsing config-rss as TOML".to_string())
}

fn nexus_external_dns_name(config: &RackInitializeRequest) -> String {
    format!(
        "{}.sys.{}",
        config.recovery_silo.silo_name.as_str(),
        config.external_dns_zone_name
    )
}

fn external_dns_addr(config: &RackInitializeRequest) -> Result<SocketAddr> {
    // From the RSS config, grab the first address from the configured services
    // IP pool as the DNS server's IP address.
    let dns_ip = config
        .internal_services_ip_pool_ranges
        .iter()
        .flat_map(|range| range.iter())
        .next()
        .ok_or_else(|| {
            anyhow!(
                "failed to get first IP from internal service \
                pool in RSS configuration"
            )
        })?;
    Ok(SocketAddr::from((dns_ip, 53)))
}

pub async fn nexus_addr() -> Result<IpAddr> {
    // Check $OXIDE_HOST first.
    if let Ok(host) =
        std::env::var("OXIDE_HOST").map_err(anyhow::Error::from).and_then(|s| {
            Ok(Url::parse(&s)?
                .host_str()
                .context("no host in OXIDE_HOST url")?
                .parse::<SocketAddr>()?
                .ip())
        })
    {
        return Ok(host);
    }

    // Otherwise, use the RSS configuration to find the DNS server, silo name,
    // and delegated DNS zone name.  Use this to look up Nexus's IP in the
    // external DNS server.  This could take a few seconds, since it's
    // asynchronous with the rack initialization request.
    let config = rss_config()?;
    let dns_addr = external_dns_addr(&config)?;
    let dns_name = nexus_external_dns_name(&config);
    let resolver = CustomDnsResolver::new(dns_addr)?;
    wait_for_records(
        &resolver,
        &dns_name,
        Duration::from_secs(1),
        Duration::from_secs(600),
    )
    .await
}

pub struct ClientParams {
    rss_config: RackInitializeRequest,
    nexus_dns_name: String,
    resolver: Arc<CustomDnsResolver>,
    proto: &'static str,
    extra_root_cert: Option<reqwest::tls::Certificate>,
}

impl ClientParams {
    pub fn new() -> Result<ClientParams> {
        // We need to find the IP of the external DNS server in the RSS
        // configuration and then set up a custom resolver to use this DNS
        // server.
        let rss_config = rss_config()?;
        let dns_addr = external_dns_addr(&rss_config)?;
        eprintln!("Using external DNS server at {:?}", dns_addr);
        let nexus_dns_name = nexus_external_dns_name(&rss_config);
        let resolver = Arc::new(CustomDnsResolver::new(dns_addr)?);

        // If we were provided with a path to a certificate in the environment,
        // add it as a trusted one.
        let (proto, extra_root_cert) = match std::env::var(E2E_TLS_CERT_ENV) {
            Err(_) => ("http", None),
            Ok(path) => {
                let cert_bytes = std::fs::read(&path).with_context(|| {
                    format!("reading certificate from {:?}", &path)
                })?;
                let cert = reqwest::tls::Certificate::from_pem(&cert_bytes)
                    .with_context(|| {
                        format!("parsing certificate from {:?}", &path)
                    })?;
                ("https", Some(cert))
            }
        };

        Ok(ClientParams {
            rss_config,
            nexus_dns_name,
            resolver,
            proto,
            extra_root_cert,
        })
    }

    pub fn base_url(&self) -> String {
        format!("{}://{}", self.proto, self.nexus_dns_name)
    }

    pub async fn resolve_nexus(&self) -> Result<String> {
        let address = self
            .resolver
            .resolve(self.nexus_dns_name.parse()?)
            .await
            .map_err(anyhow::Error::msg)?
            .next()
            .with_context(|| {
                format!(
                    "{} did not resolve to any addresses",
                    self.nexus_dns_name
                )
            })?;
        let port = match self.proto {
            "http" => 80,
            "https" => 443,
            _ => unreachable!(),
        };
        Ok(format!("{}:{}:{}", self.nexus_dns_name, port, address.ip()))
    }

    pub fn reqwest_builder(&self) -> reqwest::ClientBuilder {
        let mut builder =
            reqwest::ClientBuilder::new().dns_resolver(self.resolver.clone());

        if let Some(cert) = &self.extra_root_cert {
            builder = builder.add_root_certificate(cert.clone());
        }

        builder
    }

    pub async fn build_client(&self) -> Result<oxide_client::Client> {
        // Prepare to make a login request.
        let config = &self.rss_config;
        let base_url = self.base_url();
        let silo_name = config.recovery_silo.silo_name.as_str();
        let login_url = format!("{}/v1/login/{}/local", base_url, silo_name);
        let username: oxide_client::types::UserId =
            config.recovery_silo.user_name.as_ref().parse().map_err(|s| {
                anyhow!("parsing configured recovery user name: {:?}", s)
            })?;
        // See the comment in the config file about this password.
        let password: oxide_client::types::Password = "oxide".parse().unwrap();

        // By the time we get here, Nexus might not be up yet.  It may not have
        // published its names to external DNS, and even if it has, it may not have
        // opened its external listening socket.  So we have to retry a bit until we
        // succeed.
        let session_token = wait_for_condition(
            || async {
                // Use a raw reqwest client because it's not clear that Progenitor
                // is intended to support endpoints that return 300-level response
                // codes.  See progenitor#451.
                eprintln!("{}: attempting to log into API", Utc::now());

                let builder = self
                    .reqwest_builder()
                    .connect_timeout(Duration::from_secs(15))
                    .timeout(Duration::from_secs(60));

                oxide_client::login(
                    builder,
                    &login_url,
                    username.clone(),
                    password.clone(),
                )
                .await
                .map_err(|e| {
                    eprintln!("{}: login failed: {:#}", Utc::now(), e);
                    if let oxide_client::LoginError::RequestError(e) = &e {
                        if e.is_connect() {
                            return CondCheckError::NotYet;
                        }
                    }

                    CondCheckError::Failed(e)
                })
            },
            &Duration::from_secs(1),
            &Duration::from_secs(600),
        )
        .await
        .context("logging in")?;

        eprintln!("{}: login succeeded", Utc::now());

        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::COOKIE,
            HeaderValue::from_str(&format!("session={}", session_token))
                .unwrap(),
        );

        let reqwest_client = self
            .reqwest_builder()
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(60))
            .build()?;
        Ok(Client::new_with_client(&base_url, reqwest_client))
    }

    pub fn silo_name(&self) -> String {
        self.rss_config.recovery_silo.silo_name.to_string()
    }
}

async fn wait_for_records(
    resolver: &CustomDnsResolver,
    dns_name: &str,
    check_period: Duration,
    max: Duration,
) -> Result<IpAddr> {
    wait_for_condition::<_, anyhow::Error, _, _>(
        || async {
            resolver
                .resolver()
                .lookup_ip(dns_name)
                .await
                .map_err(|e| match resolve_error_proto_kind(&e) {
                    Some(ProtoErrorKind::NoRecordsFound { .. })
                    | Some(ProtoErrorKind::Timeout) => CondCheckError::NotYet,
                    _ => CondCheckError::Failed(anyhow::Error::new(e).context(
                        format!(
                            "resolving {:?} from {}",
                            dns_name,
                            resolver.dns_addr()
                        ),
                    )),
                })?
                .iter()
                .next()
                .ok_or(CondCheckError::NotYet)
        },
        &check_period,
        &max,
    )
    .await
    .with_context(|| {
        format!(
            "failed to resolve {:?} from {:?} within {:?}",
            dns_name,
            resolver.dns_addr(),
            max
        )
    })
}

fn resolve_error_proto_kind(
    e: &hickory_resolver::ResolveError,
) -> Option<&ProtoErrorKind> {
    let ResolveErrorKind::Proto(proto_error) = e.kind() else { return None };
    Some(proto_error.kind())
}
