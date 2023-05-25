use crate::helpers::generate_name;
use anyhow::{anyhow, Context as _, Result};
use chrono::Utc;
use futures::future::FutureExt;
use omicron_sled_agent::rack_setup::config::SetupServiceConfig;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oxide_client::types::{Name, ProjectCreate, UsernamePasswordCredentials};
use oxide_client::{Client, ClientProjectsExt, ClientVpcsExt};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use trust_dns_resolver::config::{
    NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use trust_dns_resolver::error::ResolveErrorKind;
use trust_dns_resolver::TokioAsyncResolver;

const RSS_CONFIG_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../smf/sled-agent/non-gimlet/config-rss.toml"
);
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
        Context::from_client(build_client().await?).await
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

fn rss_config() -> Result<SetupServiceConfig> {
    toml::from_str(RSS_CONFIG_STR)
        .with_context(|| format!("parsing {:?} as TOML", RSS_CONFIG_PATH))
}

fn nexus_external_dns_name(config: &SetupServiceConfig) -> String {
    format!(
        "{}.sys.{}",
        config.recovery_silo.silo_name.as_str(),
        config.external_dns_zone_name
    )
}

fn external_dns_addr(config: &SetupServiceConfig) -> Result<SocketAddr> {
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
    resolver
        .wait_for_records(
            &dns_name,
            Duration::from_secs(1),
            Duration::from_secs(300),
        )
        .await
}

pub async fn build_client() -> Result<oxide_client::Client> {
    // Make a reqwest client that we can use to make the initial login request.
    // To do this, we need to find the IP of the external DNS server in the RSS
    // configuration and then set up a custom resolver to use this DNS server.
    let config = rss_config()?;
    let dns_addr = external_dns_addr(&config)?;
    let dns_name = nexus_external_dns_name(&config);
    let resolver = Arc::new(CustomDnsResolver::new(dns_addr)?);

    // Do not have reqwest follow redirects.  That's because our login response
    // includes both a redirect and the session cookie header.  If reqwest
    // follows the redirect, we won't have a chance to get the cookie.
    let mut builder = reqwest::ClientBuilder::new()
        .connect_timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(resolver.clone())
        .timeout(Duration::from_secs(60));

    // If we were provided with a path to a certificate in the environment, add
    // it as a trusted one.
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

    if let Some(cert) = &extra_root_cert {
        builder = builder.add_root_certificate(cert.clone());
    }

    let reqwest_login_client = builder.build()?;

    // Prepare to make a login request.
    let base_url = format!("{}://{}", proto, dns_name);
    let silo_name = config.recovery_silo.silo_name.as_str();
    let login_url = format!("{}/login/{}/local", base_url, silo_name);
    let username: oxide_client::types::UserId =
        config.recovery_silo.user_name.as_str().parse().map_err(|s| {
            anyhow!("parsing configured recovery user name: {:?}", s)
        })?;
    // See the comment in the config file about this password.
    let password: oxide_client::types::Password = "oxide".parse().unwrap();
    let login_request_body =
        serde_json::to_string(&UsernamePasswordCredentials {
            username,
            password,
        })
        .context("serializing login request body")?;

    // By the time we get here, Nexus might not be up yet.  It may not have
    // published its names to external DNS, and even if it has, it may not have
    // opened its external listening socket.  So we have to retry a bit until we
    // succeed.
    let response = wait_for_condition(
        || async {
            // Use a raw reqwest client because it's not clear that Progenitor
            // is intended to support endpoints that return 300-level response
            // codes.  See progenitor#451.
            eprintln!("{}: attempting to log into API", Utc::now());
            reqwest_login_client
                .post(&login_url)
                .body(login_request_body.clone())
                .send()
                .await
                .map_err(|e| {
                    eprintln!("{}: login failed: {:#}", Utc::now(), e);
                    if e.is_connect() {
                        CondCheckError::NotYet
                    } else {
                        CondCheckError::Failed(
                            anyhow::Error::new(e).context("logging in"),
                        )
                    }
                })
        },
        &Duration::from_secs(1),
        &Duration::from_secs(300),
    )
    .await
    .context("logging in")?;

    eprintln!("{}: login succeeded", Utc::now());
    let session_cookie = response
        .headers()
        .get(http::header::SET_COOKIE)
        .ok_or_else(|| anyhow!("expected session cookie after login"))?
        .to_str()
        .context("expected session cookie token to be a string")?;
    let (session_token, rest) = session_cookie.split_once("; ").unwrap();
    assert!(session_token.starts_with("session="));
    assert!(rest.contains("Path=/; HttpOnly; SameSite=Lax; Max-Age="));

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::COOKIE,
        HeaderValue::from_str(session_token).unwrap(),
    );

    let mut builder = reqwest::ClientBuilder::new()
        .default_headers(headers)
        .connect_timeout(Duration::from_secs(15))
        .dns_resolver(resolver)
        .timeout(Duration::from_secs(60));

    if let Some(cert) = extra_root_cert {
        builder = builder.add_root_certificate(cert);
    }

    let reqwest_client = builder.build()?;
    Ok(Client::new_with_client(&base_url, reqwest_client))
}

/// Wrapper around a `TokioAsyncResolver` so that we can impl
/// `reqwest::dns::Resolve` for it.
struct CustomDnsResolver {
    dns_addr: SocketAddr,
    // The lifetime constraints on the `Resolve` trait make it hard to avoid an
    // Arc here.
    resolver: Arc<TokioAsyncResolver>,
}

impl CustomDnsResolver {
    fn new(dns_addr: SocketAddr) -> Result<CustomDnsResolver> {
        let mut resolver_config = ResolverConfig::new();
        resolver_config.add_name_server(NameServerConfig {
            socket_addr: dns_addr,
            protocol: Protocol::Udp,
            tls_dns_name: None,
            trust_nx_responses: false,
            bind_addr: None,
        });

        let resolver = Arc::new(
            TokioAsyncResolver::tokio(resolver_config, ResolverOpts::default())
                .context("failed to create resolver")?,
        );
        Ok(CustomDnsResolver { dns_addr, resolver })
    }

    async fn wait_for_records(
        &self,
        dns_name: &str,
        check_period: Duration,
        max: Duration,
    ) -> Result<IpAddr> {
        wait_for_condition::<_, anyhow::Error, _, _>(
            || async {
                self.resolver
                    .lookup_ip(dns_name)
                    .await
                    .map_err(|e| match e.kind() {
                        ResolveErrorKind::NoRecordsFound { .. }
                        | ResolveErrorKind::Timeout => CondCheckError::NotYet,
                        _ => CondCheckError::Failed(
                            anyhow::Error::new(e).context(format!(
                                "resolving {:?} from {}",
                                dns_name, self.dns_addr
                            )),
                        ),
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
                dns_name, self.dns_addr, max
            )
        })
    }
}

pub fn gateway_ip() -> String {
    std::env::var("GATEWAY_IP")
        .expect("GATEWAY_IP environment variable required")
}

impl reqwest::dns::Resolve for CustomDnsResolver {
    fn resolve(
        &self,
        name: hyper::client::connect::dns::Name,
    ) -> reqwest::dns::Resolving {
        let resolver = self.resolver.clone();
        async move {
            let list = resolver.lookup_ip(name.as_str()).await?;
            Ok(Box::new(list.into_iter().map(|s| {
                // reqwest does not appear to use the port number here.
                // (See the docs for `ClientBuilder::resolve()`, which isn't
                // the same thing, but is related.)
                SocketAddr::from((s, 0))
            })) as Box<dyn Iterator<Item = SocketAddr> + Send>)
        }
        .boxed()
    }
}
