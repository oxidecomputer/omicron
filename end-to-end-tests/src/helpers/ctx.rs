use crate::helpers::generate_name;
use anyhow::{anyhow, Context as _, Result};
use omicron_sled_agent::rack_setup::config::SetupServiceConfig;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oxide_client::types::{Name, ProjectCreate, UsernamePasswordCredentials};
use oxide_client::{Client, ClientProjectsExt, ClientVpcsExt};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;
use std::net::SocketAddr;
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

pub async fn build_client() -> Result<oxide_client::Client> {
    build_authenticated_client().await
}

fn rss_config() -> Result<SetupServiceConfig> {
    toml::from_str(RSS_CONFIG_STR)
        .with_context(|| format!("parsing {:?} as TOML", RSS_CONFIG_PATH))
}

pub async fn nexus_addr() -> Result<SocketAddr> {
    // Check $OXIDE_HOST first.
    if let Ok(host) =
        std::env::var("OXIDE_HOST").map_err(anyhow::Error::from).and_then(|s| {
            Ok(Url::parse(&s)?
                .host_str()
                .context("no host in OXIDE_HOST url")?
                .parse()?)
        })
    {
        return Ok(host);
    }

    // Otherwise, use the RSS configuration to find the DNS server, silo name,
    // and delegated DNS zone name.  Use this to look up Nexus's IP in the
    // external DNS server.
    //
    // First, load the RSS configuration file.
    let config = rss_config()?;

    // From config-rss.toml, grab the first address from the configured services
    // IP pool as the DNS server's IP address.
    let dns_ip = config
        .internal_services_ip_pool_ranges
        .iter()
        .flat_map(|range| range.iter())
        .next()
        .ok_or_else(|| {
            anyhow!(
                "failed to get first IP from internal service \
                pool in {}",
                RSS_CONFIG_PATH,
            )
        })?;
    let dns_addr = SocketAddr::from((dns_ip, 53));

    // Resolve the DNS name of the recovery Silo that ought to have been created
    // already.  This could take a few seconds, since it's asynchronous with the
    // rack initialization request.
    let silo_name = &config.recovery_silo.silo_name;
    let dns_name = format!(
        "{}.sys.{}",
        silo_name.as_str(),
        &config.external_dns_zone_name
    );

    let mut resolver_config = ResolverConfig::new();
    resolver_config.add_name_server(NameServerConfig {
        socket_addr: dns_addr,
        protocol: Protocol::Udp,
        tls_dns_name: None,
        trust_nx_responses: false,
        bind_addr: None,
    });

    let resolver =
        TokioAsyncResolver::tokio(resolver_config, ResolverOpts::default())
            .context("failed to create resolver")?;

    wait_for_condition::<_, anyhow::Error, _, _>(
        || async {
            let addr = resolver
                .lookup_ip(&dns_name)
                .await
                .map_err(|e| match e.kind() {
                    ResolveErrorKind::NoRecordsFound { .. }
                    | ResolveErrorKind::Timeout => CondCheckError::NotYet,
                    _ => CondCheckError::Failed(anyhow::Error::new(e).context(
                        format!("resolving {:?} from {}", dns_name, dns_addr),
                    )),
                })?
                .iter()
                .next()
                .ok_or(CondCheckError::NotYet)?;
            Ok(SocketAddr::from((addr, 80)))
        },
        &Duration::from_secs(1),
        &Duration::from_secs(300),
    )
    .await
    .context("failed to get Nexus addr")
}

async fn get_base_url() -> Result<String> {
    Ok(format!("http://{}", nexus_addr().await?))
}

async fn build_authenticated_client() -> Result<oxide_client::Client> {
    let config = rss_config()?;
    let base_url = get_base_url().await?;
    let silo_name = config.recovery_silo.silo_name.as_str();
    let username: oxide_client::types::UserId =
        config.recovery_silo.user_name.as_str().parse().map_err(|s| {
            anyhow!("parsing configured recovery user name: {:?}", s)
        })?;
    // See the comment in the config file.
    let password: oxide_client::types::Password = "oxide".parse().unwrap();
    let login_request_body =
        serde_json::to_string(&UsernamePasswordCredentials {
            username: username,
            password: password,
        })
        .context("serializing login request body")?;

    // Do not have reqwest follow redirects.  That's because our login response
    // includes both a redirect and the session cookie header.  If reqwest
    // follows the redirect, we won't have a chance to get the cookie.
    let reqwest_login_client = reqwest::ClientBuilder::new()
        .connect_timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(60))
        .build()?;
    let login_url = format!("{}/login/{}/local", base_url, silo_name);

    // By the time we get here, we generally would have successfully resolved
    // Nexus's external IP address from the external DNS server.  So we'd
    // expect Nexus to be up.  But that's not necessarily true: external DNS can
    // be set up during rack initialization, before Nexus has opened its
    // external listening socket.  This is arguably a bug, advertising a service
    // before it's ready, but a pretty niche corner case (rack initialization)
    // and anyway DNS is always best-effort.  The point is: let's retry a little
    // while if we can't immediately connect.
    let response = wait_for_condition(
        || async {
            // Use a raw reqwest client because it's not clear that Progenitor
            // is intended to support endpoints that return 300-level response
            // codes.  See progenitor#451.
            reqwest_login_client
                .post(&login_url)
                .body(login_request_body.clone())
                .send()
                .await
                .map_err(|e| {
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
        &Duration::from_secs(30),
    )
    .await
    .context("logging in")?;

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

    let reqwest_client = reqwest::ClientBuilder::new()
        .default_headers(headers)
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(60))
        .build()?;
    Ok(Client::new_with_client(&base_url, reqwest_client))
}
