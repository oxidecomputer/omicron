// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to the Oxide control plane.

use anyhow::Context;
use anyhow::anyhow;
use futures::FutureExt;
use hickory_resolver::TokioResolver;
use hickory_resolver::config::{
    NameServerConfig, ResolverConfig, ResolverOpts,
};
use hickory_resolver::name_server::TokioConnectionProvider;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;

progenitor::generate_api!(
    spec = "../../openapi/nexus.json",
    interface = Builder,
    tags = Separate,
);

/// Custom reqwest DNS resolver intended for use with the Oxide client
///
/// In development, the Oxide client is often used against a deployment with its
/// own DNS server that can resolve DNS names for Nexus.  This impl lets
/// consumers use that DNS server directly with reqwest to resolve IP addresses
/// for Nexus.  This is often useful when trying to connect with Nexus using
/// TLS, since you need to come in via the DNS name to do that.
///
/// This is a thin wrapper around `TokioResolver`
pub struct CustomDnsResolver {
    dns_addr: SocketAddr,
    // The lifetime constraints on the `Resolve` trait make it hard to avoid an
    // Arc here.
    resolver: Arc<TokioResolver>,
}

impl CustomDnsResolver {
    /// Make a new custom resolver that uses the DNS server at the specified
    /// address
    pub fn new(dns_addr: SocketAddr) -> anyhow::Result<CustomDnsResolver> {
        let mut resolver_config = ResolverConfig::new();
        resolver_config.add_name_server(NameServerConfig::new(
            dns_addr,
            hickory_resolver::proto::xfer::Protocol::Udp,
        ));
        let mut resolver_opts = ResolverOpts::default();
        // Enable edns for potentially larger records
        resolver_opts.edns0 = true;

        let resolver = Arc::new(
            TokioResolver::builder_with_config(
                resolver_config,
                TokioConnectionProvider::default(),
            )
            .with_options(resolver_opts)
            .build(),
        );
        Ok(CustomDnsResolver { dns_addr, resolver })
    }

    /// Returns the address of the DNS server that we're using to resolve names
    pub fn dns_addr(&self) -> SocketAddr {
        self.dns_addr
    }

    /// Returns the underlying `TokioResolver
    pub fn resolver(&self) -> &TokioResolver {
        &self.resolver
    }
}

impl reqwest::dns::Resolve for CustomDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
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

#[derive(Debug, Error)]
pub enum LoginError {
    #[error("logging in: {0:#}")]
    RequestError(#[from] reqwest::Error),
    #[error("logging in: {0:#}")]
    CatchAll(#[from] anyhow::Error),
}

/// Logs into the specified Oxide API endpoint and returns a session token.
///
/// This is intended for test suites.
pub async fn login(
    reqwest_builder: reqwest::ClientBuilder,
    silo_login_url: &str,
    username: crate::types::UserId,
    password: crate::types::Password,
) -> Result<String, LoginError> {
    let login_request_body =
        serde_json::to_string(&crate::types::UsernamePasswordCredentials {
            username,
            password,
        })
        .context("serializing login request body")?;

    // Do not have reqwest follow redirects.  That's because our login response
    // includes both a redirect and the session cookie header.  If reqwest
    // follows the redirect, we won't have a chance to get the cookie.
    let reqwest_client = reqwest_builder
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("creating reqwest client for login")?;

    let response = reqwest_client
        .post(silo_login_url)
        .body(login_request_body)
        .send()
        .await?;
    let session_cookie = response
        .headers()
        .get(http::header::SET_COOKIE)
        .ok_or_else(|| anyhow!("expected session cookie after login"))?
        .to_str()
        .context("expected session cookie token to be a string")?;
    let (session_token, rest) =
        session_cookie.split_once("; ").context("parsing session cookie")?;
    let (key, value) =
        session_token.split_once('=').context("parsing session token")?;
    if key != "session" {
        return Err(
            anyhow!("unexpected key parsing session token: {:?}", key).into()
        );
    }
    if !rest.contains("Path=/; HttpOnly; SameSite=Lax;") {
        return Err(anyhow!(
            "unexpected cookie header format: {:?}",
            session_cookie
        )
        .into());
    }
    Ok(value.to_string())
}
