// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A [client](ExternalHttpClient) for requests to endpoints external to the
//! rack, and related machinery.
//!
//! ## Motivation
//!
//! In some cases, such as delivering webhook alerts, Nexus must communicate to
//! external endpoints whose URLs are provided by user configuration. When doing
//! so, it is important to ensure that the user-provided endpoint does not
//! resolve to an IP address on the underlay network. This is necessary to
//! prevent the risk of server-side request forgery (SSRF) attacks, in which
//! Nexus is used to smuggle an external request onto an underlay network
//! service which should otherwise not be exposed externally. In addition, a
//! non-malicious operator could inadvertantly misconfigure things so that the
//! IPv6 ULA address assigned to an external service collides with an underlay
//! network address, with confusing results. In such cases, it is better to fail
//! loudly than to accidentally send the request to an underlay service which is
//! not its intended destination.
//!
//! ## Theory of Operation
//!
//! We take a defense-in-depth approach to avoid accidentally sending external
//! requests to the underlay network. First, in the production control plane, we
//! configure the clients which are used to send external requests to bind
//! sockets only on the OPTE interface. The name of the interface in a given
//! Nexus zone is provided by the sled-agent through the
//! [`ExternalHttpClientConfig`].
//!
//! Binding to the OPTE interface is in and of itself sufficient to provide
//! network-level isolation from the underlay for external clients. However, the
//! client type in this module also enforces external requests at the software
//! layer by rejecting any URL which either contains an underlay network address
//! as its host, or whose host is a domain name that resolves to underlay
//! network address(es). One reason we enforce this policy both at the client
//! and at the network level is for defense-in-depth; if either layer fails to
//! enforce the policy, we expect that the other will still ensure that external
//! requests are not sent to the underlay. In addition, checking at the client
//! level allows us to record better errors than the generic connection-level
//! errors that would otherwise be reported if the OPTE-level enforcement
//! prevents us from connecting to an address. This way, we can provide a more
//! useful error message for operators who are troubleshooting why a request was
//! not sent to the expected endpoint, and record when we prevented a potential
//! SSRF attack.
//!
//! The determination of which IP addresses are considered "external" is done by
//! the [`ExternalIpPolicy`] type, which is configured with the rack's
//! [`UnderlaySubnets`] once they are loaded from the database, or when rack is
//! initialized (if RSS has not yet run). This policy is used by the
//! [`ExternalHttpClient`] type in this module, which is a wrapper around a
//! [`reqwest::Client`] that enforces that all requested URLs are checked
//! against this policy before allowing a request to be sent. Unfortunately,
//! `reqwest` does not provide an integration point for a middleware that
//! decides which IP addresses are okay to connect to, so we have to wrap the
//! client and implement this on top of it. The policy is also passed to the
//! [`external_dns`] resolver, which is used when the client resolves a domain
//! name to IP addresses, and rejects any names that resolve to disallowed IPs.
//! Finally, we also wrap the redirect policy for the `reqwest` client to ensure
//! that following redirects also checks that the redirect does not point us at
//! an underlay address.
//!
//! Since we must construct this wrapper around the `reqwest::Client` API, we
//! also provide an [`ExternalClientBuilder`] to configure our client wrapper.
//! This is necessary because we must wrap the redirect policy used by the
//! client, as described above, and some external clients may need to follow
//! redirects, while others may not.

use super::external_dns;

use nexus_config::ExternalHttpClientConfig;
use nexus_config::TreatLoopbackAsExternal;
use omicron_common::address::UNDERLAY_MULTICAST_SUBNET;
use omicron_common::address::UnderlaySubnets;
use oxnet::Ipv6Net;

use reqwest::IntoUrl;
use reqwest::Method;
use reqwest::redirect;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use url::Url;

/// Policy for deciding whether an IP address is external to the rack.
#[derive(Clone, Debug)]
pub struct ExternalIpPolicy {
    underlay_subnets: Arc<OnceLock<UnderlaySubnets>>,
    loopback_policy: TreatLoopbackAsExternal,
}

/// Errors returned by [`ExternalIpPolicy::ensure_external_ip`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum ExternalIpError {
    #[error("address {ip} is within the underlay subnet {subnet}")]
    Underlay { ip: IpAddr, subnet: Ipv6Net },
    #[error("address {ip} is a loopback address")]
    Loopback { ip: IpAddr },
    #[error(
        "cannot check whether {ip} is an external IP address before the \
         rack's underlay subnets are known (RSS has not run yet)"
    )]
    RackNotInitialized { ip: IpAddr },
}

impl ExternalIpPolicy {
    pub fn new(
        underlay_subnets: Arc<OnceLock<UnderlaySubnets>>,
        loopback_policy: TreatLoopbackAsExternal,
    ) -> Self {
        Self { underlay_subnets, loopback_policy }
    }

    /// Returns `Ok(ip)` if `ip` is external to the rack, and an
    /// error describing why it is not otherwise.
    ///
    /// # Errors
    ///
    /// * [`ExternalIpError::Underlay`] if `ip` is within the AZ underlay
    ///   subnet or the underlay multicast subnet.
    /// * [`ExternalIpError::Loopback`] if `ip` is a loopback address
    /// * [`ExternalIpError::RackNotInitialized`] if the underlay subnets are
    ///   not yet known, in which case no determination can be made at all.
    pub fn ensure_external_ip(
        &self,
        ip: IpAddr,
    ) -> Result<IpAddr, ExternalIpError> {
        let subnets = self
            .underlay_subnets
            .get()
            .ok_or(ExternalIpError::RackNotInitialized { ip })?;
        let az_subnet = subnets.az_subnet.net();
        // If this is an IPv4-mapped IPv6 address, convert it to the canonical
        // form before checking it.
        let ip = ip.to_canonical();
        match ip {
            // IPv6 addresses that are within the AZ's underlay subnet are not
            // external to the rack.
            IpAddr::V6(v6) if az_subnet.contains(v6) => {
                Err(ExternalIpError::Underlay { ip, subnet: az_subnet })
            }
            // The underlay multicast subnet is also part of the underlay
            // network, so check that, too.
            IpAddr::V6(v6) if UNDERLAY_MULTICAST_SUBNET.contains(v6) => {
                Err(ExternalIpError::Underlay {
                    ip,
                    subnet: UNDERLAY_MULTICAST_SUBNET,
                })
            }
            // Loopback and unspecified addresses are not external, regardless of
            // whether they are v6 or v4...
            ip if (ip.is_loopback() || ip.is_unspecified())
                // ...unless we are inside of an integration test, in which
                // case, lol. lmao.
                && self.loopback_policy == TreatLoopbackAsExternal::No =>
            {
                Err(ExternalIpError::Loopback { ip })
            }
            // Note that we need not also check if the IP is contained by the
            // rack subnet, because the AZ subnet contains the rack subnet.
            _ => Ok(ip),
        }
    }
}

#[derive(Clone)]
pub struct ExternalHttpClient {
    client: reqwest::Client,
    ip_policy: ExternalIpPolicy,
}

/// Errors returned by [`ExternalHttpClient::request`] and friends.
#[derive(thiserror::Error, Debug)]
pub enum ExternalUrlError {
    /// The supplied URL could not be parsed into a [`reqwest::Url`].
    #[error(transparent)]
    IntoUrl(#[from] reqwest::Error),
    /// The URL's host is an IP address which is not external, such as an
    /// underlay network address or a loopback address --- or the rack is not
    /// yet initialized, so no determination can be made.
    #[error(
        "IP addresses in URLs for external requests must be external to the \
         underlay network"
    )]
    NotExternalIp(#[from] ExternalIpError),
    /// The URL's host is a name in the special-use `localhost.` zone, which
    /// resolves to loopback addresses.
    #[error(
        "external HTTP requests may not be made to localhost (URL host \
         {host:?} is in the \"localhost.\" zone)"
    )]
    Localhost { host: String },
}

/// A builder for [`ExternalHttpClient`]s, wrapping a
/// [`reqwest::ClientBuilder`].
///
/// All client options *except* the redirect policy and DNS resolver are set
/// using the wrapped [`reqwest::ClientBuilder`], which is then converted into
/// an `ExternalClientBuilder` using its [`From`]`<reqwest::ClientBuilder>`
/// impl.
///
/// ## Overwritten `reqwest::ClientBuilder` options
///
/// * The redirect policy must be set using [`ExternalClientBuilder::redirect`],
///   and *not* using [`reqwest::ClientBuilder::redirect`]. This is because the
///   built client checks the target of every redirect against the external IP
///   policy before consulting the redirect policy, which requires wrapping the
///   redirect policy in [our own](redirect::Policy::custom), and `reqwest`
///   does not provide a way to get a previously-set redirect policy back out of its
///   builder in order to wrap it. Unfortunately, there isn't any way to detect
///   whether we would be clobbering a policy provided by the caller, so...just
///   make sure not to do that, I guess. Sigh.
///
/// * Any DNS resolver set on the wrapped builder using
///   [`reqwest::ClientBuilder::dns_resolver`] is ignored, since this builder
///   always uses the [`external_dns::Resolver`].
///
/// * This is not *technically* an "overwritten `reqwest::ClientBuilder`
///   option", but I'm putting it here anyway: any
///   `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` environment variables are ignored,
///   since nobody should be setting those in an Omicron zone in the first
///   place.
#[must_use = "builders do nothing unless, well...built"]
pub struct ExternalClientBuilder {
    builder: reqwest::ClientBuilder,
    redirect: Option<redirect::Policy>,
}

impl From<reqwest::ClientBuilder> for ExternalClientBuilder {
    fn from(builder: reqwest::ClientBuilder) -> Self {
        Self { builder, redirect: None }
    }
}

impl Default for ExternalClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ExternalClientBuilder {
    /// Returns a new `ExternalClientBuilder` with default options.
    pub fn new() -> Self {
        Self::from(reqwest::ClientBuilder::new())
    }

    /// Sets the [`redirect::Policy`] for the built client.
    ///
    /// The provided policy is consulted only for redirect targets that are
    /// permitted by the external IP policy. A redirect target that is not
    /// external (i.e., an underlay or loopback address, or a domain name that
    /// resolves to one) fails the request with an [`ExternalUrlError`], without
    /// consulting the provided policy at all. Note that this means a redirect
    /// to a non-external target fails with an error even if the provided policy
    /// is [`redirect::Policy::none`], which would otherwise stop and return the
    /// redirect response itself.
    ///
    /// If this method is not called, the built client uses `reqwest`'s
    /// default redirect policy (following up to 10 redirects), wrapped in the
    /// same external IP policy check.
    pub fn redirect(mut self, policy: redirect::Policy) -> Self {
        self.redirect = Some(policy);
        self
    }

    /// Constructs a new [`ExternalHttpClient`] from the provided
    /// [`ExternalHttpClientConfig`], using the provided
    /// [`external_dns::Resolver`] for name resolution.
    ///
    /// # Errors
    ///
    /// This method fails if the [`reqwest::Client`] could not be built, such
    /// as if a TLS backend cannot be initialized.
    pub fn build(
        self,
        config: &ExternalHttpClientConfig,
        resolver: &Arc<external_dns::Resolver>,
    ) -> Result<ExternalHttpClient, reqwest::Error> {
        #[allow(unused_mut)] // `mut` is unused on non-unix targets
        let ExternalClientBuilder { mut builder, redirect } = self;

        // If we are configured to only bind external TCP connections on a
        // specific interface, do so.
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "illumos",
        ))]
        {
            if let Some(ref interface) = config.interface {
                builder = builder.interface(interface);
            }
        }

        let ip_policy = resolver.ip_policy().clone();

        // Wrap the provided redirect policy (or reqwest's default, if none was
        // provided) in one that checks redirect targets against the external IP
        // policy before delegating to it. The check must come first, since
        // `redirect::Action` is opaque, so we cannot ask the wrapped policy
        // what it would do and check only the targets it would actually follow.
        let redirect_policy = {
            let inner = redirect.unwrap_or_default();
            let ip_policy = ip_policy.clone();
            redirect::Policy::custom(move |attempt| {
                match ensure_external_url(attempt.url(), &ip_policy) {
                    Ok(()) => inner.redirect(attempt),
                    Err(error) => attempt.error(error),
                }
            })
        };

        let client = builder
            .redirect(redirect_policy)
            .dns_resolver(resolver.clone())
            // Disable `reqwest`'s default behavior of honoring the
            // `HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY` environment
            // variables. We don't ever expect these to be set in a real Omicron
            // zone, and if they were to be set, they could cause us to send
            // requests to underlay destinations despite all the work we go to
            // not to do that here. If they're set on someone's dev box while
            // running tests, honoring them would similarly screw up the test.
            // So, turn this off.
            .no_proxy()
            .build()?;

        Ok(ExternalHttpClient { client, ip_policy })
    }
}

impl ExternalHttpClient {
    /// Constructs a new `ExternalHttpClient` from the provided
    /// [`ExternalHttpClientConfig`], using the provided
    /// [`external_dns::Resolver`] for name resolution.
    ///
    /// # Errors
    ///
    /// This method fails if the [`reqwest::Client`] could not be built, such
    /// as if a TLS backend cannot be initialized.
    #[allow(dead_code)] // may be used later
    pub fn new(
        config: &ExternalHttpClientConfig,
        resolver: &Arc<external_dns::Resolver>,
    ) -> Result<Self, reqwest::Error> {
        ExternalClientBuilder::new().build(config, resolver)
    }

    /// Convenience method to make a `GET` request to a URL.
    ///
    /// # Errors
    ///
    /// This method returns an error under the same conditions as
    /// [`ExternalHttpClient::request`].
    #[allow(dead_code)] // Added for completeness, may be used later
    pub fn get<U: IntoUrl>(
        &self,
        url: U,
    ) -> Result<reqwest::RequestBuilder, ExternalUrlError> {
        self.request(Method::GET, url)
    }

    /// Convenience method to make a `POST` request to a URL.
    ///
    /// # Errors
    ///
    /// This method returns an error under the same conditions as
    /// [`ExternalHttpClient::request`].
    pub fn post<U: IntoUrl>(
        &self,
        url: U,
    ) -> Result<reqwest::RequestBuilder, ExternalUrlError> {
        self.request(Method::POST, url)
    }

    /// Convenience method to make a `PUT` request to a URL.
    ///
    /// # Errors
    ///
    /// This method returns an error under the same conditions as
    /// [`ExternalHttpClient::request`].
    #[allow(dead_code)] // Added for completeness, may be used later
    pub fn put<U: IntoUrl>(
        &self,
        url: U,
    ) -> Result<reqwest::RequestBuilder, ExternalUrlError> {
        self.request(Method::PUT, url)
    }

    /// Convenience method to make a `PATCH` request to a URL.
    ///
    /// # Errors
    ///
    /// This method returns an error under the same conditions as
    /// [`ExternalHttpClient::request`].
    #[allow(dead_code)] // Added for completeness, may be used later
    pub fn patch<U: IntoUrl>(
        &self,
        url: U,
    ) -> Result<reqwest::RequestBuilder, ExternalUrlError> {
        self.request(Method::PATCH, url)
    }

    /// Convenience method to make a `DELETE` request to a URL.
    ///
    /// # Errors
    ///
    /// This method returns an error under the same conditions as
    /// [`ExternalHttpClient::request`].
    #[allow(dead_code)] // Added for completeness, may be used later
    pub fn delete<U: IntoUrl>(
        &self,
        url: U,
    ) -> Result<reqwest::RequestBuilder, ExternalUrlError> {
        self.request(Method::DELETE, url)
    }

    /// Convenience method to make a `HEAD` request to a URL.
    ///
    /// # Errors
    ///
    /// This method returns an error under the same conditions as
    /// [`ExternalHttpClient::request`].
    #[allow(dead_code)] // Added for completeness, may be used later
    pub fn head<U: IntoUrl>(
        &self,
        url: U,
    ) -> Result<reqwest::RequestBuilder, ExternalUrlError> {
        self.request(Method::HEAD, url)
    }

    /// Start building a `Request` with the `Method` and `Url`.
    ///
    /// Returns a `RequestBuilder`, which will allow setting headers and
    /// the request body before sending.
    ///
    /// # Errors
    ///
    /// This method returns an error under the following conditions:
    ///
    /// * [`ExternalUrlError::IntoUrl`] if the provided `url` could not be
    ///   parsed into a URL.
    /// * [`ExternalUrlError::NotExternalIp`] if the URL's host is an IP
    ///   address that is not external --- an underlay network address, or
    ///   (unless the policy permits it) a loopback address --- or if the
    ///   rack's underlay subnets are not yet known.
    /// * [`ExternalUrlError::Localhost`] if the URL's host is a name in the
    ///   `localhost.` zone and the policy does not permit loopback addresses.
    pub fn request<U: IntoUrl>(
        &self,
        method: Method,
        url: U,
    ) -> Result<reqwest::RequestBuilder, ExternalUrlError> {
        let url = url.into_url()?;
        ensure_external_url(&url, &self.ip_policy)?;
        Ok(self.client.request(method, url))
    }

    /// Executes a `Request`.
    ///
    /// A `Request` can be built manually with `Request::new()` or obtained
    /// from a RequestBuilder with `RequestBuilder::build()`.
    ///
    /// This method's API is somewhat different from that of
    /// [`reqwest::Client::execute`], which it is intended to replicate.
    /// Reqwest's `execute` function returns a [`Future`]`<Output = `[`Result`]
    /// `<`[`reqwest::Response`]`, `[`reqwest::Error`]`>>`, while this method
    /// *synchronously* returns a [`Result`]* containing a similar [`Future`].
    /// The outer `Result` will be an [`ExternalUrlError`] if the request's URL
    /// is not allowed by the external IP policy, while the inner [`Future`]
    /// outputs the underlying [`reqwest::Response`] or [`reqwest::Error`]
    /// returned by `reqwest` upon actually executing the HTTP request. Callers
    /// can therefore handle "this URL is not allowed" and "the request failed
    /// on the wire" at two different points, rather than having to dig both out
    /// of a combined error type after the fact.
    ///
    /// The `reqwest` documentation tells us that we "should prefer to use the
    /// `RequestBuilder` and `RequestBuilder::send()`". I disagree with them,
    /// because doing so returns both errors that occurred while *building* the
    /// request, which are likely to be programmer errors, and errors that
    /// occurred while *sending* the request, which are generally runtime
    /// errors, in the same function call, and forces you to manually go through
    /// all the different error variants that `reqwest` is liable to return and
    /// figure out what to do with them yourself. But I will repeat their
    /// guidance here nonetheless, with a heavy sigh of frustration.
    ///
    /// # Errors
    ///
    /// This method returns an [`ExternalUrlError`] if the request's URL is
    /// rejected by the external IP policy, under the same conditions as
    /// [`ExternalHttpClient::request`].
    ///
    /// The returned future fails if there was an error while sending the
    /// request, a redirect loop was detected, or the redirect limit was
    /// exhausted.
    pub fn execute(
        &self,
        request: reqwest::Request,
    ) -> Result<
        impl Future<Output = Result<reqwest::Response, reqwest::Error>>,
        ExternalUrlError,
    > {
        // ...and here we arrive at one of my least favorite things about
        // `reqwest`'s API. It has this `execute` method, which takes an
        // arbitrary `Request`, and...executes it. Like it says on the tin. But
        // it _also_ has a `RequestBuilder::send` method, which is on the
        // `RequestBuilder` type, and which builds the `Request` *and goes and
        // sends it*. This works because the `RequestBuilder` owns a clone of
        // the `Client` that it uses to send the request if you call
        // `RequestBuilder::send`.
        //
        // I assume all of this was done to make it so you could feel slightly
        // more like you were writing JavaScript, or something, but it makes
        // wrapping the `reqwest::Client` in a thing that performs validation of
        // the request (which is what this whole module is) infinitely more
        // painful. Now, because one can send a request either by having the
        // `RequestBuilder` build the request and pass it to `Client::execute`,
        // *or* by calling `RequestBuilder::send`, which internally calls
        // `Client::execute`, you have two different ways to send the request.
        // If we don't validate the URL in *both* locations, you can
        // (accidentally or on purpose) smuggle a request past the validation we
        // want to do here. So we have to check the URL in both the
        // `ExternalClient::request` method, which gives you a `RequestBuilder`,
        // and *again* in `ExternalClient::execute`, even though the `Request`
        // you are trying to `execute` *may* have come from
        // `ExternalClient::request` in the first place, and therefore been
        // validated *twice*. Have I mentioned that `reqwest`'s API is not
        // really my favorite thing?
        //
        // Sigh. Whatever. It's fine.
        ensure_external_url(request.url(), &self.ip_policy)?;
        let client = self.client.clone();
        Ok(async move { client.execute(request).await })
    }
}

fn ensure_external_url(
    url: &Url,
    policy: &ExternalIpPolicy,
) -> Result<(), ExternalUrlError> {
    match url.host() {
        // Domain names are mostly allowed here: if the name *resolves* to a
        // non-external IP, that will be rejected by `external_dns::Resolver`,
        // later. The exception is, of course, "localhost". As per RFC 6761 §
        // 6.3, resolvers may answer names in it with loopback addresses without
        // consulting DNS at all, so reject it eagerly here rather than relying
        // on the resolver --- unless the loopback policy permits it, in which
        // case the name is passed through to the resolver like any other.
        Some(url::Host::Domain(domain))
            if policy.loopback_policy == TreatLoopbackAsExternal::No =>
        {
            let name = domain.trim_end_matches('.');
            // The special-use "localhost." zone can, according to RFC 6761 §
            // 6.3, contain subdomains. Although this usage is pretty uncommon,
            // it would also resolve to a loopback address and therefore must be
            // rejected as well.
            let last_label = name.rsplit('.').next().unwrap_or(name);
            if last_label.eq_ignore_ascii_case("localhost") {
                return Err(ExternalUrlError::Localhost {
                    host: domain.to_string(),
                });
            }
        }
        Some(url::Host::Domain(_)) => {}
        // If the host is an IP address, check that it is considered an external
        // IP.
        Some(url::Host::Ipv6(v6)) => {
            policy.ensure_external_ip(IpAddr::V6(v6))?;
        }
        Some(url::Host::Ipv4(v4)) => {
            policy.ensure_external_ip(IpAddr::V4(v4))?;
        }
        None => {}
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use internal_dns_types::config::DnsRecord;
    use omicron_common::address::{
        Ipv6Subnet, RACK_PREFIX, ReservedRackSubnet, UNDERLAY_MULTICAST_SUBNET,
        UNDERLAY_MULTICAST_SUBNET_LAST,
    };
    use std::collections::HashMap;
    use std::net::Ipv6Addr;
    use transient_dns_server::TransientDnsServer;

    // Until the rack is initialized, no determination can be made about IP
    // hosts, so they are rejected. Domain hosts still pass, since their
    // resolved addresses are checked by `external_dns::Resolver` (which
    // fails fast before rack initialization itself).
    #[test]
    fn test_ensure_external_url_rejects_ip_hosts_before_rack_init() {
        let policy = ExternalIpPolicy::new(
            Arc::new(OnceLock::new()),
            TreatLoopbackAsExternal::No,
        );
        let url = "http://[2001:db8::1]/".parse::<Url>().unwrap();
        let result = ensure_external_url(&url, &policy);
        assert!(
            matches!(
                &result,
                Err(ExternalUrlError::NotExternalIp(
                    ExternalIpError::RackNotInitialized { .. }
                ))
            ),
            "an IP-host URL must be rejected before the underlay subnets \
             are known, but got: {result:?}"
        );
    }

    #[test]
    fn test_ensure_external_url_rejects_underlay_ipv6_hosts() {
        let subnets = underlay_subnets();
        let az = subnets.az_subnet.net();
        let rack = subnets.rack_subnet.net();
        // An arbitrary host address on this rack's subnet.
        let rack_ip = underlay_ip();
        // An address in a sled subnet (the rack's second /64).
        let sled_ip = nth_addr(rack, (1 << 64) + 5);
        // An address in a different rack's /56, in the same AZ.
        let other_rack_ip = {
            let rack_size = 1u128 << (128 - RACK_PREFIX);
            let rack_offset =
                u128::from(rack.first_addr()) - u128::from(az.first_addr());
            nth_addr(az, rack_offset + rack_size + 5)
        };
        // An internal DNS server address, derived from the reserved rack
        // subnet the same way production does.
        let dns_ip = ReservedRackSubnet::from_subnet(subnets.rack_subnet)
            .get_dns_subnets()[0]
            .dns_address();
        // An address in the middle of the underlay multicast subnet.
        let multicast_ip = nth_addr(UNDERLAY_MULTICAST_SUBNET, (1 << 16) + 2);

        let cases = vec![
            format!("http://[{rack_ip}]/"),
            // Explicit port, path, query, and fragment don't change the answer.
            format!("http://[{rack_ip}]:12345/some/path?q=1#frag"),
            // Neither does the scheme...
            format!("https://[{rack_ip}]/"),
            // ...nor userinfo.
            format!("http://user:pass@[{rack_ip}]/"),
            format!("http://[{sled_ip}]/"),
            format!("http://[{other_rack_ip}]/"),
            format!("http://[{dns_ip}]/"),
            // The very first address in the AZ subnet...
            format!("http://[{}]/", az.first_addr()),
            // ...and the very last.
            format!("http://[{}]/", az.last_addr()),
            // The underlay multicast subnet is also part of the underlay
            // network: first address...
            format!("http://[{}]/", UNDERLAY_MULTICAST_SUBNET.first_addr()),
            // ...one in the middle...
            format!("http://[{multicast_ip}]/"),
            // ...and the last.
            format!("http://[{UNDERLAY_MULTICAST_SUBNET_LAST}]/"),
            // Non-canonical textual forms are normalized by the URL parser
            // before we see them: uppercase/verbose...
            format!("http://[{}]/", verbose_upper(rack_ip)),
            // ...and an embedded dotted-quad tail.
            format!("http://[{}]/", dotted_quad_tail(rack)),
        ];
        for url in &cases {
            let result = test_url(url);
            assert!(
                result.is_err(),
                "URL {url:?} has an underlay IP host and should have been \
                 rejected, but got: {result:?}"
            );
        }
    }

    #[test]
    fn test_ensure_external_url_allows_external_ipv6_hosts() {
        let az = underlay_subnets().az_subnet.net();
        // The addresses immediately below and above the AZ subnet: the
        // tightest possible off-by-one boundary cases.
        let below_az = Ipv6Addr::from(u128::from(az.first_addr()) - 1);
        let above_az = Ipv6Addr::from(u128::from(az.last_addr()) + 1);
        // The address immediately after the underlay multicast subnet.
        let after_multicast =
            Ipv6Addr::from(u128::from(UNDERLAY_MULTICAST_SUBNET_LAST) + 1);
        // A ULA that is *not* the underlay (e.g. a guest VPC prefix).
        // ULA-ness alone must not be the discriminator.
        let other_ula: Ipv6Addr = "fd12:3456:789a::1".parse().unwrap();
        assert!(
            !az.contains(other_ula),
            "the test's \"non-underlay ULA\" ({other_ula}) is within the \
             test AZ subnet ({az}); pick a different one"
        );

        let cases = vec![
            // Global unicast.
            "http://[2001:db8::1]/".to_string(),
            format!("http://[{other_ula}]/"),
            format!("http://[{below_az}]/"),
            format!("http://[{above_az}]/"),
            // Link-local.
            "http://[fe80::1]/".to_string(),
            // Admin-scoped multicast just *outside* the underlay multicast
            // subnet...
            format!("http://[{after_multicast}]/"),
            // ...and multicast in a different scope entirely (site-local).
            "http://[ff05::1:2]/".to_string(),
            // IPv4-mapped IPv6.
            "http://[::ffff:1.2.3.4]/".to_string(),
        ];
        for url in &cases {
            let result = test_url(url);
            assert!(
                result.is_ok(),
                "URL {url:?} is not an underlay IP and should have been \
                 allowed, but got: {result:?}"
            );
        }
    }

    // There is no IPv4 underlay, so non-loopback IPv4 hosts pass the check
    // today, including RFC 1918 addresses. If an IPv4 range ever becomes
    // internal-only, `ensure_external_ip` is the place to teach, and these
    // cases should be revisited.
    #[test]
    fn test_ensure_external_url_allows_ipv4_hosts() {
        const CASES: &[&str] = &[
            "http://1.2.3.4/",
            "http://1.2.3.4:8080/webhooks",
            "http://10.0.0.1/",
            "http://192.168.1.1:8080/",
        ];
        for url in CASES {
            // Verify the parser actually produced an IPv4 host: without this,
            // a case that parsed as a *domain* would also pass the check, and
            // this test would silently stop covering the IPv4 arm.
            let parsed = url.parse::<Url>().unwrap();
            assert!(
                matches!(parsed.host(), Some(url::Host::Ipv4(_))),
                "expected URL {url:?} to parse as an IPv4 host, but got: {:?}",
                parsed.host(),
            );
            let result = test_url(url);
            assert!(
                result.is_ok(),
                "URL {url:?} is an IPv4 host and should have been allowed \
                 (there is no IPv4 underlay), but got: {result:?}"
            );
        }
    }

    // Loopback is not external, in any of its spellings: IPv6, IPv4 (the
    // whole 127.0.0.0/8), or names in the special-use "localhost." zone.
    //
    // NOTE: this means test environments cannot point an ExternalHttpClient
    // at a server on localhost; tests exercising webhook delivery et al.
    // must bind receivers to a non-loopback address.
    #[test]
    fn test_ensure_external_url_rejects_loopback() {
        const CASES: &[&str] = &[
            // IPv6 loopback.
            "http://[::1]/",
            "https://[::1]:8443/receiver",
            // IPv4 loopback, including the rest of 127.0.0.0/8.
            "http://127.0.0.1/",
            "http://127.0.0.1:8080/webhooks",
            "http://127.255.255.254/",
            // The WHATWG URL parser canonicalizes non-dotted-quad IPv4 forms
            // into `Host::Ipv4` before we see them, so these hit the IPv4 arm
            // rather than sneaking through as "domains": hexadecimal...
            "http://0x7f.0.0.1/",
            // ...and a single decimal integer (127.0.0.1).
            "http://2130706433/",
            // The "localhost." zone (RFC 6761 § 6.3): the name itself...
            "http://localhost/",
            "http://localhost:8080/webhooks",
            // ...its subdomains, which are also in the zone...
            "http://webhooks.localhost/",
            // ...case-insensitively...
            "http://LOCALHOST/",
            // ...and fully-qualified (with a trailing dot).
            "http://localhost./",
        ];
        for url in CASES {
            let result = test_url(url);
            assert!(
                result.is_err(),
                "URL {url:?} is loopback and should have been rejected, \
                 but got: {result:?}"
            );
        }
    }

    // With `LoopbackPolicy::AllowedForTests`, all the loopback spellings are
    // permitted...
    #[test]
    fn test_ensure_external_url_allows_loopback_for_tests() {
        const CASES: &[&str] = &[
            "http://[::1]/",
            "https://[::1]:8443/receiver",
            "http://127.0.0.1/",
            "http://127.0.0.1:8080/webhooks",
            "http://localhost/",
            "http://localhost:8080/webhooks",
            "http://webhooks.localhost/",
        ];
        for url in CASES {
            let result = test_url_loopback_allowed(url);
            assert!(
                result.is_ok(),
                "URL {url:?} is loopback, which the policy permits in test \
                 environments, but got: {result:?}"
            );
        }
    }

    // ...but underlay addresses are rejected regardless of the loopback
    // policy: it must never weaken the underlay check.
    #[test]
    fn test_loopback_policy_does_not_weaken_underlay_check() {
        let cases = vec![
            format!("http://[{}]/", underlay_ip()),
            format!(
                "http://[{}]/",
                nth_addr(UNDERLAY_MULTICAST_SUBNET, (1 << 16) + 2)
            ),
        ];
        for url in &cases {
            let result = test_url_loopback_allowed(url);
            assert!(
                result.is_err(),
                "URL {url:?} has an underlay IP host and should have been \
                 rejected even with loopback allowed, but got: {result:?}"
            );
        }
    }

    // Domain hosts (other than the "localhost." zone) always pass this
    // check. If they *resolve* to a non-external address, that's rejected by
    // `external_dns::Resolver` at connect time instead.
    #[test]
    fn test_ensure_external_url_allows_domain_hosts() {
        const CASES: &[&str] = &[
            "http://example.com/",
            "https://webhooks.example.com:8443/receiver",
            // Looks underlay-flavored, but it's a domain name.
            "http://fd00.example.com/",
            // IDN (punycoded by the parser).
            "http://b\u{fc}cher.example/",
            // "localhost" as a non-final label is *not* in the localhost
            // zone: this is a legitimate external name.
            "http://localhost.example.com/",
        ];
        for url in CASES {
            let result = test_url(url);
            assert!(
                result.is_ok(),
                "URL {url:?} has a domain host and should have been allowed \
                 (domains are checked at resolution time), but got: {result:?}"
            );
        }
    }

    // URLs with no host at all pass this check; reqwest itself refuses to send
    // requests to non-http(s) schemes, so nothing is lost by letting them
    // through here.
    #[test]
    fn test_ensure_external_url_allows_hostless_urls() {
        const CASES: &[&str] = &[
            "file:///etc/passwd",
            "data:text/plain,hello",
            "mailto:nobody@example.com",
            "unix:/var/run/thing.sock",
        ];
        for url in CASES {
            let result = test_url(url);
            assert!(
                result.is_ok(),
                "URL {url:?} has no host and should have been allowed \
                 (reqwest will reject it at send time), but got: {result:?}"
            );
        }
    }

    // A redirect to an underlay address fails the request, using the default
    // (wrapped) redirect policy.
    #[tokio::test]
    async fn test_redirect_to_underlay_is_rejected() {
        let target = underlay_ip();
        let server = httpmock::MockServer::start_async().await;
        let mock = mock_redirect(&server, &format!("http://[{target}]/")).await;
        let client = client_without_dns(ExternalClientBuilder::new());
        let error = client
            .get(server.url("/redirect"))
            .expect("the initial URL is permitted")
            .send()
            .await
            .expect_err("a redirect to an underlay address must fail");
        assert!(
            error.is_redirect(),
            "the failure should be a redirect error, but got: {error:?}"
        );
        assert_underlay_rejection(&error, target);
        // The initial request must have been sent exactly once (i.e., the
        // rejected redirect must not be retried).
        mock.assert_async().await;
    }

    // A redirect to a permitted target is delegated to the configured redirect
    // policy: reqwest's default policy follows it to the final 200.
    #[tokio::test]
    async fn test_redirect_to_permitted_target_is_followed() {
        let server = httpmock::MockServer::start_async().await;
        let ok_mock = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/ok");
                then.status(200);
            })
            .await;
        let redirect_mock = mock_redirect(&server, &server.url("/ok")).await;
        let client = client_without_dns(ExternalClientBuilder::new());
        let rsp = client
            .get(server.url("/redirect"))
            .expect("the initial URL is permitted")
            .send()
            .await
            .expect("a redirect to a permitted target must be followed");
        assert_eq!(rsp.status(), reqwest::StatusCode::OK);
        redirect_mock.assert_async().await;
        ok_mock.assert_async().await;
    }

    // A `redirect::Policy::none` is honored for permitted targets: the 3xx
    // response itself is returned, and the redirect target is never
    // requested.
    #[tokio::test]
    async fn test_wrapped_policy_none_stops_at_permitted_target() {
        let server = httpmock::MockServer::start_async().await;
        let ok_mock = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/ok");
                then.status(200);
            })
            .await;
        let redirect_mock = mock_redirect(&server, &server.url("/ok")).await;
        let client = client_without_dns(
            ExternalClientBuilder::new().redirect(redirect::Policy::none()),
        );
        let rsp = client
            .get(server.url("/redirect"))
            .expect("the initial URL is permitted")
            .send()
            .await
            .expect("`Policy::none` returns the 3xx response itself");
        assert_eq!(rsp.status(), reqwest::StatusCode::MOVED_PERMANENTLY);
        redirect_mock.assert_async().await;
        // The wrapped policy stopped the redirect, so the target must never
        // be requested.
        ok_mock.assert_calls_async(0).await;
    }

    // A URL whose domain name *resolves* to an underlay address is rejected
    // at resolution time by `external_dns::Resolver`: the URL-level check
    // passes domain names through (it can't know what they resolve to), and
    // the resolver applies the IP policy to the resolved addresses.
    #[tokio::test]
    async fn test_domain_resolving_to_underlay_is_rejected() {
        use internal_dns_types::config::DnsRecord;

        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_domain_resolving_to_underlay_is_rejected",
        );
        let target = underlay_ip();
        let records = std::collections::HashMap::from([(
            "underlay".to_string(),
            vec![DnsRecord::Aaaa(target)],
        )]);
        let (_dns, client) = client_with_dns_server(
            &logctx.log,
            records,
            ExternalClientBuilder::new(),
        )
        .await;

        let error = client
            .get("http://underlay.example.com/")
            .expect("a domain-host URL passes the URL-level check")
            .send()
            .await
            .expect_err("a domain resolving to an underlay address must fail");
        assert_underlay_rejection(&error, target);
        logctx.cleanup_successful();
    }

    // A *redirect* to a URL whose domain name resolves to an underlay
    // address is also rejected: the redirect-policy check passes domain
    // names through (like the URL-level check), the redirect is followed,
    // and the resolver then rejects the resolved address.
    #[tokio::test]
    async fn test_redirect_to_domain_resolving_to_underlay_is_rejected() {
        use internal_dns_types::config::DnsRecord;

        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_redirect_to_domain_resolving_to_underlay_is_rejected",
        );
        let target = underlay_ip();
        let records = std::collections::HashMap::from([(
            "underlay".to_string(),
            vec![DnsRecord::Aaaa(target)],
        )]);
        let (_dns, client) = client_with_dns_server(
            &logctx.log,
            records,
            ExternalClientBuilder::new(),
        )
        .await;

        let server = httpmock::MockServer::start_async().await;
        let mock = mock_redirect(&server, "http://underlay.example.com/").await;

        let error = client
            .get(server.url("/redirect"))
            .expect("the initial URL is permitted")
            .send()
            .await
            .expect_err(
                "a redirect to a domain resolving to an underlay address \
                 must fail",
            );
        assert_underlay_rejection(&error, target);
        // The redirecting endpoint must have been requested exactly once.
        mock.assert_async().await;
        logctx.cleanup_successful();
    }

    // The `external_dns::Resolver` policy does *not* reject domains that
    // resolve to addresses that aren't on the underlay (in this case, including
    // loopback, since we are configured to permit it for tests).
    #[tokio::test]
    async fn test_domain_resolving_to_permitted_address_is_connected() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_domain_resolving_to_permitted_address_is_connected",
        );
        let records = std::collections::HashMap::from([(
            "ok".to_string(),
            vec![DnsRecord::A(std::net::Ipv4Addr::LOCALHOST)],
        )]);
        let (_dns, client) = client_with_dns_server(
            &logctx.log,
            records,
            ExternalClientBuilder::new(),
        )
        .await;

        let server = httpmock::MockServer::start_async().await;
        let ok_mock = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/ok");
                then.status(200);
            })
            .await;

        let rsp = client
            .get(format!("http://ok.example.com:{}/ok", server.port()))
            .expect("a domain-host URL passes the URL-level check")
            .send()
            .await
            .expect("a domain resolving to a permitted address must work");
        assert_eq!(rsp.status(), reqwest::StatusCode::OK);
        ok_mock.assert_async().await;
        logctx.cleanup_successful();
    }

    // The external IP policy check runs *before* the wrapped policy: a
    // redirect to an underlay address fails with an error even when the
    // wrapped policy is `Policy::none`, which would otherwise stop and
    // return the 3xx response, as in the test above.
    #[tokio::test]
    async fn test_underlay_redirect_rejected_even_with_policy_none() {
        let target = underlay_ip();
        let server = httpmock::MockServer::start_async().await;
        let mock = mock_redirect(&server, &format!("http://[{target}]/")).await;
        let client = client_without_dns(
            ExternalClientBuilder::new().redirect(redirect::Policy::none()),
        );
        let error = client
            .get(server.url("/redirect"))
            .expect("the initial URL is permitted")
            .send()
            .await
            .expect_err(
                "a redirect to an underlay address must fail even with \
                 `Policy::none`",
            );
        assert!(
            error.is_redirect(),
            "the failure should be a redirect error, but got: {error:?}"
        );
        assert_underlay_rejection(&error, target);
        mock.assert_async().await;
    }

    // === various test utilities ===

    fn test_policy(
        loopback_policy: TreatLoopbackAsExternal,
    ) -> ExternalIpPolicy {
        ExternalIpPolicy::new(
            Arc::new(OnceLock::from(underlay_subnets())),
            loopback_policy,
        )
    }

    /// Returns the `UnderlaySubnets` for the test rack subnet
    /// (`nexus_test_utils::RACK_SUBNET`).
    ///
    /// Test addresses are derived from this rather than hardcoded, so that
    /// they remain on the underlay if the test rack subnet ever changes.
    fn underlay_subnets() -> UnderlaySubnets {
        let rack_subnet: ipnetwork::Ipv6Network =
            nexus_test_utils::RACK_SUBNET.parse().unwrap();
        UnderlaySubnets::new(Ipv6Subnet::<RACK_PREFIX>::from(rack_subnet))
    }

    /// Returns the `n`th address in `net`, asserting that it is actually
    /// within `net` (i.e., that `n` doesn't overflow the subnet's host
    /// bits).
    fn nth_addr(net: Ipv6Net, n: u128) -> Ipv6Addr {
        let addr = Ipv6Addr::from(u128::from(net.first_addr()) + n);
        assert!(net.contains(addr), "address {addr} must be within {net}");
        addr
    }

    /// An arbitrary host address on the test rack's subnet, used wherever a
    /// test just needs "some underlay address".
    fn underlay_ip() -> Ipv6Addr {
        nth_addr(underlay_subnets().rack_subnet.net(), 1)
    }

    /// Renders `addr` in fully-expanded, uppercase textual form (e.g.
    /// `FD00:1122:3344:0100:0000:0000:0000:0001`), for exercising the URL
    /// parser's normalization of non-canonical spellings.
    fn verbose_upper(addr: Ipv6Addr) -> String {
        addr.segments().map(|segment| format!("{segment:04X}")).join(":")
    }

    /// Formats an address within `net` (whose bits 64..96 must be zero) in
    /// the embedded dotted-quad textual form (e.g.
    /// `fd00:1122:3344:100::1.2.3.4`), for exercising the URL parser's
    /// normalization of non-canonical spellings.
    fn dotted_quad_tail(net: Ipv6Net) -> String {
        let s = net.first_addr().segments();
        assert_eq!(
            s[4..6],
            [0, 0],
            "the `::` in the dotted-quad form requires bits 64..96 of {net} \
             to be zero"
        );
        format!("{:x}:{:x}:{:x}:{:x}::1.2.3.4", s[0], s[1], s[2], s[3])
    }

    /// Asserts that `error`'s cause chain contains an
    /// [`ExternalIpError::Underlay`] rejection of `ip`.
    #[track_caller]
    fn assert_underlay_rejection(
        error: &(dyn std::error::Error + 'static),
        expected_ip: Ipv6Addr,
    ) {
        let mut found_ip_error = None;
        let mut next = Some(error);
        while let Some(error) = dbg!(next) {
            if let Some(e) = dbg!(error.downcast_ref::<ExternalIpError>()) {
                found_ip_error = Some(e);
                break;
            }
            next = error.source();
        }
        match found_ip_error {
            Some(ExternalIpError::Underlay { ip, .. }) => assert_eq!(
                *ip,
                IpAddr::V6(expected_ip),
                "the underlay rejection should be for {expected_ip}",
            ),
            other => panic!(
                "the error chain should contain an underlay rejection, but \
                 found {other:?} (chain: {})",
                slog_error_chain::InlineErrorChain::new(error),
            ),
        }
    }

    #[track_caller]
    fn test_url(url: &str) -> Result<Url, ExternalUrlError> {
        let url = dbg!(url)
            .parse::<Url>()
            .unwrap_or_else(|e| panic!("test URL {url:?} must parse: {e}"));
        let result = ensure_external_url(
            &url,
            &test_policy(TreatLoopbackAsExternal::No),
        )
        .map(|()| url);
        dbg!(result)
    }

    #[track_caller]
    fn test_url_loopback_allowed(url: &str) -> Result<Url, ExternalUrlError> {
        let url = url
            .parse::<Url>()
            .unwrap_or_else(|e| panic!("test URL {url:?} must parse: {e}"));
        ensure_external_url(
            &url,
            &test_policy(TreatLoopbackAsExternal::YesForTestPurposesOnly),
        )
        .map(|()| url)
    }

    /// Mocks `GET /redirect` on `server` to respond with a 301 to
    /// `redirect_to`.
    async fn mock_redirect<'a>(
        server: &'a httpmock::MockServer,
        redirect_to: &str,
    ) -> httpmock::Mock<'a> {
        server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/redirect");
                then.status(301).header("location", redirect_to);
            })
            .await
    }

    /// Builds an `ExternalHttpClient` from `builder` using the provided
    /// resolver, with loopback treated as external so that the test servers
    /// on localhost are reachable.
    fn client_with_resolver(
        builder: ExternalClientBuilder,
        resolver: Arc<external_dns::Resolver>,
    ) -> ExternalHttpClient {
        let config = ExternalHttpClientConfig {
            interface: None,
            treat_loopback_as_external:
                TreatLoopbackAsExternal::YesForTestPurposesOnly,
        };
        builder.build(&config, &resolver).expect("client must build")
    }

    /// Builds an `ExternalHttpClient` from `builder` for tests that use only
    /// IP-literal URLs.
    fn client_without_dns(
        builder: ExternalClientBuilder,
    ) -> ExternalHttpClient {
        // The DNS server address is never actually queried, since the tests
        // using this helper use IP-literal URLs; the resolver just needs to
        // exist in order to build the client.
        let resolver = Arc::new(external_dns::Resolver::new(
            &[IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
            test_policy(TreatLoopbackAsExternal::YesForTestPurposesOnly),
        ));
        client_with_resolver(builder, resolver)
    }

    /// Spawns a `TransientDnsServer` serving the given `records` in the
    /// `example.com` zone, and returns it along with an
    /// `ExternalHttpClient` whose resolver points at it.
    ///
    /// The server is returned so that it stays alive for the duration of the
    /// test.
    async fn client_with_dns_server(
        log: &slog::Logger,
        records: HashMap<String, Vec<DnsRecord>>,
        builder: ExternalClientBuilder,
    ) -> (TransientDnsServer, ExternalHttpClient) {
        use internal_dns_types::config::DnsConfigParams;
        use internal_dns_types::config::DnsConfigZone;
        use omicron_common::api::external::Generation;

        let dns =
            TransientDnsServer::new(log).await.expect("DNS server must start");
        dns.initialize_with_config(
            log,
            &DnsConfigParams {
                generation: Generation::new(),
                serial: 0,
                time_created: chrono::Utc::now(),
                zones: vec![DnsConfigZone {
                    zone_name: "example.com".to_string(),
                    records,
                }],
            },
        )
        .await
        .expect("DNS server must accept its config");

        let resolver = Arc::new(external_dns::Resolver::new_from_addr(
            dns.dns_server.local_address(),
            test_policy(TreatLoopbackAsExternal::YesForTestPurposesOnly),
        ));
        let client = client_with_resolver(builder, resolver);
        (dns, client)
    }
}
