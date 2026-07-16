// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::external_dns;

use nexus_config::ExternalHttpClientConfig;
use nexus_config::TreatLoopbackAsExternal;
use omicron_common::address::UNDERLAY_MULTICAST_SUBNET;
use omicron_common::address::UnderlaySubnets;
use oxnet::Ipv6Net;

use qorb::resolver;
use reqwest::IntoUrl;
use reqwest::Method;
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
    #[error(
        "expected an external IP, but address {ip} is within the underlay \
         subnet {subnet}"
    )]
    Underlay { ip: IpAddr, subnet: Ipv6Net },
    #[error("expected an external IP, but {ip} is a loopback address")]
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
            // Loopback addresses are not external, regardless of whether they
            // are v6 or v4...
            ip if ip.is_loopback()
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
    #[error(transparent)]
    NotExternalIp(#[from] ExternalIpError),
    /// The URL's host is a name in the special-use `localhost.` zone, which
    /// resolves to loopback addresses.
    #[error(
        "external HTTP requests may not be made to localhost (URL host \
         {host:?} is in the \"localhost.\" zone)"
    )]
    Localhost { host: String },
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
    pub fn new(
        config: &ExternalHttpClientConfig,
        resolver: &Arc<external_dns::Resolver>,
    ) -> Result<Self, reqwest::Error> {
        Self::from_builder(config, resolver, reqwest::ClientBuilder::new())
    }

    /// Constructs a new `ExternalHttpClient` from the provided
    /// [`reqwest::ClientBuilder`] and [`ExternalHttpClientConfig`], using the
    /// provided [`external_dns::Resolver`] for name resolution.
    ///
    /// # Errors
    ///
    /// This method fails if the [`reqwest::Client`] could not be built, such
    /// as if a TLS backend cannot be initialized.
    pub fn from_builder(
        config: &ExternalHttpClientConfig,
        resolver: &Arc<external_dns::Resolver>,
        mut builder: reqwest::ClientBuilder,
    ) -> Result<Self, reqwest::Error> {
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

        let client = builder.dns_resolver(resolver.clone()).build()?;
        let ip_policy = resolver.ip_policy().clone();

        Ok(Self { client, ip_policy })
    }

    /// Convenience method to make a `GET` request to a URL.
    ///
    /// # Errors
    ///
    /// This method returns an error under the same conditions as
    /// [`ExternalHttpClient::request`].
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
    /// * [`UrlError::IntoUrl`] if the provided `url` could not be parsed into a
    ///   URL.
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
        let url = ensure_external_url(url, &self.ip_policy)?;
        Ok(self.client.request(method, url))
    }
}

fn ensure_external_url(
    url: Url,
    policy: &ExternalIpPolicy,
) -> Result<Url, ExternalUrlError> {
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

    Ok(url)
}

#[cfg(test)]
mod test {
    use super::*;
    use omicron_common::address::{Ipv6Subnet, RACK_PREFIX};

    fn test_policy(
        loopback_policy: TreatLoopbackAsExternal,
    ) -> ExternalIpPolicy {
        let rack_subnet: ipnetwork::Ipv6Network =
            nexus_test_utils::RACK_SUBNET.parse().unwrap();
        let subnets =
            UnderlaySubnets::new(Ipv6Subnet::<RACK_PREFIX>::from(rack_subnet));
        ExternalIpPolicy::new(
            Arc::new(OnceLock::from(subnets)),
            loopback_policy,
        )
    }

    #[track_caller]
    fn test_url(url: &str) -> Result<Url, ExternalUrlError> {
        let url = url
            .parse::<Url>()
            .unwrap_or_else(|e| panic!("test URL {url:?} must parse: {e}"));
        ensure_external_url(url, &test_policy(TreatLoopbackAsExternal::No))
    }

    #[track_caller]
    fn test_url_loopback_allowed(url: &str) -> Result<Url, ExternalUrlError> {
        let url = url
            .parse::<Url>()
            .unwrap_or_else(|e| panic!("test URL {url:?} must parse: {e}"));
        ensure_external_url(
            url,
            &test_policy(TreatLoopbackAsExternal::YesForTestPurposesOnly),
        )
    }

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
        let result = ensure_external_url(url, &policy);
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
        const CASES: &[&str] = &[
            // An address within this rack's subnet.
            "http://[fd00:1122:3344:100::1]/",
            // Explicit port, path, query, and fragment don't change the answer.
            "http://[fd00:1122:3344:100::1]:12345/some/path?q=1#frag",
            // Neither does the scheme...
            "https://[fd00:1122:3344:100::1]/",
            // ...nor userinfo.
            "http://user:pass@[fd00:1122:3344:100::1]/",
            // A sled subnet within this rack.
            "http://[fd00:1122:3344:101::5]/",
            // A different rack in the same AZ.
            "http://[fd00:1122:3344:200::5]/",
            // An internal DNS server address (in the reserved rack subnet).
            "http://[fd00:1122:3344:1::1]/",
            // The very first address in the AZ /48...
            "http://[fd00:1122:3344::]/",
            // ...and the very last.
            "http://[fd00:1122:3344:ffff:ffff:ffff:ffff:ffff]/",
            // The underlay multicast subnet (ff04::/64) is also part of the
            // underlay network: first address...
            "http://[ff04::]/",
            // ...one in the middle...
            "http://[ff04::1:2]/",
            // ...and the last (`UNDERLAY_MULTICAST_SUBNET_LAST`).
            "http://[ff04::ffff:ffff:ffff:ffff]/",
            // Non-canonical textual forms are normalized by the URL parser
            // before we see them: uppercase/verbose...
            "http://[FD00:1122:3344:0100:0000:0000:0000:0001]/",
            // ...and an embedded dotted-quad tail.
            "http://[fd00:1122:3344:100::1.2.3.4]/",
        ];
        for url in CASES {
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
        const CASES: &[&str] = &[
            // Global unicast.
            "http://[2001:db8::1]/",
            // A ULA that is *not* the underlay (e.g. a guest VPC prefix).
            // ULA-ness alone must not be the discriminator.
            "http://[fd12:3456:789a::1]/",
            // Adjacent /48s: one below the AZ subnet...
            "http://[fd00:1122:3343:ffff::1]/",
            // ...and one above.
            "http://[fd00:1122:3345::1]/",
            // Link-local.
            "http://[fe80::1]/",
            // Admin-scoped multicast just *outside* the underlay multicast
            // /64 subnet...
            "http://[ff04:0:0:1::1]/",
            // ...and multicast in a different scope entirely (site-local).
            "http://[ff05::1:2]/",
            // IPv4-mapped IPv6.
            "http://[::ffff:1.2.3.4]/",
        ];
        for url in CASES {
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
        const CASES: &[&str] = &[
            "http://[fd00:1122:3344:100::1]/",
            "http://[fd00:1122:3344:1::1]/",
            "http://[ff04::1:2]/",
        ];
        for url in CASES {
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
}
