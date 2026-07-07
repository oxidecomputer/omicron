// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common objects used for configuration

use anyhow::Context;
use std::fmt;
use std::net::SocketAddr;
use std::ops::Deref;
use std::str::FromStr;

/// Describes a URL for connecting to a PostgreSQL server
// The config pattern that we're using requires that types in the config impl
// Serialize.  If tokio_postgres::config::Config impl'd Serialize or even
// Display, we'd just use that directly instead of this type.  But it doesn't.
// We could implement a serialize function ourselves, but URLs support many
// different properties, and this could be brittle and easy to get wrong.
// Instead, this type just wraps tokio_postgres::config::Config and keeps the
// original String around.  (The downside is that a consumer _generating_ a
// nexus::db::Config needs to generate a URL that matches the
// tokio_postgres::config::Config that they construct here, but this is not
// currently an important use case.)
//
// To ensure that the URL and config are kept in sync, we currently only support
// constructing one of these via `FromStr` and the fields are not public.
#[derive(Clone, Debug, PartialEq)]
pub struct PostgresConfigWithUrl {
    url_raw: String,
    config: tokio_postgres::config::Config,
}

impl PostgresConfigWithUrl {
    pub fn url(&self) -> &str {
        &self.url_raw
    }

    /// Accesses the first ip / port pair within the URL.
    ///
    /// # Panics
    ///
    /// This method makes the assumption that the hostname has at least one
    /// "host IP / port" pair which can be extracted. If the supplied URL
    /// does not have such a pair, this function will panic.
    // Yes, panicking in the above scenario sucks. But this type is already
    // pretty ubiquitous within Omicron, and integration with the qorb
    // connection pooling library requires access to database by SocketAddr.
    pub fn address(&self) -> SocketAddr {
        let tokio_postgres::config::Host::Tcp(host) =
            &self.config.get_hosts()[0]
        else {
            panic!("Non-TCP hostname");
        };
        let ip: std::net::IpAddr =
            host.parse().expect("Failed to parse host as IP address");

        let port = self.config.get_ports()[0];
        SocketAddr::new(ip, port)
    }

    /// Accesses all ip / port pairs within the URL.
    ///
    /// Returns an error if any hostname is non-TCP or not a valid IP address.
    pub fn all_addresses(&self) -> anyhow::Result<Vec<SocketAddr>> {
        self.config
            .get_hosts()
            .iter()
            .zip(self.config.get_ports())
            .map(|(host, port)| {
                let tokio_postgres::config::Host::Tcp(host) = host else {
                    anyhow::bail!("non-TCP hostname in database URL");
                };
                let ip: std::net::IpAddr = host
                    .parse()
                    .context("failed to parse host as IP address")?;
                Ok(SocketAddr::new(ip, *port))
            })
            .collect()
    }
}

impl FromStr for PostgresConfigWithUrl {
    type Err = tokio_postgres::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(PostgresConfigWithUrl { url_raw: s.to_owned(), config: s.parse()? })
    }
}

impl fmt::Display for PostgresConfigWithUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.url_raw)
    }
}

impl Deref for PostgresConfigWithUrl {
    type Target = tokio_postgres::config::Config;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

#[cfg(test)]
mod test {
    use super::PostgresConfigWithUrl;

    #[test]
    fn test_bad_url() {
        // There is surprisingly little that we can rely on the
        // tokio_postgres::config::Config parser to include in the error
        // message.
        let error = "foo".parse::<PostgresConfigWithUrl>().unwrap_err();
        assert!(
            error.to_string().contains("invalid connection string"),
            "'{error}' does not contain 'invalid connection string'"
        );
        "http://127.0.0.1:1234".parse::<PostgresConfigWithUrl>().unwrap_err();
        let error = "postgresql://example.com?sslmode=not-a-real-ssl-mode"
            .parse::<PostgresConfigWithUrl>()
            .unwrap_err();
        assert!(
            error.to_string().contains("invalid connection string"),
            "'{error}' does not contain 'invalid connection string'"
        );
    }

    #[test]
    fn test_example_url() {
        let config = "postgresql://notauser@10.2.3.4:1789?sslmode=disable"
            .parse::<PostgresConfigWithUrl>()
            .unwrap();
        assert_eq!(config.get_user(), Some("notauser"));
        assert_eq!(
            config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Disable
        );
        assert_eq!(
            config.get_hosts(),
            &[tokio_postgres::config::Host::Tcp("10.2.3.4".to_string())]
        );
        assert_eq!(config.get_ports(), &[1789]);
    }

    #[test]
    fn test_all_addresses_single_host() {
        let config = "postgresql://root@10.2.3.4:1234?sslmode=disable"
            .parse::<PostgresConfigWithUrl>()
            .unwrap();
        let addrs = config.all_addresses().unwrap();
        assert_eq!(
            addrs,
            vec!["10.2.3.4:1234".parse::<std::net::SocketAddr>().unwrap()]
        );
        // Should agree with address()
        assert_eq!(addrs[0], config.address());
    }

    #[test]
    fn test_all_addresses_multiple_hosts() {
        let config =
            "postgresql://root@10.0.0.1:100,10.0.0.2:200,10.0.0.3:300/omicron?sslmode=disable"
                .parse::<PostgresConfigWithUrl>()
                .unwrap();
        let addrs = config.all_addresses().unwrap();
        assert_eq!(
            addrs,
            vec![
                "10.0.0.1:100".parse::<std::net::SocketAddr>().unwrap(),
                "10.0.0.2:200".parse::<std::net::SocketAddr>().unwrap(),
                "10.0.0.3:300".parse::<std::net::SocketAddr>().unwrap(),
            ]
        );
    }

    #[test]
    fn test_all_addresses_non_ip_hostname() {
        // tokio_postgres parses hostnames that aren't IPs just fine,
        // but all_addresses() should return an error for them.
        let config = "postgresql://root@example.com:5432?sslmode=disable"
            .parse::<PostgresConfigWithUrl>()
            .unwrap();
        let err = config.all_addresses().unwrap_err();
        assert!(
            err.to_string().contains("failed to parse host as IP address"),
            "unexpected error: {err}"
        );
    }
}
