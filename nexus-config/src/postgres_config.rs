// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common objects used for configuration

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
    pub fn url(&self) -> String {
        self.url_raw.clone()
    }

    /// Accesses the first ip / port pair within the URL.
    ///
    /// # Safety
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
        assert!(error.to_string().contains("unexpected EOF"));
        "http://127.0.0.1:1234".parse::<PostgresConfigWithUrl>().unwrap_err();
        let error = "postgresql://example.com?sslmode=not-a-real-ssl-mode"
            .parse::<PostgresConfigWithUrl>()
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid value for option `sslmode`"));
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
}
