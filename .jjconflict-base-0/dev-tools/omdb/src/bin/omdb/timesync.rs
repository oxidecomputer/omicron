// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Check status of time-synchronization.

use crate::Omdb;
use chrono::DateTime;
use chrono::Utc;
use clap::Args;
use clap::Subcommand;
use internal_dns_types::names::ServiceName;
use ntp_admin_client::Client;
use ntp_admin_client::types::TimeSync;
use omicron_common::address::NTP_ADMIN_PORT;
use slog::Logger;
use slog::debug;
use slog::error;
use std::net::IpAddr;
use std::time::Duration;
use tabled::Table;
use tabled::Tabled;

/// Arguments for the timesync subcommand.
#[derive(Debug, Args)]
pub struct TimesyncArgs {
    #[command(subcommand)]
    command: TimesyncSubcommands,
}

/// Subcommands that query time synchronization status
#[derive(Debug, Subcommand)]
enum TimesyncSubcommands {
    /// Check the status of timesync across all NTP servers in DNS.
    Status,
}

impl TimesyncArgs {
    /// Run the command.
    pub async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<()> {
        match &self.command {
            TimesyncSubcommands::Status => {
                self.timesync_status(omdb, log).await
            }
        }
    }

    async fn timesync_status(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<()> {
        let boundaries =
            lookup_ntp_admin_servers(omdb, log, ServiceName::BoundaryNtp).await;
        let internal =
            lookup_ntp_admin_servers(omdb, log, ServiceName::InternalNtp).await;

        // Fetch timesync from all of them.
        let mut rows = Vec::with_capacity(boundaries.len() + internal.len());
        let boundaries = boundaries.into_iter().zip(std::iter::repeat(true));
        let internal = internal.into_iter().zip(std::iter::repeat(false));
        for (sock, is_boundary) in boundaries.chain(internal) {
            let url = format!("http://{sock}");
            let client = Client::new(&url, log.clone());
            let result = match client.timesync().await.map(|t| t.into_inner()) {
                Ok(ts) => {
                    let TimeSync {
                        correction,
                        ip_addr,
                        last_offset,
                        max_error,
                        ref_id,
                        ref_time,
                        rms_offset,
                        root_delay,
                        root_dispersion,
                        stratum,
                        sync,
                    } = ts;
                    let ref_time_human = compute_human_ref_time(ref_time);
                    TimesyncStatus {
                        is_boundary,
                        is_synchronized: sync,
                        upstream_ip: ip_addr,
                        ref_id,
                        stratum,
                        ref_time,
                        ref_time_human,
                        last_offset,
                        rms_offset,
                        correction,
                        root_delay,
                        root_dispersion,
                        max_error,
                    }
                }
                Err(e) => {
                    error!(
                        log,
                        "failed to collect timesync status";
                        "address" => %sock,
                        "is_boundary" => is_boundary,
                        "error" => e.to_string(),
                    );
                    continue;
                }
            };
            rows.push(result);
        }
        let table = Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();
        println!("{table}");
        Ok(())
    }
}

fn compute_human_ref_time(ref_time: f64) -> Option<DateTime<Utc>> {
    if ref_time < 0.0 || ref_time > i64::MAX as f64 {
        return None;
    }
    let secs = ref_time.floor() as i64;
    // Ignore the subsecond part here, just for human-friendliness.
    DateTime::from_timestamp(secs, 0)
}

async fn lookup_ntp_admin_servers(
    omdb: &Omdb,
    log: &Logger,
    srv: ServiceName,
) -> Vec<std::net::SocketAddrV6> {
    let records = match omdb.dns_lookup_all(log.clone(), srv).await {
        Ok(s) => {
            debug!(
                log,
                "looked up NTP services";
                "srv" => ?srv,
                "n_records" => s.len()
            );
            s
        }
        Err(e) => {
            error!(
                log,
                "failed to look up NTP SRV records";
                "srv" => ?srv,
                "error" => e.to_string(),
            );
            vec![]
        }
    };

    // The DNS SRV records are for the actual NTP service. We need the NTP admin
    // servers, which are on a different port at the same address.
    records
        .into_iter()
        .map(|mut sock| {
            sock.set_port(NTP_ADMIN_PORT);
            sock
        })
        .collect()
}

#[derive(Debug, Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct TimesyncStatus {
    /// True if this is a boundary NTP server, false otherwise.
    is_boundary: bool,
    /// True if the NTP admin server reports that time is synchronized.
    is_synchronized: bool,
    /// IP address of the current upstream source server.
    upstream_ip: IpAddr,
    /// NTP reference ID (derived from the IP address)
    #[tabled(display_with = "display_ref_id")]
    ref_id: u32,
    /// Our NTP server's stratum (the upstream stratum plus one).
    stratum: u8,
    /// The "true" NTP reference time that the NTP server thinks it is.
    #[tabled(display_with = "display_secs_f64")]
    ref_time: f64,
    /// The true NTP reference time, in human-friendly UTC format.
    #[tabled(display_with = "display_optional_utc")]
    ref_time_human: Option<DateTime<Utc>>,
    /// The offset between the local and upstream NTP clocks at the last
    /// measurement.
    #[tabled(display_with = "display_secs_f64")]
    last_offset: f64,
    /// Root-mean-square of the offsets between local and upstream NTP clocks.
    #[tabled(display_with = "display_secs_f64")]
    rms_offset: f64,
    /// The current offset between this server's NTP time and the system clock
    /// time.
    #[tabled(display_with = "display_secs_f64")]
    correction: f64,
    /// The total delay along the network path from this server to the stratum 1
    /// servers from which we're ultimately deriving our time.
    #[tabled(display_with = "display_secs_f64")]
    root_delay: f64,
    /// The total accumulated error along the path from this server to the
    /// stratum 1 server. This is due to system clock resolution, measurement
    /// errors, network jitter, etc.
    #[tabled(display_with = "display_secs_f64")]
    root_dispersion: f64,
    /// The absolute upper bound on the local clock's error from true NTP time.
    #[tabled(display_with = "display_secs_f64")]
    max_error: f64,
}

fn display_secs_f64(s: &f64) -> String {
    let sign = if *s >= 0.0 { "" } else { "-" };
    format!("{}{:?}", sign, Duration::from_secs_f64(s.abs()))
}

fn display_optional_utc(dt: &Option<DateTime<Utc>>) -> String {
    match dt {
        Some(dt) => dt.to_string(),
        None => String::from("None"),
    }
}

fn display_ref_id(x: &u32) -> String {
    format!("{x:x}")
}
