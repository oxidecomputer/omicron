// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
use ntp_admin_api::*;
use ntp_admin_types::TimeSync;
use slog::error;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::str::FromStr;
use std::sync::Arc;

type NtpApiDescription = dropshot::ApiDescription<Arc<ServerContext>>;

pub fn api() -> NtpApiDescription {
    ntp_admin_api_mod::api_description::<NtpAdminImpl>()
        .expect("registered entrypoints")
}

#[derive(Debug, thiserror::Error)]
pub enum TimeSyncError {
    #[error("failed to execute chronyc within NTP zone")]
    ExecuteChronyc(#[source] std::io::Error),
    #[error(
        "failed to parse chronyc tracking output: {reason} (stdout: {stdout:?})"
    )]
    FailedToParse { reason: &'static str, stdout: String },
}

impl From<TimeSyncError> for HttpError {
    fn from(err: TimeSyncError) -> Self {
        // All errors are currently treated as 500s
        HttpError::for_internal_error(InlineErrorChain::new(&err).to_string())
    }
}

enum NtpAdminImpl {}

impl NtpAdminImpl {
    async fn timesync_get(
        ctx: &ServerContext,
    ) -> Result<TimeSync, TimeSyncError> {
        let log = ctx.log();
        info!(log, "querying chronyc");

        let output = tokio::process::Command::new("/usr/bin/chronyc")
            .args(["-c", "tracking"])
            .output()
            .await
            .map_err(TimeSyncError::ExecuteChronyc)?;
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();

        let v: Vec<&str> = stdout.split(',').collect();

        if v.len() < 10 {
            error!(log, "too few fields"; "stdout" => &stdout);
            return Err(TimeSyncError::FailedToParse {
                reason: "too few fields",
                stdout,
            });
        }

        let Ok(ref_id) = u32::from_str_radix(v[0], 16) else {
            error!(log, "failed to parse ref_id"; "stdout" => &stdout);
            return Err(TimeSyncError::FailedToParse {
                reason: "bad ref_id",
                stdout,
            });
        };
        let ip_addr =
            IpAddr::from_str(v[1]).unwrap_or(Ipv6Addr::UNSPECIFIED.into());
        let Ok(stratum) = u8::from_str(v[2]) else {
            error!(log, "bad stratum"; "stdout" => &stdout);
            return Err(TimeSyncError::FailedToParse {
                reason: "bad stratum",
                stdout,
            });
        };
        let Ok(ref_time) = f64::from_str(v[3]) else {
            error!(log, "bad ref_time"; "stdout" => &stdout);
            return Err(TimeSyncError::FailedToParse {
                reason: "bad ref_time",
                stdout,
            });
        };
        let Ok(correction) = f64::from_str(v[4]) else {
            error!(log, "bad correction"; "stdout" => &stdout);
            return Err(TimeSyncError::FailedToParse {
                reason: "bad correction",
                stdout,
            });
        };

        // Per `chronyc waitsync`'s implementation, if either the
        // reference IP address is not unspecified or the reference
        // ID is not 0 or 0x7f7f0101, we are synchronized to a peer.
        let peer_sync =
            !ip_addr.is_unspecified() || (ref_id != 0 && ref_id != 0x7f7f0101);

        let sync = stratum < 10
            && ref_time > 1234567890.0
            && peer_sync
            && correction.abs() <= 0.05;

        info!(log, "timesync result"; "sync" => sync);

        Ok(TimeSync { sync, ref_id, ip_addr, stratum, ref_time, correction })
    }
}

impl NtpAdminApi for NtpAdminImpl {
    type Context = Arc<ServerContext>;

    async fn timesync(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<TimeSync>, HttpError> {
        let ctx = rqctx.context();
        let response = Self::timesync_get(ctx).await?;
        Ok(HttpResponseOk(response))
    }
}
