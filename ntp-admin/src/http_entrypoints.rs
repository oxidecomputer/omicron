// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
use ntp_admin_api::*;
use ntp_admin_types::timesync::TimeSync;
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

const TIMESYNC_STRATUM_MAX: u8 = 9;
const TIMESYNC_REFTIME_MIN: f64 = 1234567890.0;
const TIMESYNC_CORRECTION_MAX: f64 = 0.05;
// CockroachDB panics if any node's clock is more than 400ms from at least half
// of its peers. See
// https://www.cockroachlabs.com/docs/stable/recommended-production-settings#clock-synchronization
// for more details.
//
// If all nodes have max_error <= 175ms, the worst-case
// inter-node difference is 350ms, keeping us well within CRDB's threshold.
//
// This value is in seconds.
const TIMESYNC_MAX_ERROR_MAX: f64 = 0.275;
const TIMESYNC_REF_IDS_NO_PEER_SYNC: [u32; 2] = [0, 0x7f7f0101];

fn parse_timesync_result(stdout: &str) -> Result<TimeSync, TimeSyncError> {
    let v: Vec<&str> = stdout.split(',').collect();

    if v.len() < 12 {
        return Err(TimeSyncError::FailedToParse {
            reason: "too few fields",
            stdout: stdout.to_string(),
        });
    }

    let Ok(ref_id) = u32::from_str_radix(v[0], 16) else {
        return Err(TimeSyncError::FailedToParse {
            reason: "bad ref_id",
            stdout: stdout.to_string(),
        });
    };
    let ip_addr =
        IpAddr::from_str(v[1]).unwrap_or(Ipv6Addr::UNSPECIFIED.into());
    let Ok(stratum) = u8::from_str(v[2]) else {
        return Err(TimeSyncError::FailedToParse {
            reason: "bad stratum",
            stdout: stdout.to_string(),
        });
    };
    let Ok(ref_time) = f64::from_str(v[3]) else {
        return Err(TimeSyncError::FailedToParse {
            reason: "bad ref_time",
            stdout: stdout.to_string(),
        });
    };
    let Ok(correction) = f64::from_str(v[4]) else {
        return Err(TimeSyncError::FailedToParse {
            reason: "bad correction",
            stdout: stdout.to_string(),
        });
    };
    let Ok(last_offset) = f64::from_str(v[5]) else {
        return Err(TimeSyncError::FailedToParse {
            reason: "bad last_offset",
            stdout: stdout.to_string(),
        });
    };
    let Ok(rms_offset) = f64::from_str(v[6]) else {
        return Err(TimeSyncError::FailedToParse {
            reason: "bad rms_offset",
            stdout: stdout.to_string(),
        });
    };
    let Ok(root_delay) = f64::from_str(v[10]) else {
        return Err(TimeSyncError::FailedToParse {
            reason: "bad root_delay",
            stdout: stdout.to_string(),
        });
    };
    let Ok(root_dispersion) = f64::from_str(v[11]) else {
        return Err(TimeSyncError::FailedToParse {
            reason: "bad root_dispersion",
            stdout: stdout.to_string(),
        });
    };

    let max_error = compute_max_error(correction, root_delay, root_dispersion);

    // Per `chronyc waitsync`'s implementation, if either the
    // reference IP address is not unspecified or the reference
    // ID is not 0 or 0x7f7f0101, we are synchronized to a peer.
    let peer_sync = !ip_addr.is_unspecified()
        || !TIMESYNC_REF_IDS_NO_PEER_SYNC.contains(&ref_id);

    let sync = stratum <= TIMESYNC_STRATUM_MAX
        && TIMESYNC_REFTIME_MIN < ref_time
        && peer_sync
        && correction.abs() <= TIMESYNC_CORRECTION_MAX
        && max_error <= TIMESYNC_MAX_ERROR_MAX;

    Ok(TimeSync {
        sync,
        ref_id,
        ip_addr,
        stratum,
        ref_time,
        correction,
        last_offset,
        rms_offset,
        root_delay,
        root_dispersion,
        max_error,
    })
}

/// Compute the maximum clock error from the current offset, root delay, and
/// dispersion.
///
/// The returned value is defined by:
///
/// ```ignore
/// max_error <= |correction| + root_delay / 2 + root_dispersion.
/// ```
///
/// See <https://chrony-project.org/doc/latest/chronyc.html> for a reference.
///
/// The term `root_delay / 2` comes from the assumption that the RTT delay from
/// us to the root stratum is symmetric, i.e., on average the same going to the
/// server as coming back from it. To that, we add the accumulated error at each
/// layer from all sources (dispersion). Adding the correction gets us an
/// estimate of how far off the _system clock_ is from the estimate of "true
/// time", which is the maximum error that programs using `gettimeofday(3)` or
/// similar would see.
const fn compute_max_error(
    correction: f64,
    root_delay: f64,
    root_dispersion: f64,
) -> f64 {
    correction.abs() + root_delay / 2.0 + root_dispersion
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
        let stdout = String::from_utf8_lossy(&output.stdout);
        let result = parse_timesync_result(&stdout);
        info!(log, "parse_timesync_result"; "result" => ?result);
        result
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    // Standard field values used across tests. chronyc -c tracking fields are:
    // ref_id, ip_addr, stratum, ref_time, correction (v[4]),
    // last_offset (v[5]), rms_offset (v[6]), freq_ppm (v[7]),
    // resid_freq_ppm (v[8]), skew_ppm (v[9]),
    // root_delay (v[10]), root_dispersion (v[11]), ...
    const GOOD_FIELDS_5_TO_11: &str = "0.001,0.001,x,x,x,0.01,0.001";

    #[test]
    fn test_parse_timesync_result_success() {
        let input = format!(
            "C0A80001,192.168.0.1,2,1234567891.123456,0.001,{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input).unwrap();
        assert_eq!(result.ref_id, 0xC0A80001);
        assert_eq!(result.ip_addr, IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)));
        assert_eq!(result.stratum, 2);
        assert_eq!(result.ref_time, 1234567891.123456);
        assert_eq!(result.correction, 0.001);
        assert_eq!(result.last_offset, 0.001);
        assert_eq!(result.rms_offset, 0.001);
        assert_eq!(result.root_delay, 0.01);
        assert_eq!(result.root_dispersion, 0.001);
        assert_eq!(result.max_error, 0.01 / 2.0 + 0.001);
        assert!(result.sync);
    }

    #[test]
    fn test_parse_timesync_result_not_synced_high_stratum() {
        let stratum = TIMESYNC_STRATUM_MAX + 1;
        let input = format!(
            "C0A80001,192.168.0.1,{stratum},1234567891.123456,0.001,{GOOD_FIELDS_5_TO_11}"
        );
        let result = parse_timesync_result(&input).unwrap();
        assert!(!result.sync);
    }

    #[test]
    fn test_parse_timesync_result_not_synced_old_ref_time() {
        let ref_time = TIMESYNC_REFTIME_MIN - 0.01;
        let input = format!(
            "C0A80001,192.168.0.1,2,{ref_time},0.001,{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input).unwrap();
        assert!(!result.sync);
    }

    #[test]
    fn test_parse_timesync_result_boundary_correction() {
        let input = format!(
            "C0A80001,192.168.0.1,2,1234567891.123456,0.05,{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input).unwrap();
        assert_eq!(result.correction, 0.05);
        assert!(result.sync);
    }

    #[test]
    fn test_parse_timesync_result_not_synced_high_correction() {
        let correction = TIMESYNC_CORRECTION_MAX + 0.01;
        let input = format!(
            "C0A80001,192.168.0.1,2,1234567891.123456,{correction},{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input).unwrap();
        assert!(!result.sync);
    }

    #[test]
    fn test_parse_timesync_result_negative_correction() {
        let input = format!(
            "C0A80001,192.168.0.1,2,1234567891.123456,-0.01,{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input).unwrap();
        assert_eq!(result.correction, -0.01);
        assert!(result.sync);
    }

    #[test]
    fn test_parse_timesync_result_not_synced_no_peer() {
        let input =
            format!("0,::/0,2,1234567891.123456,0.001,{GOOD_FIELDS_5_TO_11}");

        let result = parse_timesync_result(&input).unwrap();
        assert!(!result.sync);
    }

    #[test]
    fn test_parse_timesync_result_special_ref_id() {
        let input =
            format!("0,::/0,2,1234567891.123456,0.001,{GOOD_FIELDS_5_TO_11}");
        let result = parse_timesync_result(&input).unwrap();
        assert_eq!(result.ref_id, TIMESYNC_REF_IDS_NO_PEER_SYNC[0]);
        assert!(!result.sync);

        let input = format!(
            "7f7f0101,::/0,2,1234567891.123456,0.001,{GOOD_FIELDS_5_TO_11}"
        );
        let result = parse_timesync_result(&input).unwrap();
        assert_eq!(result.ref_id, TIMESYNC_REF_IDS_NO_PEER_SYNC[1]);
        assert!(!result.sync);

        let input = format!(
            "7f7f0102,192.168.0.1,2,1234567891.123456,0.001,{GOOD_FIELDS_5_TO_11}"
        );
        let result = parse_timesync_result(&input).unwrap();
        assert_eq!(result.ref_id, 0x7f7f0102);
        assert!(result.sync);
    }

    #[test]
    fn test_parse_timesync_result_ipv6_address() {
        let input = format!(
            "C0A80001,2001:db8::1,2,1234567891.123456,0.001,{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input).unwrap();
        assert_eq!(result.ip_addr, "2001:db8::1".parse::<IpAddr>().unwrap());
        assert!(result.sync);
    }

    #[test]
    fn test_parse_timesync_result_invalid_ip_fallback() {
        let input = format!(
            "C0A80001,invalid_ip,2,1234567891.123456,0.001,{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input).unwrap();
        assert_eq!(result.ip_addr, IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        // Still syncs if ref_id is not in TIMESYNC_REF_IDS_NO_PEER_SYNC
        assert!(result.sync);
    }

    #[test]
    fn test_parse_timesync_result_not_synced_high_max_error() {
        // root_delay/2 + root_dispersion > TIMESYNC_MAX_ERROR_MAX
        let input = "C0A80001,192.168.0.1,2,1234567891.123456,0.001,0.001,0.001,x,x,x,0.3,0.1";

        let result = parse_timesync_result(input).unwrap();
        assert_eq!(result.root_delay, 0.3);
        assert_eq!(result.root_dispersion, 0.1);
        assert_eq!(result.max_error, 0.3 / 2.0 + 0.1);
        assert!(!result.sync);
    }

    #[test]
    fn test_parse_timesync_result_boundary_max_error() {
        // root_delay/2 + root_dispersion == TIMESYNC_MAX_ERROR_MAX exactly
        let root_delay = 0.2;
        let root_dispersion = 0.1;
        let input = format!(
            "C0A80001,192.168.0.1,2,1234567891.123456,0.001,0.001,0.001,x,x,x,{root_delay},{root_dispersion}"
        );

        let result = parse_timesync_result(&input).unwrap();
        assert_eq!(result.max_error, TIMESYNC_MAX_ERROR_MAX);
        assert!(result.sync);
    }

    #[test]
    fn test_parse_timesync_result_too_few_fields() {
        let input = "C0A80001,192.168.0.1,2,1234567891.123456";

        let result = parse_timesync_result(input);
        assert!(result.is_err());
        match result.unwrap_err() {
            TimeSyncError::FailedToParse { reason, .. } => {
                assert_eq!(reason, "too few fields");
            }
            _ => panic!("Expected FailedToParse error"),
        }
    }

    #[test]
    fn test_parse_timesync_result_invalid_ref_id() {
        let input = format!(
            "INVALID,192.168.0.1,2,1234567891.123456,0.001,{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input);
        assert!(result.is_err());
        match result.unwrap_err() {
            TimeSyncError::FailedToParse { reason, .. } => {
                assert_eq!(reason, "bad ref_id");
            }
            _ => panic!("Expected FailedToParse error"),
        }
    }

    #[test]
    fn test_parse_timesync_result_invalid_stratum() {
        let input = format!(
            "C0A80001,192.168.0.1,invalid,1234567891.123456,0.001,{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input);
        assert!(result.is_err());
        match result.unwrap_err() {
            TimeSyncError::FailedToParse { reason, .. } => {
                assert_eq!(reason, "bad stratum");
            }
            _ => panic!("Expected FailedToParse error"),
        }
    }

    #[test]
    fn test_parse_timesync_result_invalid_ref_time() {
        let input = format!(
            "C0A80001,192.168.0.1,2,invalid,0.001,{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input);
        assert!(result.is_err());
        match result.unwrap_err() {
            TimeSyncError::FailedToParse { reason, .. } => {
                assert_eq!(reason, "bad ref_time");
            }
            _ => panic!("Expected FailedToParse error"),
        }
    }

    #[test]
    fn test_parse_timesync_result_invalid_correction() {
        let input = format!(
            "C0A80001,192.168.0.1,2,1234567891.123456,invalid,{GOOD_FIELDS_5_TO_11}"
        );

        let result = parse_timesync_result(&input);
        assert!(result.is_err());
        match result.unwrap_err() {
            TimeSyncError::FailedToParse { reason, .. } => {
                assert_eq!(reason, "bad correction");
            }
            _ => panic!("Expected FailedToParse error"),
        }
    }

    #[test]
    fn test_parse_timesync_result_invalid_last_offset() {
        let input = "C0A80001,192.168.0.1,2,1234567891.123456,0.001,invalid,0.001,x,x,x,0.01,0.001";

        let result = parse_timesync_result(input);
        assert!(result.is_err());
        match result.unwrap_err() {
            TimeSyncError::FailedToParse { reason, .. } => {
                assert_eq!(reason, "bad last_offset");
            }
            _ => panic!("Expected FailedToParse error"),
        }
    }

    #[test]
    fn test_parse_timesync_result_invalid_rms_offset() {
        let input = "C0A80001,192.168.0.1,2,1234567891.123456,0.001,0.001,invalid,x,x,x,0.01,0.001";

        let result = parse_timesync_result(input);
        assert!(result.is_err());
        match result.unwrap_err() {
            TimeSyncError::FailedToParse { reason, .. } => {
                assert_eq!(reason, "bad rms_offset");
            }
            _ => panic!("Expected FailedToParse error"),
        }
    }

    #[test]
    fn test_parse_timesync_result_invalid_root_delay() {
        let input = "C0A80001,192.168.0.1,2,1234567891.123456,0.001,0.001,0.001,x,x,x,invalid,0.001";

        let result = parse_timesync_result(input);
        assert!(result.is_err());
        match result.unwrap_err() {
            TimeSyncError::FailedToParse { reason, .. } => {
                assert_eq!(reason, "bad root_delay");
            }
            _ => panic!("Expected FailedToParse error"),
        }
    }

    #[test]
    fn test_parse_timesync_result_invalid_root_dispersion() {
        let input = "C0A80001,192.168.0.1,2,1234567891.123456,0.001,0.001,0.001,x,x,x,0.01,invalid";

        let result = parse_timesync_result(input);
        assert!(result.is_err());
        match result.unwrap_err() {
            TimeSyncError::FailedToParse { reason, .. } => {
                assert_eq!(reason, "bad root_dispersion");
            }
            _ => panic!("Expected FailedToParse error"),
        }
    }
}
