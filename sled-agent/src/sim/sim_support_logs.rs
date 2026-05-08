// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated implementation of the sled-agent's `support_logs_download`
//! endpoint.
//!
//! Tests inject [`super::SimLogEntry`] entries via
//! `SledAgent::insert_support_log`. This module renders the entries
//! for a given zone into an in-memory zip, applying the same
//! `start_time`/`end_time`/`max_rotated` filters as the real
//! handler.

use super::sled_agent::SledAgent;
use dropshot::HttpError;
use sled_diagnostics::LogTimeWindow;
use std::io::Write;

/// Build a zip of synthetic log entries for `zone` and return it as
/// an `application/zip` HTTP response. Entries with mtime outside the
/// requested `window` are excluded; if `max_rotated` is `Some(n)`, no
/// more than `n` entries are returned (after time filtering, sorted
/// newest-first).
pub(super) fn serve_zip(
    sa: &SledAgent,
    zone: &str,
    max_rotated: Option<usize>,
    window: LogTimeWindow,
) -> Result<http::Response<dropshot::Body>, HttpError> {
    let mut entries = {
        let logs = sa.support_logs.lock().unwrap();
        logs.get(zone).cloned().unwrap_or_default()
    };

    entries.retain(|e| {
        if let Some(start) = window.start {
            if e.mtime < start {
                return false;
            }
        }
        if let Some(end) = window.end {
            if e.mtime > end {
                return false;
            }
        }
        true
    });

    // Sort newest-first so the count cap takes the most recent.
    entries.sort_by(|a, b| b.mtime.cmp(&a.mtime));
    if let Some(n) = max_rotated {
        entries.truncate(n);
    }

    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        for entry in &entries {
            let opts: zip::write::FileOptions<'_, ()> =
                zip::write::FileOptions::default()
                    .compression_method(zip::CompressionMethod::Stored);
            zip.start_file(&entry.filename, opts).map_err(|err| {
                HttpError::for_internal_error(err.to_string())
            })?;
            zip.write_all(&entry.contents).map_err(|err| {
                HttpError::for_internal_error(err.to_string())
            })?;
        }
        zip.finish()
            .map_err(|err| HttpError::for_internal_error(err.to_string()))?;
    }

    Ok(http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/zip")
        .body(dropshot::Body::from(buf))
        .unwrap())
}
