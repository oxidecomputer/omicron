// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::Body;
use dropshot::HttpError;
use dropshot::HttpResponseHeaders;
use futures::TryStreamExt;
use hyper::{
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE},
    Response, StatusCode,
};
use schemars::JsonSchema;
use serde::Serialize;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Using multiple ranges is not supported")]
    MultipleRangesUnsupported,

    #[error("Failed to parse range")]
    Parse(http_range::HttpRangeParseError),

    #[error(transparent)]
    Http(#[from] http::Error),
}

impl From<Error> for HttpError {
    fn from(err: Error) -> Self {
        match err {
            Error::MultipleRangesUnsupported | Error::Parse(_) => {
                HttpError::for_bad_request(None, err.to_string())
            }
            Error::Http(err) => err.into(),
        }
    }
}

pub fn bad_range_response(file_size: u64) -> Response<Body> {
    hyper::Response::builder()
        .status(StatusCode::RANGE_NOT_SATISFIABLE)
        .header(ACCEPT_RANGES, "bytes")
        .header(CONTENT_RANGE, format!("bytes */{file_size}"))
        .body(Body::empty())
        .unwrap()
}

#[derive(Serialize, JsonSchema)]
pub struct RangeHeaders {
    /// The unit accepted by this range request. Typically "bytes".
    #[serde(rename = "accept-ranges")]
    accept_ranges: String,
    /// The content being accessed by this range request.
    #[serde(rename = "content-type")]
    content_type: String,
    /// The total length of the document accessed via range request.
    #[serde(rename = "content-length")]
    content_length: String,
    /// The portion of this message which is accessed via range request.
    #[serde(rename = "content-range")]
    content_range: Option<String>,
}

impl Default for RangeHeaders {
    fn default() -> Self {
        Self {
            accept_ranges: "bytes".to_string(),
            content_type: "application/octet-stream".to_string(),
            content_length: 0.to_string(),
            content_range: None,
        }
    }
}

/// Generate a GET response, optionally for a HTTP range request.  The total
/// file length should be provided, whether or not the expected Content-Length
/// for a range request is shorter.
pub fn make_get_response<E, S, D>(
    range: Option<SingleRange>,
    file_length: u64,
    content_type: Option<&str>,
    rx: S,
) -> Result<Response<Body>, Error>
where
    E: Send + Sync + std::error::Error + 'static,
    D: Into<bytes::Bytes>,
    S: Send + Sync + futures::stream::Stream<Item = Result<D, E>> + 'static,
{
    Ok(make_response_common(range, file_length, content_type).body(
        Body::wrap(http_body_util::StreamBody::new(
            rx.map_ok(|b| hyper::body::Frame::data(b.into())),
        )),
    )?)
}

/// Generate a HEAD response, optionally for a HTTP range request.  The total
/// file length should be provided, whether or not the expected Content-Length
/// for a range request is shorter.
pub fn make_head_response(
    range: Option<SingleRange>,
    file_length: u64,
    content_type: Option<&str>,
) -> Result<Response<Body>, Error> {
    Ok(make_response_common(range, file_length, content_type)
        .body(Body::empty())?)
}

fn make_response_common(
    range: Option<SingleRange>,
    file_length: u64,
    content_type: Option<&str>,
) -> hyper::http::response::Builder {
    let mut res = Response::builder();
    res = res.header(ACCEPT_RANGES, "bytes");
    res = res.header(
        CONTENT_TYPE,
        content_type.unwrap_or("application/octet-stream"),
    );

    if let Some(range) = range {
        res = res.header(CONTENT_LENGTH, range.content_length().to_string());
        res = res.header(CONTENT_RANGE, range.to_content_range());
        res = res.status(StatusCode::PARTIAL_CONTENT);
    } else {
        res = res.header(CONTENT_LENGTH, file_length.to_string());
        res = res.status(StatusCode::OK);
    }

    res
}

pub struct PotentialRange(Vec<u8>);

impl PotentialRange {
    pub fn single_range(&self, len: u64) -> Result<SingleRange, Error> {
        match http_range::HttpRange::parse_bytes(&self.0, len) {
            Ok(ranges) => {
                if ranges.len() != 1 || ranges[0].length < 1 {
                    // Right now, we don't want to deal with encoding a
                    // response that has multiple ranges.
                    Err(Error::MultipleRangesUnsupported)
                } else {
                    Ok(SingleRange(ranges[0], len))
                }
            }
            Err(err) => Err(Error::Parse(err)),
        }
    }
}

#[derive(Clone)]
pub struct SingleRange(http_range::HttpRange, u64);

impl SingleRange {
    /// Return the first byte in this range for use in inclusive ranges.
    pub fn start(&self) -> u64 {
        self.0.start
    }

    /// Return the last byte in this range for use in inclusive ranges.
    pub fn end(&self) -> u64 {
        assert!(self.0.length > 0);

        self.0.start.checked_add(self.0.length).unwrap().checked_sub(1).unwrap()
    }

    /// Generate the Content-Range header for inclusion in a HTTP 206 partial
    /// content response using this range.
    pub fn to_content_range(&self) -> String {
        format!("bytes {}-{}/{}", self.0.start, self.end(), self.1)
    }

    /// Generate a Range header for inclusion in another HTTP request; e.g.,
    /// to a backend object store.
    #[allow(dead_code)]
    pub fn to_range(&self) -> String {
        format!("bytes={}-{}", self.0.start, self.end())
    }

    pub fn content_length(&self) -> u64 {
        assert!(self.0.length > 0);

        self.0.length
    }
}

pub trait RequestContextEx {
    fn range(&self) -> Option<PotentialRange>;
}

impl<T> RequestContextEx for dropshot::RequestContext<T>
where
    T: Send + Sync + 'static,
{
    /// If there is a Range header, return it for processing during response
    /// generation.
    fn range(&self) -> Option<PotentialRange> {
        self.request
            .headers()
            .get(hyper::header::RANGE)
            .map(|hv| PotentialRange(hv.as_bytes().to_vec()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio_util::io::ReaderStream;

    #[test]
    fn get_response_no_range() {
        let bytes = b"Hello world";

        let response = make_get_response(
            None,
            bytes.len() as u64,
            None,
            ReaderStream::new(bytes.as_slice()),
        ).expect("Should have mader response");

        assert_eq!(response.status(), StatusCode::OK);

        let headers = response.headers();
        println!("Headers: {headers:#?}");
        assert_eq!(headers.len(), 3);
        assert_eq!(headers.get(ACCEPT_RANGES).unwrap(), "bytes");
        assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "application/octet-stream");
        assert_eq!(headers.get(CONTENT_LENGTH).unwrap(), &bytes.len().to_string());
    }

    #[test]
    fn get_response_with_range() {
        let bytes = b"Hello world";

        let response = make_get_response(
            None,
            bytes.len() as u64,
            None,
            ReaderStream::new(bytes.as_slice()),
        ).expect("Should have mader response");

        assert_eq!(response.status(), StatusCode::OK);

        let headers = response.headers();
        println!("Headers: {headers:#?}");
        assert_eq!(headers.len(), 3);
        assert_eq!(headers.get(ACCEPT_RANGES).unwrap(), "bytes");
        assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "application/octet-stream");
        assert_eq!(headers.get(CONTENT_LENGTH).unwrap(), &bytes.len().to_string());
    }

}
