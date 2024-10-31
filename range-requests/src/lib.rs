// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::Body;
use dropshot::HttpError;
use futures::TryStreamExt;
use hyper::{
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE},
    Response, StatusCode,
};

/// Errors which may be returned when processing range requests
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

fn bad_range_response(file_size: u64) -> Response<Body> {
    hyper::Response::builder()
        .status(StatusCode::RANGE_NOT_SATISFIABLE)
        .header(ACCEPT_RANGES, "bytes")
        .header(CONTENT_RANGE, format!("bytes */{file_size}"))
        .body(Body::empty())
        .unwrap()
}

/// Generate a GET response, optionally for a HTTP range request.  The total
/// file length should be provided, whether or not the expected Content-Length
/// for a range request is shorter.
///
/// It is the responsibility of the caller to ensure that `rx` is a stream of
/// data matching the requested range in the `range` argument, if it is
/// supplied.
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

/// Represents the raw, unparsed values of "range" from a request header.
pub struct PotentialRange(Vec<u8>);

impl PotentialRange {
    /// Parses a single range request out of the range request.
    ///
    /// `len` is the total length of the document, for the range request being made.
    ///
    /// On failure, returns a range response with the appropriate headers
    /// to inform the caller how to make a correct range request.
    pub fn parse(&self, len: u64) -> Result<SingleRange, Response<Body>> {
        self.single_range(len).map_err(|_err| bad_range_response(len))
    }

    fn single_range(&self, len: u64) -> Result<SingleRange, Error> {
        match http_range::HttpRange::parse_bytes(&self.0, len) {
            Ok(ranges) => {
                if ranges.len() != 1 || ranges[0].length < 1 {
                    // Right now, we don't want to deal with encoding a
                    // response that has multiple ranges.
                    Err(Error::MultipleRangesUnsupported)
                } else {
                    Ok(SingleRange::new(ranges[0], len)?)
                }
            }
            Err(err) => Err(Error::Parse(err)),
        }
    }
}

/// A parsed range request, and associated "total document length".
#[derive(Clone, Debug)]
pub struct SingleRange {
    range: http_range::HttpRange,
    total: u64,
}

#[cfg(test)]
impl PartialEq for SingleRange {
    fn eq(&self, other: &Self) -> bool {
        self.range.start == other.range.start
            && self.range.length == other.range.length
            && self.total == other.total
    }
}

impl SingleRange {
    pub fn new(
        range: http_range::HttpRange,
        total: u64,
    ) -> Result<Self, Error> {
        let http_range::HttpRange { start, mut length } = range;

        const INVALID_RANGE: Error =
            Error::Parse(http_range::HttpRangeParseError::InvalidRange);

        // Clip the length to avoid going beyond the end of the total range
        if start.checked_add(length).ok_or(INVALID_RANGE)? >= total {
            length = total.checked_sub(start).ok_or(INVALID_RANGE)?;
        }
        // If the length is zero, we cannot satisfy the range request
        if length == 0 {
            return Err(INVALID_RANGE);
        }

        Ok(Self { range: http_range::HttpRange { start, length }, total })
    }

    /// Return the first byte in this range for use in inclusive ranges.
    pub fn start(&self) -> u64 {
        self.range.start
    }

    /// Return the last byte in this range for use in inclusive ranges.
    pub fn end_inclusive(&self) -> u64 {
        assert!(self.range.length > 0);

        self.range
            .start
            .checked_add(self.range.length)
            .unwrap()
            .checked_sub(1)
            .unwrap()
    }

    /// Generate the Content-Range header for inclusion in a HTTP 206 partial
    /// content response using this range.
    pub fn to_content_range(&self) -> String {
        format!(
            "bytes {}-{}/{}",
            self.range.start,
            self.end_inclusive(),
            self.total
        )
    }

    /// Generate a Range header for inclusion in another HTTP request; e.g.,
    /// to a backend object store.
    pub fn to_range(&self) -> String {
        format!("bytes={}-{}", self.range.start, self.end_inclusive())
    }

    pub fn content_length(&self) -> u64 {
        assert!(self.range.length > 0);

        self.range.length
    }
}

/// A trait, implemented for [dropshot::RequestContext], to pull a range header
/// out of the request headers.
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

    use bytes::Bytes;
    use futures::stream::once;
    use http_body_util::BodyExt;
    use std::convert::Infallible;
    use tokio_util::io::ReaderStream;

    #[test]
    fn parse_range_valid() {
        // Whole range
        let pr = PotentialRange(b"bytes=0-100".to_vec());
        assert_eq!(
            pr.single_range(100).unwrap(),
            SingleRange {
                range: http_range::HttpRange { start: 0, length: 100 },
                total: 100
            }
        );

        // Clipped
        let pr = PotentialRange(b"bytes=0-100".to_vec());
        assert_eq!(
            pr.single_range(50).unwrap(),
            SingleRange {
                range: http_range::HttpRange { start: 0, length: 50 },
                total: 50
            }
        );

        // Single byte
        let pr = PotentialRange(b"bytes=49-49".to_vec());
        assert_eq!(
            pr.single_range(50).unwrap(),
            SingleRange {
                range: http_range::HttpRange { start: 49, length: 1 },
                total: 50
            }
        );
    }

    #[test]
    fn parse_range_invalid() {
        let pr = PotentialRange(b"bytes=50-50".to_vec());
        assert!(matches!(
            pr.single_range(50).expect_err("Range should be invalid"),
            Error::Parse(http_range::HttpRangeParseError::NoOverlap),
        ));

        let pr = PotentialRange(b"bytes=20-1".to_vec());
        assert!(matches!(
            pr.single_range(50).expect_err("Range should be invalid"),
            Error::Parse(http_range::HttpRangeParseError::InvalidRange),
        ));
    }

    #[test]
    fn get_response_no_range() {
        let bytes = b"Hello world";

        let response = make_get_response(
            None,
            bytes.len() as u64,
            None,
            ReaderStream::new(bytes.as_slice()),
        )
        .expect("Should have made response");

        assert_eq!(response.status(), StatusCode::OK);

        expect_headers(
            response.headers(),
            &[
                (ACCEPT_RANGES, "bytes"),
                (CONTENT_TYPE, "application/octet-stream"),
                (CONTENT_LENGTH, &bytes.len().to_string()),
            ],
        );
    }

    // Makes a get response with a Vec of bytes that counts from zero.
    //
    // The u8s aren't normal bounds on the length, but they make the mapping
    // of "the data is the index" easy.
    fn ranged_get_request(
        start: u8,
        length: u8,
        total_length: u8,
    ) -> Response<Body> {
        let range = SingleRange::new(
            http_range::HttpRange {
                start: start.into(),
                length: length.into(),
            },
            total_length.into(),
        )
        .unwrap();

        let b: Vec<_> = (u8::try_from(range.start()).unwrap()
            ..=u8::try_from(range.end_inclusive()).unwrap())
            .collect();

        let response = make_get_response(
            Some(range.clone()),
            total_length.into(),
            None,
            once(async move { Ok::<_, Infallible>(b) }),
        )
        .expect("Should have made response");

        response
    }

    // Validates the headers exactly match the map
    fn expect_headers(
        headers: &http::HeaderMap,
        expected: &[(http::HeaderName, &str)],
    ) {
        println!("Headers: {headers:#?}");
        assert_eq!(headers.len(), expected.len());
        for (k, v) in expected {
            assert_eq!(headers.get(k).unwrap(), v);
        }
    }

    // Validates the data matches an incrementing Vec of u8 values
    async fn expect_data(
        body: &mut (dyn http_body::Body<
            Data = Bytes,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Unpin),
        start: u8,
        length: u8,
    ) {
        println!("Checking data from {start}, with length {length}");
        let frame = body
            .frame()
            .await
            .expect("Error reading frame")
            .expect("Should have one frame")
            .into_data()
            .expect("Should be a DATA frame");
        assert_eq!(frame.len(), usize::from(length),);

        for i in 0..length {
            assert_eq!(frame[i as usize], i + start);
        }
    }

    #[tokio::test]
    async fn get_response_with_range() {
        // First half
        let mut response = ranged_get_request(0, 32, 64);
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        expect_data(response.body_mut(), 0, 32).await;
        expect_headers(
            response.headers(),
            &[
                (ACCEPT_RANGES, "bytes"),
                (CONTENT_TYPE, "application/octet-stream"),
                (CONTENT_LENGTH, "32"),
                (CONTENT_RANGE, "bytes 0-31/64"),
            ],
        );

        // Second half
        let mut response = ranged_get_request(32, 32, 64);
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        expect_data(response.body_mut(), 32, 32).await;
        expect_headers(
            response.headers(),
            &[
                (ACCEPT_RANGES, "bytes"),
                (CONTENT_TYPE, "application/octet-stream"),
                (CONTENT_LENGTH, "32"),
                (CONTENT_RANGE, "bytes 32-63/64"),
            ],
        );

        // Partially out of bounds
        let mut response = ranged_get_request(60, 32, 64);
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        expect_data(response.body_mut(), 60, 4).await;
        expect_headers(
            response.headers(),
            &[
                (ACCEPT_RANGES, "bytes"),
                (CONTENT_TYPE, "application/octet-stream"),
                (CONTENT_LENGTH, "4"),
                (CONTENT_RANGE, "bytes 60-63/64"),
            ],
        );

        // Fully out of bounds
        assert!(matches!(
            SingleRange::new(
                http_range::HttpRange { start: 64, length: 32 },
                64
            )
            .expect_err("Should have thrown an error"),
            Error::Parse(http_range::HttpRangeParseError::InvalidRange)
        ));
    }
}
