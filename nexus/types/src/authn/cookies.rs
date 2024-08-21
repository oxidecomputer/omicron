// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use async_trait::async_trait;
use cookie::{Cookie, CookieJar, ParseError};
use dropshot::{
    ApiEndpointBodyContentType, ExtensionMode, ExtractorMetadata, HttpError,
    RequestContext, ServerContext, SharedExtractor,
};
use newtype_derive::NewtypeDeref;
use newtype_derive::NewtypeFrom;

pub fn parse_cookies(
    headers: &http::HeaderMap<http::HeaderValue>,
) -> Result<CookieJar, ParseError> {
    let mut cookies = CookieJar::new();
    for header in headers.get_all("Cookie") {
        let raw_str =
            match header.to_str().context("parsing Cookie header as UTF-8") {
                Ok(string) => string,
                Err(_) => continue,
            };
        for chunk in raw_str.split(';').map(|s| s.trim()) {
            if let Ok(cookie) = Cookie::parse(chunk) {
                cookies.add_original(cookie.into_owned());
            }
        }
    }
    Ok(cookies)
}
pub struct Cookies(pub CookieJar);

NewtypeFrom! { () pub struct Cookies(pub CookieJar); }
NewtypeDeref! { () pub struct Cookies(pub CookieJar); }

#[async_trait]
impl SharedExtractor for Cookies {
    async fn from_request<Context: ServerContext>(
        rqctx: &RequestContext<Context>,
    ) -> Result<Self, HttpError> {
        let cookies = parse_cookies(rqctx.request.headers())
            .unwrap_or_else(|_| CookieJar::new());
        Ok(cookies.into())
    }

    fn metadata(
        _body_content_type: ApiEndpointBodyContentType,
    ) -> ExtractorMetadata {
        ExtractorMetadata {
            extension_mode: ExtensionMode::None,
            parameters: vec![],
        }
    }
}

#[cfg(test)]
mod test {
    use super::parse_cookies;
    use http::{
        header::{ACCEPT, COOKIE},
        HeaderMap,
    };

    #[test]
    fn test_parse_cookies_empty_headers() {
        let headers = HeaderMap::new();
        let cookies = parse_cookies(&headers).unwrap();
        assert_eq!(cookies.iter().count(), 0);
    }

    #[test]
    fn test_parse_cookies_one_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, "session=abc".parse().unwrap());
        let cookies = parse_cookies(&headers).unwrap();

        assert_eq!(cookies.iter().count(), 1);

        let cookie = cookies.get("session").unwrap();
        assert_eq!(cookie.name(), "session");
        assert_eq!(cookie.value(), "abc");
    }

    #[test]
    fn test_parse_cookies_two_cookies() {
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, "cookie1=abc; cookie2=def".parse().unwrap());
        let cookies = parse_cookies(&headers).unwrap();

        assert_eq!(cookies.iter().count(), 2);

        let cookie1 = cookies.get("cookie1").unwrap();
        assert_eq!(cookie1.name(), "cookie1");
        assert_eq!(cookie1.value(), "abc");

        let cookie2 = cookies.get("cookie2").unwrap();
        assert_eq!(cookie2.name(), "cookie2");
        assert_eq!(cookie2.value(), "def");
    }

    #[test]
    fn test_parse_cookies_two_cookie_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, "cookie1=abc".parse().unwrap());
        headers.append(COOKIE, "cookie2=def".parse().unwrap());

        let cookies = parse_cookies(&headers).unwrap();

        assert_eq!(cookies.iter().count(), 2);

        let cookie1 = cookies.get("cookie1").unwrap();
        assert_eq!(cookie1.name(), "cookie1");
        assert_eq!(cookie1.value(), "abc");

        let cookie2 = cookies.get("cookie2").unwrap();
        assert_eq!(cookie2.name(), "cookie2");
        assert_eq!(cookie2.value(), "def");
    }

    #[test]
    fn test_parse_cookies_two_cookie_headers_same_name() {
        // when two cookies in two separate cookie headers have the same name,
        // the second one should override the first
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, "cookie=abc".parse().unwrap());
        headers.append(COOKIE, "cookie=def".parse().unwrap());

        let cookies = parse_cookies(&headers).unwrap();

        assert_eq!(cookies.iter().count(), 1);

        let cookie = cookies.get("cookie").unwrap();
        assert_eq!(cookie.name(), "cookie");
        assert_eq!(cookie.value(), "def");
    }

    #[test]
    fn test_parse_cookies_ignore_other_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, "session=abc".parse().unwrap());
        headers.insert(ACCEPT, "application/json".parse().unwrap());

        let cookies = parse_cookies(&headers).unwrap();

        assert_eq!(cookies.iter().count(), 1);

        let cookie = cookies.get("session").unwrap();
        assert_eq!(cookie.name(), "session");
        assert_eq!(cookie.value(), "abc");
    }
}
