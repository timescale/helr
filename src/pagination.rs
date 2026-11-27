//! Pagination: parse Link header (RFC 5988) and return next URL.
//!
//! Format: Link: <url>; rel="next"[, <url2>; rel="prev"]

use reqwest::header::HeaderMap;

/// Parse Link header and return the URL for the given `rel` (e.g. "next").
/// Handles multiple Link headers and comma-separated entries in one header.
pub fn next_link_from_headers(headers: &HeaderMap, rel: &str) -> Option<String> {
    let link_values = headers.get_all(reqwest::header::LINK);
    for value in link_values {
        if let Ok(s) = value.to_str()
            && let Some(url) = parse_link_header(s, rel)
        {
            return Some(url);
        }
    }
    None
}

/// Parse a single Link header value string. Format: <uri>; rel="next"[, <uri2>; rel="prev"]
/// URIs may contain commas, so links are split on `<...>` rather than on `,`.
fn parse_link_header(s: &str, want_rel: &str) -> Option<String> {
    let mut rest = s;
    while let Some(start) = rest.find('<') {
        let (uri, tail) = rest[start + 1..].split_once('>')?;
        let params_end = tail.find('<').unwrap_or(tail.len());
        for param in tail[..params_end].split([';', ',']) {
            if let Some(rel_val) = param.trim().strip_prefix("rel=") {
                let rel_val = rel_val.trim().trim_matches('"');
                if rel_val
                    .split_whitespace()
                    .any(|r| r.eq_ignore_ascii_case(want_rel))
                {
                    return Some(uri.trim().to_string());
                }
            }
        }
        rest = &tail[params_end..];
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderValue, LINK};

    fn header_map(link: &str) -> HeaderMap {
        let mut m = HeaderMap::new();
        m.insert(LINK, HeaderValue::from_str(link).unwrap());
        m
    }

    #[test]
    fn parse_next_link() {
        let h = header_map(r#"<https://api.example.com/logs?after=xyz>; rel="next""#);
        assert_eq!(
            next_link_from_headers(&h, "next"),
            Some("https://api.example.com/logs?after=xyz".into())
        );
    }

    #[test]
    fn parse_next_and_self() {
        let h = header_map(
            r#"<https://api.example.com/logs>; rel="self", <https://api.example.com/logs?after=abc>; rel="next""#,
        );
        assert_eq!(
            next_link_from_headers(&h, "next"),
            Some("https://api.example.com/logs?after=abc".into())
        );
    }

    #[test]
    fn parse_next_with_comma_in_uri_and_multi_value_rel() {
        let h = header_map(
            r#"<https://api.example.com/logs?ids=1,2>; rel="prev", <https://api.example.com/logs?ids=3,4&after=z>; rel="next last""#,
        );
        assert_eq!(
            next_link_from_headers(&h, "next"),
            Some("https://api.example.com/logs?ids=3,4&after=z".into())
        );
    }

    #[test]
    fn no_next_returns_none() {
        let h = header_map(r#"<https://api.example.com/logs>; rel="self""#);
        assert_eq!(next_link_from_headers(&h, "next"), None);
    }

    #[test]
    fn rel_case_insensitive() {
        let h = header_map(r#"<https://api.example.com/next>; rel="Next""#);
        assert_eq!(
            next_link_from_headers(&h, "next"),
            Some("https://api.example.com/next".into())
        );
    }

    #[test]
    fn empty_link_header_returns_none() {
        let h = header_map("");
        assert_eq!(next_link_from_headers(&h, "next"), None);
    }

    #[test]
    fn malformed_link_missing_angle_returns_none() {
        let h = header_map(r#"https://api.example.com/next; rel="next""#);
        assert_eq!(next_link_from_headers(&h, "next"), None);
    }

    #[test]
    fn malformed_link_unclosed_brace_returns_none() {
        let h = header_map(r#"<https://api.example.com/next; rel="next""#);
        assert_eq!(next_link_from_headers(&h, "next"), None);
    }
}
