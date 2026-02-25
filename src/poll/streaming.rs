use anyhow::Context;

/// Find byte range `[start, end)` of the JSON array at `path`.
/// `start` is the index of `[`, `end` is the index one past `]`.
/// For top-level arrays (`path` is `None`), returns `(0, bytes.len())` after validation.
/// For dotted paths like `"data.records"`, navigates the object structure.
/// When no path is given and the root is an object, falls back to well-known keys:
/// `items`, `data`, `events`, `logs`, `entries`.
pub(crate) fn find_array_range(bytes: &[u8], path: Option<&str>) -> anyhow::Result<(usize, usize)> {
    let start_pos = skip_whitespace(bytes, 0);
    if start_pos >= bytes.len() {
        anyhow::bail!("empty JSON body");
    }

    match path {
        Some(p) => find_array_at_path(bytes, p),
        None => {
            if bytes[start_pos] == b'[' {
                let end = find_matching_bracket(bytes, start_pos)?;
                Ok((start_pos, end))
            } else if bytes[start_pos] == b'{' {
                find_array_at_default_keys(bytes)
            } else {
                anyhow::bail!("root JSON is neither array nor object");
            }
        }
    }
}

/// Parse body bytes for metadata, replacing the events array with `[]`.
/// Returns a small Value tree containing cursor, pagination tokens, etc.
/// For top-level arrays (where the whole body is the array), returns `Value::Null`.
pub(crate) fn extract_metadata(
    bytes: &[u8],
    array_range: (usize, usize),
) -> anyhow::Result<serde_json::Value> {
    let (array_start, array_end) = array_range;

    if array_start == 0 && array_end == bytes.len() {
        return Ok(serde_json::Value::Null);
    }

    let prefix = &bytes[..array_start];
    let suffix = &bytes[array_end..];
    let mut synthetic = Vec::with_capacity(prefix.len() + 2 + suffix.len());
    synthetic.extend_from_slice(prefix);
    synthetic.extend_from_slice(b"[]");
    synthetic.extend_from_slice(suffix);

    serde_json::from_slice(&synthetic).context("parse metadata (body with events replaced by [])")
}

/// Iterator over JSON array elements, parsing one `Value` at a time from raw bytes.
pub(crate) struct ArrayElementIter<'a> {
    bytes: &'a [u8],
    pos: usize,
    end: usize,
    done: bool,
}

impl<'a> ArrayElementIter<'a> {
    /// Create a new iterator over array elements.
    /// `array_range` is `(start, end)` where `start` is `[` index and `end` is one past `]`.
    pub(crate) fn new(bytes: &'a [u8], array_range: (usize, usize)) -> Self {
        let (start, end) = array_range;
        Self {
            bytes,
            pos: start + 1, // skip opening `[`
            end: end - 1,   // stop before closing `]`
            done: false,
        }
    }
}

impl Iterator for ArrayElementIter<'_> {
    type Item = anyhow::Result<serde_json::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        self.pos = skip_ws_and_commas(self.bytes, self.pos, self.end);
        if self.pos >= self.end {
            self.done = true;
            return None;
        }

        let slice = &self.bytes[self.pos..self.end];
        let mut stream =
            serde_json::Deserializer::from_slice(slice).into_iter::<serde_json::Value>();
        match stream.next() {
            Some(Ok(val)) => {
                self.pos += stream.byte_offset();
                Some(Ok(val))
            }
            Some(Err(e)) => {
                self.done = true;
                Some(Err(e).context("parse array element"))
            }
            None => {
                self.done = true;
                None
            }
        }
    }
}

/// Unwrap a single event element at dotted `object_path` (e.g. `"node"`).
/// Returns `None` if the path does not resolve.
pub(crate) fn unwrap_event_object(
    mut value: serde_json::Value,
    object_path: Option<&str>,
) -> Option<serde_json::Value> {
    let Some(path) = object_path else {
        return Some(value);
    };
    let mut v = &mut value;
    for segment in path.split('.') {
        v = v.get_mut(segment)?;
    }
    Some(std::mem::take(v))
}

/// High-level helper: prepare metadata + iterator from buffered body bytes.
/// Handles `on_invalid_utf8` by converting to lossy string first when needed.
pub(crate) fn parse_streaming(
    body_bytes: &[u8],
    source: &crate::config::SourceConfig,
) -> anyhow::Result<StreamingParseResult> {
    use crate::config::InvalidUtf8Behavior;

    let events_path = source.response_events_path.as_deref();

    match source.on_invalid_utf8 {
        Some(InvalidUtf8Behavior::Replace) | Some(InvalidUtf8Behavior::Escape) => {
            let lossy = String::from_utf8_lossy(body_bytes).into_owned();
            let lossy_bytes = lossy.into_bytes();
            let array_range = find_array_range(&lossy_bytes, events_path)?;
            let metadata = extract_metadata(&lossy_bytes, array_range)?;
            Ok(StreamingParseResult::Owned {
                bytes: lossy_bytes,
                array_range,
                metadata,
            })
        }
        _ => {
            let array_range = find_array_range(body_bytes, events_path)?;
            let metadata = extract_metadata(body_bytes, array_range)?;
            Ok(StreamingParseResult::Borrowed {
                array_range,
                metadata,
            })
        }
    }
}

/// Result of `parse_streaming`, which may own its bytes (lossy conversion case).
pub(crate) enum StreamingParseResult {
    /// Body bytes were valid UTF-8 or no lossy conversion needed; borrow from caller.
    Borrowed {
        array_range: (usize, usize),
        metadata: serde_json::Value,
    },
    /// Lossy conversion was applied; owns the converted bytes.
    Owned {
        bytes: Vec<u8>,
        array_range: (usize, usize),
        metadata: serde_json::Value,
    },
}

impl StreamingParseResult {
    pub(crate) fn metadata(&self) -> &serde_json::Value {
        match self {
            Self::Borrowed { metadata, .. } | Self::Owned { metadata, .. } => metadata,
        }
    }

    pub(crate) fn array_range(&self) -> (usize, usize) {
        match self {
            Self::Borrowed { array_range, .. } | Self::Owned { array_range, .. } => *array_range,
        }
    }

    pub(crate) fn iter<'a>(&'a self, body_bytes: &'a [u8]) -> ArrayElementIter<'a> {
        match self {
            Self::Borrowed { array_range, .. } => ArrayElementIter::new(body_bytes, *array_range),
            Self::Owned {
                bytes, array_range, ..
            } => ArrayElementIter::new(bytes, *array_range),
        }
    }
}

// --- Internal helpers ---

fn skip_whitespace(bytes: &[u8], mut pos: usize) -> usize {
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

fn skip_ws_and_commas(bytes: &[u8], mut pos: usize, end: usize) -> usize {
    while pos < end && (bytes[pos].is_ascii_whitespace() || bytes[pos] == b',') {
        pos += 1;
    }
    pos
}

/// Find the end of a bracket-delimited region (array or object), returning index one past the closing bracket.
fn find_matching_bracket(bytes: &[u8], start: usize) -> anyhow::Result<usize> {
    let open = bytes[start];
    let close = match open {
        b'[' => b']',
        b'{' => b'}',
        _ => anyhow::bail!("expected '[' or '{{' at position {}", start),
    };
    let mut depth = 1u32;
    let mut in_string = false;
    let mut pos = start + 1;
    while pos < bytes.len() {
        let b = bytes[pos];
        if in_string {
            if b == b'\\' {
                pos += 1; // skip escaped char
            } else if b == b'"' {
                in_string = false;
            }
        } else {
            match b {
                b'"' => in_string = true,
                _ if b == open => depth += 1,
                _ if b == close => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(pos + 1);
                    }
                }
                _ => {}
            }
        }
        pos += 1;
    }
    anyhow::bail!("unmatched bracket starting at position {}", start);
}

/// Navigate to a dotted path (e.g. `"data.records"`) and find the array value.
fn find_array_at_path(bytes: &[u8], path: &str) -> anyhow::Result<(usize, usize)> {
    let segments: Vec<&str> = path.split('.').collect();
    let mut pos = skip_whitespace(bytes, 0);

    for (i, segment) in segments.iter().enumerate() {
        if pos >= bytes.len() || bytes[pos] != b'{' {
            anyhow::bail!(
                "expected object at path segment {:?} (segment {} of {:?})",
                segment,
                i,
                path
            );
        }
        pos = find_key_in_object(bytes, pos, segment)?;
        if i < segments.len() - 1 {
            pos = skip_whitespace(bytes, pos);
        }
    }

    pos = skip_whitespace(bytes, pos);
    if pos >= bytes.len() || bytes[pos] != b'[' {
        anyhow::bail!("value at path {:?} is not an array", path);
    }
    let end = find_matching_bracket(bytes, pos)?;
    Ok((pos, end))
}

/// Inside an object starting at `obj_start`, find the value position of `key`.
fn find_key_in_object(bytes: &[u8], obj_start: usize, key: &str) -> anyhow::Result<usize> {
    let mut pos = obj_start + 1; // skip `{`
    loop {
        pos = skip_whitespace(bytes, pos);
        if pos >= bytes.len() {
            anyhow::bail!("unexpected end of object looking for key {:?}", key);
        }
        if bytes[pos] == b'}' {
            anyhow::bail!("key {:?} not found in object", key);
        }
        if bytes[pos] == b',' {
            pos += 1;
            continue;
        }
        if bytes[pos] != b'"' {
            anyhow::bail!("expected string key at position {}", pos);
        }
        let (found_key, key_end) = read_string(bytes, pos)?;
        pos = skip_whitespace(bytes, key_end);
        if pos >= bytes.len() || bytes[pos] != b':' {
            anyhow::bail!("expected ':' after key at position {}", pos);
        }
        pos = skip_whitespace(bytes, pos + 1);
        if found_key == key {
            return Ok(pos);
        }
        pos = skip_json_value(bytes, pos)?;
    }
}

/// Read a JSON string starting at `pos` (which must be `"`), returning the content and the position after the closing `"`.
fn read_string(bytes: &[u8], pos: usize) -> anyhow::Result<(String, usize)> {
    debug_assert_eq!(bytes[pos], b'"');
    let mut i = pos + 1;
    let mut s = String::new();
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\\' {
            i += 1;
            if i < bytes.len() {
                s.push(bytes[i] as char);
            }
        } else if b == b'"' {
            return Ok((s, i + 1));
        } else {
            s.push(b as char);
        }
        i += 1;
    }
    anyhow::bail!("unterminated string at position {}", pos);
}

/// Skip over one JSON value starting at `pos`, returning the position after it.
fn skip_json_value(bytes: &[u8], pos: usize) -> anyhow::Result<usize> {
    if pos >= bytes.len() {
        anyhow::bail!("unexpected end of JSON at position {}", pos);
    }
    match bytes[pos] {
        b'"' => {
            let (_, end) = read_string(bytes, pos)?;
            Ok(end)
        }
        b'{' | b'[' => find_matching_bracket(bytes, pos),
        b't' => expect_literal(bytes, pos, b"true"),
        b'f' => expect_literal(bytes, pos, b"false"),
        b'n' => expect_literal(bytes, pos, b"null"),
        b'-' | b'0'..=b'9' => skip_number(bytes, pos),
        b => anyhow::bail!("unexpected byte '{}' at position {}", b as char, pos),
    }
}

fn expect_literal(bytes: &[u8], pos: usize, literal: &[u8]) -> anyhow::Result<usize> {
    let end = pos + literal.len();
    if end > bytes.len() || &bytes[pos..end] != literal {
        anyhow::bail!(
            "expected {:?} at position {}",
            std::str::from_utf8(literal).unwrap_or("?"),
            pos
        );
    }
    Ok(end)
}

fn skip_number(bytes: &[u8], mut pos: usize) -> anyhow::Result<usize> {
    let start = pos;
    if pos < bytes.len() && bytes[pos] == b'-' {
        pos += 1;
    }
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    if pos < bytes.len() && bytes[pos] == b'.' {
        pos += 1;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
    }
    if pos < bytes.len() && (bytes[pos] == b'e' || bytes[pos] == b'E') {
        pos += 1;
        if pos < bytes.len() && (bytes[pos] == b'+' || bytes[pos] == b'-') {
            pos += 1;
        }
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
    }
    if pos == start {
        anyhow::bail!("invalid number at position {}", pos);
    }
    Ok(pos)
}

/// Find the first well-known key whose value is an array (fallback when no path is configured).
fn find_array_at_default_keys(bytes: &[u8]) -> anyhow::Result<(usize, usize)> {
    let pos = skip_whitespace(bytes, 0);
    if pos >= bytes.len() || bytes[pos] != b'{' {
        anyhow::bail!("expected object at root");
    }
    let mut scan = pos + 1; // skip `{`
    loop {
        scan = skip_whitespace(bytes, scan);
        if scan >= bytes.len() || bytes[scan] == b'}' {
            anyhow::bail!(
                "no top-level array or known events key (items/data/events/logs/entries)"
            );
        }
        if bytes[scan] == b',' {
            scan += 1;
            continue;
        }
        if bytes[scan] != b'"' {
            anyhow::bail!("expected string key at position {}", scan);
        }
        let (found_key, key_end) = read_string(bytes, scan)?;
        scan = skip_whitespace(bytes, key_end);
        if scan >= bytes.len() || bytes[scan] != b':' {
            anyhow::bail!("expected ':' after key at position {}", scan);
        }
        scan = skip_whitespace(bytes, scan + 1);

        let is_default_key = matches!(
            found_key.as_str(),
            "items" | "data" | "events" | "logs" | "entries"
        );
        if is_default_key && scan < bytes.len() && bytes[scan] == b'[' {
            let end = find_matching_bracket(bytes, scan)?;
            return Ok((scan, end));
        }
        scan = skip_json_value(bytes, scan)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_top_level_array() {
        let json = br#"[{"a":1},{"a":2}]"#;
        let (s, e) = find_array_range(json, None).unwrap();
        assert_eq!(s, 0);
        assert_eq!(e, json.len());
    }

    #[test]
    fn test_top_level_array_with_whitespace() {
        let json = b"  \n  [1, 2, 3]  ";
        let (s, e) = find_array_range(json, None).unwrap();
        assert_eq!(s, 5);
        assert_eq!(e, 14);
        assert_eq!(&json[s..e], b"[1, 2, 3]");
    }

    #[test]
    fn test_nested_path_single_segment() {
        let json = br#"{"data":[1,2,3],"meta":"ok"}"#;
        let (s, e) = find_array_range(json, Some("data")).unwrap();
        assert_eq!(&json[s..e], b"[1,2,3]");
    }

    #[test]
    fn test_nested_path_multi_segment() {
        let json = br#"{"outer":{"inner":{"records":[10,20]},"x":1}}"#;
        let (s, e) = find_array_range(json, Some("outer.inner.records")).unwrap();
        assert_eq!(&json[s..e], b"[10,20]");
    }

    #[test]
    fn test_default_keys_events() {
        let json = br#"{"cursor":"abc","events":[{"id":1}]}"#;
        let (s, e) = find_array_range(json, None).unwrap();
        assert_eq!(&json[s..e], br#"[{"id":1}]"#);
    }

    #[test]
    fn test_default_keys_items() {
        let json = br#"{"total":5,"items":[{"n":1},{"n":2}]}"#;
        let (s, e) = find_array_range(json, None).unwrap();
        assert_eq!(&json[s..e], br#"[{"n":1},{"n":2}]"#);
    }

    #[test]
    fn test_default_keys_skips_non_array() {
        let json = br#"{"data":"string","items":[1]}"#;
        let (s, e) = find_array_range(json, None).unwrap();
        assert_eq!(&json[s..e], b"[1]");
    }

    #[test]
    fn test_path_not_found() {
        let json = br#"{"a":1}"#;
        let err = find_array_range(json, Some("missing")).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_path_value_not_array() {
        let json = br#"{"data":"hello"}"#;
        let err = find_array_range(json, Some("data")).unwrap_err();
        assert!(err.to_string().contains("not an array"));
    }

    #[test]
    fn test_extract_metadata_object_with_array() {
        let json = br#"{"cursor":"next123","events":[{"a":1},{"a":2}],"total":2}"#;
        let (s, e) = find_array_range(json, Some("events")).unwrap();
        let meta = extract_metadata(json, (s, e)).unwrap();
        assert_eq!(meta["cursor"], "next123");
        assert_eq!(meta["total"], 2);
        assert!(meta["events"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_extract_metadata_top_level_array() {
        let json = b"[1,2,3]";
        let meta = extract_metadata(json, (0, json.len())).unwrap();
        assert!(meta.is_null());
    }

    #[test]
    fn test_iter_basic() {
        let json = br#"[{"id":1},{"id":2},{"id":3}]"#;
        let range = (0, json.len());
        let iter = ArrayElementIter::new(json, range);
        let items: Vec<serde_json::Value> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0]["id"], 1);
        assert_eq!(items[2]["id"], 3);
    }

    #[test]
    fn test_iter_empty_array() {
        let json = b"[]";
        let range = (0, json.len());
        let iter = ArrayElementIter::new(json, range);
        let items: Vec<serde_json::Value> = iter.map(|r| r.unwrap()).collect();
        assert!(items.is_empty());
    }

    #[test]
    fn test_iter_whitespace_and_nested() {
        let json = br#"  [ { "a" : [1,2] } , { "b" : true } ]  "#;
        let (s, e) = find_array_range(json, None).unwrap();
        let iter = ArrayElementIter::new(json, (s, e));
        let items: Vec<serde_json::Value> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["a"], serde_json::json!([1, 2]));
        assert_eq!(items[1]["b"], true);
    }

    #[test]
    fn test_iter_embedded_in_object() {
        let json = br#"{"cursor":"c1","data":[{"x":10},{"x":20}],"more":true}"#;
        let (s, e) = find_array_range(json, Some("data")).unwrap();
        let iter = ArrayElementIter::new(json, (s, e));
        let items: Vec<serde_json::Value> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[1]["x"], 20);
    }

    #[test]
    fn test_unwrap_event_object_with_path() {
        let val = serde_json::json!({"node": {"id": 1, "name": "test"}});
        let result = unwrap_event_object(val, Some("node")).unwrap();
        assert_eq!(result["id"], 1);
    }

    #[test]
    fn test_unwrap_event_object_no_path() {
        let val = serde_json::json!({"id": 1});
        let result = unwrap_event_object(val, None).unwrap();
        assert_eq!(result["id"], 1);
    }

    #[test]
    fn test_unwrap_event_object_missing_path() {
        let val = serde_json::json!({"id": 1});
        let result = unwrap_event_object(val, Some("nonexistent"));
        assert!(result.is_none());
    }

    #[test]
    fn test_unwrap_event_object_nested_path() {
        let val = serde_json::json!({"edge": {"node": {"id": 42}}});
        let result = unwrap_event_object(val, Some("edge.node")).unwrap();
        assert_eq!(result["id"], 42);
    }

    #[test]
    fn test_strings_with_escaped_quotes() {
        let json = br#"{"key":"val\"ue","arr":[1]}"#;
        let (s, e) = find_array_range(json, Some("arr")).unwrap();
        assert_eq!(&json[s..e], b"[1]");
    }

    #[test]
    fn test_deeply_nested_3_segments() {
        let json = br#"{"a":{"b":{"c":[100,200]}}}"#;
        let (s, e) = find_array_range(json, Some("a.b.c")).unwrap();
        assert_eq!(&json[s..e], b"[100,200]");
    }

    #[test]
    fn test_iter_single_element() {
        let json = br#"[{"only":true}]"#;
        let range = (0, json.len());
        let iter = ArrayElementIter::new(json, range);
        let items: Vec<serde_json::Value> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 1);
        assert!(items[0]["only"].as_bool().unwrap());
    }

    #[test]
    fn test_no_default_key_found() {
        let json = br#"{"foo":"bar","baz":42}"#;
        let err = find_array_range(json, None).unwrap_err();
        assert!(err.to_string().contains("known events key"));
    }

    #[test]
    fn test_equivalence_streaming_vs_standard() {
        let json = br#"{"events":[{"id":1,"msg":"hello"},{"id":2,"msg":"world"}],"cursor":"abc"}"#;
        let standard: serde_json::Value = serde_json::from_slice(json).unwrap();
        let standard_events: Vec<serde_json::Value> =
            standard["events"].as_array().unwrap().clone();

        let (s, e) = find_array_range(json, Some("events")).unwrap();
        let meta = extract_metadata(json, (s, e)).unwrap();
        let streaming_events: Vec<serde_json::Value> = ArrayElementIter::new(json, (s, e))
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(standard_events, streaming_events);
        assert_eq!(meta["cursor"], "abc");
        assert!(meta["events"].as_array().unwrap().is_empty());
    }
}
