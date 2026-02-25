use anyhow::Context;
use std::io::{self, BufRead, Read, Write};

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

// ==================== Phase 3: End-to-End Async Streaming ====================

/// Read wrapper that counts total bytes and aborts when `max_response_bytes` is exceeded.
pub(crate) struct ByteCountingReader<R> {
    inner: R,
    count: u64,
    limit: Option<u64>,
}

impl<R> ByteCountingReader<R> {
    pub(crate) fn new(inner: R, limit: Option<u64>) -> Self {
        Self {
            inner,
            count: 0,
            limit,
        }
    }
}

impl<R: Read> Read for ByteCountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count += n as u64;
        if let Some(limit) = self.limit
            && self.count > limit
        {
            return Err(io::Error::other(format!(
                "response body size {} exceeds max_response_bytes {}",
                self.count, limit
            )));
        }
        Ok(n)
    }
}

/// Read wrapper that tees all bytes to a file on disk for session recording.
pub(crate) struct TeeReader<R> {
    inner: R,
    tee: Option<io::BufWriter<std::fs::File>>,
}

impl<R> TeeReader<R> {
    pub(crate) fn new(inner: R, tee_path: Option<&std::path::Path>) -> io::Result<Self> {
        let tee = match tee_path {
            Some(p) => Some(io::BufWriter::new(std::fs::File::create(p)?)),
            None => None,
        };
        Ok(Self { inner, tee })
    }
}

impl<R: Read> Read for TeeReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0
            && let Some(ref mut tee) = self.tee
        {
            tee.write_all(&buf[..n])?;
        }
        Ok(n)
    }
}

/// Wrapper to make a pinned `AsyncRead` usable with `SyncIoBridge` (requires `Unpin`).
struct PinnedAsyncRead<T>(std::pin::Pin<Box<T>>);

impl<T> Unpin for PinnedAsyncRead<T> {}

impl<T: tokio::io::AsyncRead> tokio::io::AsyncRead for PinnedAsyncRead<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        self.0.as_mut().poll_read(cx, buf)
    }
}

fn read_one(reader: &mut impl Read) -> io::Result<Option<u8>> {
    let mut b = [0u8; 1];
    match reader.read_exact(&mut b) {
        Ok(()) => Ok(Some(b[0])),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

/// Forward-only JSON scanner: navigate a `Read` stream to the events array.
/// Returns pre-array metadata bytes (everything before `[`).
/// After return, the reader is positioned right after `[`.
pub(crate) fn scan_to_array(
    reader: &mut impl BufRead,
    path: Option<&str>,
) -> anyhow::Result<Vec<u8>> {
    let mut pre = Vec::new();
    match path {
        Some(p) => {
            let segments: Vec<&str> = p.split('.').collect();
            let first = scan_skip_ws_peek(reader, &mut pre)?;
            if first != b'{' {
                anyhow::bail!("expected object at root for path {:?}", p);
            }
            pre.push(first);
            for (i, segment) in segments.iter().enumerate() {
                scan_find_key(reader, &mut pre, segment)?;
                if i < segments.len() - 1 {
                    let b = scan_skip_ws_peek(reader, &mut pre)?;
                    if b != b'{' {
                        anyhow::bail!("expected object at path segment {:?}", segment);
                    }
                    pre.push(b);
                }
            }
            let arr = scan_skip_ws_peek(reader, &mut pre)?;
            if arr != b'[' {
                anyhow::bail!("value at path {:?} is not an array", p);
            }
            Ok(pre)
        }
        None => {
            let first = scan_skip_ws_no_buf(reader)?;
            match first {
                b'[' => Ok(Vec::new()),
                b'{' => {
                    pre.push(b'{');
                    scan_find_default_key_array(reader, &mut pre)?;
                    Ok(pre)
                }
                _ => anyhow::bail!("root JSON is neither array nor object"),
            }
        }
    }
}

/// Skip whitespace, buffering ws bytes. Return first non-ws byte WITHOUT buffering it.
fn scan_skip_ws_peek(reader: &mut impl Read, buf: &mut Vec<u8>) -> anyhow::Result<u8> {
    loop {
        let b = read_one(reader)?.ok_or_else(|| anyhow::anyhow!("unexpected EOF in JSON"))?;
        if b.is_ascii_whitespace() {
            buf.push(b);
        } else {
            return Ok(b);
        }
    }
}

/// Skip whitespace without buffering. Return first non-ws byte (also not buffered).
fn scan_skip_ws_no_buf(reader: &mut impl Read) -> anyhow::Result<u8> {
    loop {
        let b = read_one(reader)?.ok_or_else(|| anyhow::anyhow!("unexpected EOF in JSON"))?;
        if !b.is_ascii_whitespace() {
            return Ok(b);
        }
    }
}

/// Read a JSON string after the opening `"` has been consumed.
/// Buffers all content bytes including the closing `"`. Returns the string content.
fn scan_read_string(reader: &mut impl Read, buf: &mut Vec<u8>) -> anyhow::Result<String> {
    let mut s = String::new();
    loop {
        let b = read_one(reader)?.ok_or_else(|| anyhow::anyhow!("unterminated string"))?;
        buf.push(b);
        if b == b'\\' {
            let esc =
                read_one(reader)?.ok_or_else(|| anyhow::anyhow!("unterminated string escape"))?;
            buf.push(esc);
            s.push(esc as char);
        } else if b == b'"' {
            return Ok(s);
        } else {
            s.push(b as char);
        }
    }
}

/// Skip one JSON value. `first` byte already consumed and buffered by caller.
fn scan_skip_value(reader: &mut impl BufRead, buf: &mut Vec<u8>, first: u8) -> anyhow::Result<()> {
    match first {
        b'"' => {
            scan_read_string(reader, buf)?;
            Ok(())
        }
        b'{' | b'[' => {
            let close = if first == b'{' { b'}' } else { b']' };
            let mut depth = 1u32;
            let mut in_string = false;
            loop {
                let b = read_one(reader)?
                    .ok_or_else(|| anyhow::anyhow!("unterminated {} in JSON", first as char))?;
                buf.push(b);
                if in_string {
                    if b == b'\\' {
                        let esc = read_one(reader)?
                            .ok_or_else(|| anyhow::anyhow!("unterminated string escape"))?;
                        buf.push(esc);
                    } else if b == b'"' {
                        in_string = false;
                    }
                } else {
                    match b {
                        b'"' => in_string = true,
                        _ if b == first => depth += 1,
                        _ if b == close => {
                            depth -= 1;
                            if depth == 0 {
                                return Ok(());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        b't' => scan_read_literal(reader, buf, b"rue"),
        b'f' => scan_read_literal(reader, buf, b"alse"),
        b'n' => scan_read_literal(reader, buf, b"ull"),
        b'-' | b'0'..=b'9' => scan_skip_number(reader, buf),
        _ => anyhow::bail!("unexpected byte '{}' in JSON value", first as char),
    }
}

fn scan_read_literal(
    reader: &mut impl Read,
    buf: &mut Vec<u8>,
    expected: &[u8],
) -> anyhow::Result<()> {
    for &exp in expected {
        let b = read_one(reader)?.ok_or_else(|| anyhow::anyhow!("unexpected EOF in literal"))?;
        buf.push(b);
        if b != exp {
            anyhow::bail!("expected '{}', got '{}'", exp as char, b as char);
        }
    }
    Ok(())
}

/// Skip a JSON number. First digit/minus already consumed and buffered by caller.
/// Uses `BufRead` peek to avoid consuming the delimiter byte after the number.
fn scan_skip_number(reader: &mut impl BufRead, buf: &mut Vec<u8>) -> anyhow::Result<()> {
    loop {
        let (n, buf_len) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                return Ok(());
            }
            let count = available
                .iter()
                .take_while(|&&b| {
                    b.is_ascii_digit()
                        || b == b'.'
                        || b == b'e'
                        || b == b'E'
                        || b == b'+'
                        || b == b'-'
                })
                .count();
            if count > 0 {
                buf.extend_from_slice(&available[..count]);
            }
            (count, available.len())
        };
        if n > 0 {
            reader.consume(n);
        }
        if n == 0 || n < buf_len {
            return Ok(());
        }
    }
}

/// Find a key in the current JSON object (reader after `{`).
/// Buffers all bytes. After return, reader is at the value (after `:`).
fn scan_find_key(reader: &mut impl BufRead, buf: &mut Vec<u8>, target: &str) -> anyhow::Result<()> {
    loop {
        let b = scan_skip_ws_peek(reader, buf)?;
        match b {
            b'}' => {
                buf.push(b);
                anyhow::bail!("key {:?} not found in object", target);
            }
            b',' => {
                buf.push(b);
                continue;
            }
            b'"' => {
                buf.push(b);
                let key = scan_read_string(reader, buf)?;
                let colon = scan_skip_ws_peek(reader, buf)?;
                if colon != b':' {
                    anyhow::bail!("expected ':' after key, got '{}'", colon as char);
                }
                buf.push(colon);
                if key == target {
                    return Ok(());
                }
                let val_byte = scan_skip_ws_peek(reader, buf)?;
                buf.push(val_byte);
                scan_skip_value(reader, buf, val_byte)?;
            }
            _ => anyhow::bail!("expected '\"' or '}}' in object, got '{}'", b as char),
        }
    }
}

/// Scan for the first well-known key whose value is an array (default key fallback).
/// Reader positioned after `{` (which is already in buf).
fn scan_find_default_key_array(reader: &mut impl BufRead, buf: &mut Vec<u8>) -> anyhow::Result<()> {
    const DEFAULT_KEYS: &[&str] = &["items", "data", "events", "logs", "entries"];
    loop {
        let b = scan_skip_ws_peek(reader, buf)?;
        match b {
            b'}' => {
                buf.push(b);
                anyhow::bail!(
                    "no top-level array or known events key (items/data/events/logs/entries)"
                );
            }
            b',' => {
                buf.push(b);
                continue;
            }
            b'"' => {
                buf.push(b);
                let key = scan_read_string(reader, buf)?;
                let colon = scan_skip_ws_peek(reader, buf)?;
                if colon != b':' {
                    anyhow::bail!("expected ':' after key");
                }
                buf.push(colon);
                let is_default = DEFAULT_KEYS.contains(&key.as_str());
                let val_byte = scan_skip_ws_peek(reader, buf)?;
                if is_default && val_byte == b'[' {
                    return Ok(());
                }
                buf.push(val_byte);
                scan_skip_value(reader, buf, val_byte)?;
            }
            _ => anyhow::bail!("expected '\"' or '}}' in object, got '{}'", b as char),
        }
    }
}

/// Skip whitespace and commas in a `BufRead`, peeking at the next meaningful byte
/// without consuming it.
fn bufread_skip_ws_commas_peek(reader: &mut impl BufRead) -> io::Result<Option<u8>> {
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(None);
        }
        let skip = available
            .iter()
            .take_while(|&&b| b.is_ascii_whitespace() || b == b',')
            .count();
        if skip > 0 {
            let had_more = skip < available.len();
            reader.consume(skip);
            if had_more {
                let available = reader.fill_buf()?;
                return Ok(available.first().copied());
            }
            continue;
        }
        return Ok(Some(available[0]));
    }
}

/// Parse JSON array elements one at a time from a `BufRead` stream.
/// Reader must be positioned right after the opening `[`.
/// Sends each parsed `Value` through `event_tx` and metadata through `meta_tx`.
pub(crate) fn parse_elements_from_reader(
    reader: &mut impl BufRead,
    event_tx: &tokio::sync::mpsc::Sender<anyhow::Result<serde_json::Value>>,
    pre_array_bytes: Vec<u8>,
    meta_tx: tokio::sync::oneshot::Sender<anyhow::Result<serde_json::Value>>,
) -> anyhow::Result<()> {
    use serde::Deserialize;

    loop {
        let next = bufread_skip_ws_commas_peek(reader)?;
        match next {
            None => break,
            Some(b']') => {
                reader.consume(1);
                break;
            }
            Some(_) => {
                let mut de = serde_json::Deserializer::from_reader(reader.by_ref());
                let value = serde_json::Value::deserialize(&mut de)
                    .context("parse array element from stream")?;
                if event_tx.blocking_send(Ok(value)).is_err() {
                    return Ok(());
                }
            }
        }
    }

    let mut post_array = Vec::new();
    reader.read_to_end(&mut post_array)?;

    let metadata = if pre_array_bytes.is_empty() {
        serde_json::Value::Null
    } else {
        let mut synthetic = Vec::with_capacity(pre_array_bytes.len() + 2 + post_array.len());
        synthetic.extend_from_slice(&pre_array_bytes);
        synthetic.extend_from_slice(b"[]");
        synthetic.extend_from_slice(&post_array);
        serde_json::from_slice(&synthetic).context("parse metadata from stream")?
    };

    let _ = meta_tx.send(Ok(metadata));
    Ok(())
}

/// Phase 3 async entry point: stream response bytes directly to a blocking JSON parser.
/// Returns `(event_receiver, metadata_receiver, join_handle)`.
/// The caller MUST await the `JoinHandle` after consuming events to detect panics.
pub(crate) async fn stream_and_parse(
    response: reqwest::Response,
    events_path: Option<String>,
    max_response_bytes: Option<u64>,
    tee_path: Option<std::path::PathBuf>,
) -> anyhow::Result<(
    tokio::sync::mpsc::Receiver<anyhow::Result<serde_json::Value>>,
    tokio::sync::oneshot::Receiver<anyhow::Result<serde_json::Value>>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
)> {
    use futures_util::StreamExt;
    use tokio_util::io::{StreamReader, SyncIoBridge};

    let byte_stream = response
        .bytes_stream()
        .map(|result| result.map_err(io::Error::other));
    let stream_reader = StreamReader::new(byte_stream);
    let pinned = PinnedAsyncRead(Box::pin(stream_reader));

    let (event_tx, event_rx) = tokio::sync::mpsc::channel(128);
    let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

    let join_handle = tokio::task::spawn_blocking(move || {
        let sync_read = SyncIoBridge::new(pinned);
        let tee_read = TeeReader::new(sync_read, tee_path.as_deref())
            .map_err(|e| anyhow::anyhow!("create tee file: {}", e))?;
        let counting_read = ByteCountingReader::new(tee_read, max_response_bytes);
        let mut buffered = io::BufReader::new(counting_read);

        let path_ref = events_path.as_deref();
        let pre_array = scan_to_array(&mut buffered, path_ref)?;
        parse_elements_from_reader(&mut buffered, &event_tx, pre_array, meta_tx)?;

        Ok(())
    });

    Ok((event_rx, meta_rx, join_handle))
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

    // ==================== Phase 3 unit tests ====================

    #[test]
    fn test_byte_counting_reader_within_limit() {
        let data = b"hello world";
        let mut reader = ByteCountingReader::new(io::Cursor::new(data), Some(100));
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    fn test_byte_counting_reader_exceeds_limit() {
        let data = b"hello world";
        let mut reader = ByteCountingReader::new(io::Cursor::new(data), Some(5));
        let mut buf = Vec::new();
        let err = reader.read_to_end(&mut buf).unwrap_err();
        assert!(err.to_string().contains("exceeds max_response_bytes"));
    }

    #[test]
    fn test_byte_counting_reader_no_limit() {
        let data = b"hello world";
        let mut reader = ByteCountingReader::new(io::Cursor::new(data), None);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    fn test_tee_reader_writes_to_file() {
        let data = b"hello tee world";
        let tmp = std::env::temp_dir().join("helr_test_tee_reader.bin");
        let mut reader = TeeReader::new(io::Cursor::new(data), Some(tmp.as_path())).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        drop(reader);
        assert_eq!(buf, data);
        let tee_contents = std::fs::read(&tmp).unwrap();
        assert_eq!(tee_contents, data);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn test_tee_reader_passthrough() {
        let data = b"no tee";
        let mut reader = TeeReader::new(io::Cursor::new(data), None).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    fn test_scan_to_array_top_level() {
        let json = br#"[{"a":1},{"a":2}]"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let pre = scan_to_array(&mut reader, None).unwrap();
        assert!(pre.is_empty());
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, br#"{"a":1},{"a":2}]"#);
    }

    #[test]
    fn test_scan_to_array_top_level_with_whitespace() {
        let json = b"  \n  [1,2,3]  ";
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let pre = scan_to_array(&mut reader, None).unwrap();
        assert!(pre.is_empty());
    }

    #[test]
    fn test_scan_to_array_dotted_path_single() {
        let json = br#"{"data":[1,2,3],"meta":"ok"}"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let pre = scan_to_array(&mut reader, Some("data")).unwrap();
        assert_eq!(pre, br#"{"data":"#);
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, br#"1,2,3],"meta":"ok"}"#);
    }

    #[test]
    fn test_scan_to_array_dotted_path_multi() {
        let json = br#"{"outer":{"inner":{"records":[10,20]},"x":1}}"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let pre = scan_to_array(&mut reader, Some("outer.inner.records")).unwrap();
        assert_eq!(pre, br#"{"outer":{"inner":{"records":"#);
    }

    #[test]
    fn test_scan_to_array_default_keys() {
        let json = br#"{"cursor":"abc","events":[{"id":1}]}"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let pre = scan_to_array(&mut reader, None).unwrap();
        assert_eq!(pre, br#"{"cursor":"abc","events":"#);
    }

    #[test]
    fn test_scan_to_array_default_keys_skips_non_array() {
        let json = br#"{"data":"string","items":[1]}"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let pre = scan_to_array(&mut reader, None).unwrap();
        assert_eq!(pre, br#"{"data":"string","items":"#);
    }

    #[test]
    fn test_scan_to_array_key_not_found() {
        let json = br#"{"a":1}"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let err = scan_to_array(&mut reader, Some("missing")).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_scan_to_array_value_not_array() {
        let json = br#"{"data":"hello"}"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let err = scan_to_array(&mut reader, Some("data")).unwrap_err();
        assert!(err.to_string().contains("not an array"));
    }

    #[test]
    fn test_scan_to_array_with_number_value() {
        let json = br#"{"count":42,"events":[1,2]}"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let pre = scan_to_array(&mut reader, Some("events")).unwrap();
        assert_eq!(pre, br#"{"count":42,"events":"#);
    }

    #[test]
    fn test_scan_to_array_with_nested_objects() {
        let json = br#"{"meta":{"page":1},"items":[{"id":1}]}"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let pre = scan_to_array(&mut reader, None).unwrap();
        assert_eq!(pre, br#"{"meta":{"page":1},"items":"#);
    }

    #[test]
    fn test_scan_to_array_with_boolean_null_values() {
        let json = br#"{"active":true,"deleted":false,"extra":null,"events":[1]}"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let pre = scan_to_array(&mut reader, Some("events")).unwrap();
        assert_eq!(
            pre,
            br#"{"active":true,"deleted":false,"extra":null,"events":"#
        );
    }

    #[test]
    fn test_scan_to_array_escaped_strings() {
        let json = br#"{"key":"val\"ue","arr":[1]}"#;
        let mut reader = io::BufReader::new(io::Cursor::new(json.as_slice()));
        let pre = scan_to_array(&mut reader, Some("arr")).unwrap();
        assert_eq!(pre, br#"{"key":"val\"ue","arr":"#);
    }

    #[tokio::test]
    async fn test_parse_elements_basic() {
        let after_bracket = br#"{"id":1},{"id":2}]"#.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(after_bracket));
            parse_elements_from_reader(&mut reader, &event_tx, Vec::new(), meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            events.push(result.unwrap());
        }
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["id"], 1);
        assert_eq!(events[1]["id"], 2);

        let metadata = meta_rx.await.unwrap().unwrap();
        assert!(metadata.is_null());
    }

    #[tokio::test]
    async fn test_parse_elements_empty_array() {
        let after_bracket = b"]".to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(after_bracket));
            parse_elements_from_reader(&mut reader, &event_tx, Vec::new(), meta_tx).unwrap();
        })
        .await
        .unwrap();

        assert!(event_rx.recv().await.is_none());
        let metadata = meta_rx.await.unwrap().unwrap();
        assert!(metadata.is_null());
    }

    #[tokio::test]
    async fn test_parse_elements_with_metadata() {
        let pre = br#"{"cursor":"abc","events":"#.to_vec();
        let after_bracket = br#"{"id":1},{"id":2}],"total":2}"#.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        let pre_clone = pre.clone();
        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(after_bracket));
            parse_elements_from_reader(&mut reader, &event_tx, pre_clone, meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            events.push(result.unwrap());
        }
        assert_eq!(events.len(), 2);

        let metadata = meta_rx.await.unwrap().unwrap();
        assert_eq!(metadata["cursor"], "abc");
        assert_eq!(metadata["total"], 2);
        assert!(metadata["events"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_scan_and_parse_end_to_end() {
        let json =
            br#"{"cursor":"next","events":[{"id":1,"msg":"a"},{"id":2,"msg":"b"}],"total":2}"#
                .to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(json));
            let pre = scan_to_array(&mut reader, Some("events")).unwrap();
            parse_elements_from_reader(&mut reader, &event_tx, pre, meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            events.push(result.unwrap());
        }
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["msg"], "a");
        assert_eq!(events[1]["msg"], "b");

        let metadata = meta_rx.await.unwrap().unwrap();
        assert_eq!(metadata["cursor"], "next");
        assert_eq!(metadata["total"], 2);
    }

    #[tokio::test]
    async fn test_parse_elements_objects_no_byte_loss() {
        let after_bracket = br#" {"a":1} , {"b":2} , {"c":3} ]"#.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, _meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(after_bracket));
            parse_elements_from_reader(&mut reader, &event_tx, Vec::new(), meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            events.push(result.unwrap());
        }
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["a"], 1);
        assert_eq!(events[1]["b"], 2);
        assert_eq!(events[2]["c"], 3);
    }

    #[tokio::test]
    async fn test_phase3_equivalence_with_phase2() {
        let json = br#"{"events":[{"id":1,"msg":"hello"},{"id":2,"msg":"world"}],"cursor":"abc"}"#;

        let (s, e) = find_array_range(json, Some("events")).unwrap();
        let phase2_meta = extract_metadata(json, (s, e)).unwrap();
        let phase2_events: Vec<serde_json::Value> = ArrayElementIter::new(json, (s, e))
            .map(|r| r.unwrap())
            .collect();

        let json_vec = json.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(json_vec));
            let pre = scan_to_array(&mut reader, Some("events")).unwrap();
            parse_elements_from_reader(&mut reader, &event_tx, pre, meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut phase3_events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            phase3_events.push(result.unwrap());
        }
        let phase3_meta = meta_rx.await.unwrap().unwrap();

        assert_eq!(phase2_events, phase3_events);
        assert_eq!(phase2_meta["cursor"], phase3_meta["cursor"]);
    }

    #[tokio::test]
    async fn test_phase3_metadata_after_events_array() {
        let json = br#"{"events":[{"id":1},{"id":2}],"cursor":"page2","total":42}"#.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(json));
            let pre = scan_to_array(&mut reader, Some("events")).unwrap();
            parse_elements_from_reader(&mut reader, &event_tx, pre, meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            events.push(result.unwrap());
        }
        assert_eq!(events.len(), 2);

        let metadata = meta_rx.await.unwrap().unwrap();
        assert_eq!(metadata["cursor"], "page2");
        assert_eq!(metadata["total"], 42);
        assert!(metadata["events"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_phase3_top_level_array_no_metadata() {
        let json = br#"[{"x":1},{"x":2},{"x":3}]"#.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(json));
            let pre = scan_to_array(&mut reader, None).unwrap();
            parse_elements_from_reader(&mut reader, &event_tx, pre, meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            events.push(result.unwrap());
        }
        assert_eq!(events.len(), 3);
        let metadata = meta_rx.await.unwrap().unwrap();
        assert!(metadata.is_null());
    }

    #[tokio::test]
    async fn test_phase3_default_key_with_metadata() {
        let json = br#"{"count":3,"items":[{"a":1},{"a":2},{"a":3}],"next":"http://x"}"#.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(json));
            let pre = scan_to_array(&mut reader, None).unwrap();
            parse_elements_from_reader(&mut reader, &event_tx, pre, meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            events.push(result.unwrap());
        }
        assert_eq!(events.len(), 3);

        let metadata = meta_rx.await.unwrap().unwrap();
        assert_eq!(metadata["count"], 3);
        assert_eq!(metadata["next"], "http://x");
    }

    #[tokio::test]
    async fn test_phase3_dotted_path_deeply_nested() {
        let json = br#"{"result":{"data":{"records":[{"v":10},{"v":20}]},"ok":true}}"#.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(json));
            let pre = scan_to_array(&mut reader, Some("result.data.records")).unwrap();
            parse_elements_from_reader(&mut reader, &event_tx, pre, meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            events.push(result.unwrap());
        }
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["v"], 10);
        assert_eq!(events[1]["v"], 20);

        let metadata = meta_rx.await.unwrap().unwrap();
        assert_eq!(metadata["result"]["ok"], true);
    }

    #[test]
    fn test_byte_counting_reader_exact_limit() {
        let data = b"12345";
        let mut reader = ByteCountingReader::new(io::Cursor::new(data), Some(5));
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    fn test_tee_reader_large_data() {
        let data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let tmp = std::env::temp_dir().join("helr_test_tee_large.bin");
        let mut reader =
            TeeReader::new(io::Cursor::new(data.clone()), Some(tmp.as_path())).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        drop(reader);
        assert_eq!(buf, data);
        let tee_contents = std::fs::read(&tmp).unwrap();
        assert_eq!(tee_contents, data);
        let _ = std::fs::remove_file(&tmp);
    }

    #[tokio::test]
    async fn test_phase3_max_response_bytes_enforcement() {
        let json = br#"[{"id":1},{"id":2},{"id":3},{"id":4},{"id":5}]"#.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, _meta_rx) = tokio::sync::oneshot::channel();

        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let counting = ByteCountingReader::new(io::Cursor::new(json), Some(10));
            let mut reader = io::BufReader::new(counting);
            let pre = scan_to_array(&mut reader, None)?;
            parse_elements_from_reader(&mut reader, &event_tx, pre, meta_tx)?;
            Ok(())
        })
        .await
        .unwrap();

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("exceeds max_response_bytes"));

        let mut count = 0;
        while event_rx.recv().await.is_some() {
            count += 1;
        }
        assert!(count <= 5);
    }

    #[tokio::test]
    async fn test_phase3_equivalence_default_keys() {
        let json = br#"{"total":2,"data":[{"name":"alice"},{"name":"bob"}],"page":1}"#;

        let (s, e) = find_array_range(json, None).unwrap();
        let phase2_meta = extract_metadata(json, (s, e)).unwrap();
        let phase2_events: Vec<serde_json::Value> = ArrayElementIter::new(json, (s, e))
            .map(|r| r.unwrap())
            .collect();

        let json_vec = json.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(json_vec));
            let pre = scan_to_array(&mut reader, None).unwrap();
            parse_elements_from_reader(&mut reader, &event_tx, pre, meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut phase3_events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            phase3_events.push(result.unwrap());
        }
        let phase3_meta = meta_rx.await.unwrap().unwrap();

        assert_eq!(phase2_events, phase3_events);
        assert_eq!(phase2_meta["total"], phase3_meta["total"]);
        assert_eq!(phase2_meta["page"], phase3_meta["page"]);
    }

    #[tokio::test]
    async fn test_phase3_equivalence_top_level_array() {
        let json = br#"[{"id":1},{"id":2},{"id":3}]"#;

        let (s, e) = find_array_range(json, None).unwrap();
        let phase2_events: Vec<serde_json::Value> = ArrayElementIter::new(json, (s, e))
            .map(|r| r.unwrap())
            .collect();

        let json_vec = json.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, _meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(json_vec));
            let pre = scan_to_array(&mut reader, None).unwrap();
            parse_elements_from_reader(&mut reader, &event_tx, pre, meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut phase3_events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            phase3_events.push(result.unwrap());
        }

        assert_eq!(phase2_events, phase3_events);
    }

    #[tokio::test]
    async fn test_phase3_equivalence_dotted_path() {
        let json = br#"{"result":{"items":[{"k":"v1"},{"k":"v2"}]},"cursor":"c"}"#;

        let (s, e) = find_array_range(json, Some("result.items")).unwrap();
        let phase2_meta = extract_metadata(json, (s, e)).unwrap();
        let phase2_events: Vec<serde_json::Value> = ArrayElementIter::new(json, (s, e))
            .map(|r| r.unwrap())
            .collect();

        let json_vec = json.to_vec();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut reader = io::BufReader::new(io::Cursor::new(json_vec));
            let pre = scan_to_array(&mut reader, Some("result.items")).unwrap();
            parse_elements_from_reader(&mut reader, &event_tx, pre, meta_tx).unwrap();
        })
        .await
        .unwrap();

        let mut phase3_events = Vec::new();
        while let Some(result) = event_rx.recv().await {
            phase3_events.push(result.unwrap());
        }
        let phase3_meta = meta_rx.await.unwrap().unwrap();

        assert_eq!(phase2_events, phase3_events);
        assert_eq!(phase2_meta["cursor"], phase3_meta["cursor"]);
    }

    #[test]
    fn test_tee_reader_combined_with_byte_counting() {
        let data = b"hello world 12345";
        let tmp = std::env::temp_dir().join("helr_test_tee_counting.bin");
        let tee = TeeReader::new(io::Cursor::new(data.as_slice()), Some(tmp.as_path())).unwrap();
        let mut counting = ByteCountingReader::new(tee, Some(100));
        let mut buf = Vec::new();
        counting.read_to_end(&mut buf).unwrap();
        drop(counting);
        assert_eq!(buf, data);
        let tee_contents = std::fs::read(&tmp).unwrap();
        assert_eq!(tee_contents, data);
        let _ = std::fs::remove_file(&tmp);
    }
}
