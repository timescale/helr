//! Session replay: record HTTP responses to disk and replay them for testing.
//!
//! Record: `--record --record-dir PATH` saves each response to record-dir/source_id/NNN.json.
//! Replay: `--replay --replay-dir PATH` loads recordings and serves them from a local server,
//! rewriting source URLs so the poll uses recorded responses instead of live APIs.

use anyhow::Context;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::debug;

/// One recorded HTTP response: URL, status, headers, body (base64).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recording {
    pub url: String,
    pub status: u16,
    pub headers: HashMap<String, String>,
    #[serde(rename = "body_base64")]
    pub body_base64: String,
}

impl Recording {
    pub fn body_bytes(&self) -> anyhow::Result<Vec<u8>> {
        BASE64
            .decode(&self.body_base64)
            .context("decode recording body_base64")
    }
}

/// State for recording: directory and per-source sequence counter.
pub struct RecordState {
    dir: std::path::PathBuf,
    counters: Mutex<HashMap<String, usize>>,
}

impl RecordState {
    pub fn new(dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir).context("create record dir")?;
        Ok(Self {
            dir: dir.to_path_buf(),
            counters: Mutex::new(HashMap::new()),
        })
    }

    /// Save one response to record_dir/source_id/NNN.json. NNN is the next sequence for that source.
    pub fn save(
        &self,
        source_id: &str,
        url: &str,
        status: u16,
        headers: &reqwest::header::HeaderMap,
        body: &[u8],
    ) -> anyhow::Result<()> {
        let mut counters = self
            .counters
            .lock()
            .map_err(|e| anyhow::anyhow!("lock: {}", e))?;
        let seq = counters.entry(source_id.to_string()).or_insert(0);
        let n = *seq;
        *seq += 1;

        let source_dir = self.dir.join(sanitize_source_id(source_id));
        std::fs::create_dir_all(&source_dir).context("create source record dir")?;
        let path = source_dir.join(format!("{:03}.json", n));

        let headers_map: HashMap<String, String> = headers
            .iter()
            .filter_map(|(k, v)| {
                let name = k.as_str().to_string();
                let value = v.to_str().ok()?.to_string();
                Some((name, value))
            })
            .collect();

        let rec = Recording {
            url: url.to_string(),
            status,
            headers: headers_map,
            body_base64: BASE64.encode(body),
        };
        let json = serde_json::to_string_pretty(&rec).context("serialize recording")?;
        std::fs::write(&path, json).context("write recording file")?;
        debug!(source = %source_id, seq = n, path = %path.display(), "recorded response");
        Ok(())
    }
}

fn sanitize_source_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Load all recordings from a directory: source_id -> list of recordings in order.
pub fn load_recordings(dir: &Path) -> anyhow::Result<HashMap<String, Vec<Recording>>> {
    let mut out: HashMap<String, Vec<Recording>> = HashMap::new();
    if !dir.exists() {
        anyhow::bail!("replay dir does not exist: {}", dir.display());
    }
    for entry in std::fs::read_dir(dir).context("read replay dir")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let source_id = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let mut recordings = Vec::new();
            let mut names: Vec<_> = std::fs::read_dir(&path)
                .context("read source replay dir")?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map_or(false, |e| e == "json"))
                .collect();
            names.sort();
            for p in names {
                let s = std::fs::read_to_string(&p).context("read recording file")?;
                let rec: Recording = serde_json::from_str(&s).context("parse recording")?;
                recordings.push(rec);
            }
            if !recordings.is_empty() {
                out.insert(source_id, recordings);
            }
        }
    }
    Ok(out)
}

/// Shared state for the replay server: responses per source and next index per source.
struct ReplayServerState {
    responses: HashMap<String, Vec<Recording>>,
    next: HashMap<String, usize>,
}

/// Start the replay HTTP server. Binds to 127.0.0.1:port (port 0 = pick one). Returns (addr, join_handle).
pub async fn start_replay_server(
    recordings: HashMap<String, Vec<Recording>>,
    port: u16,
) -> anyhow::Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>)> {
    let state = Arc::new(tokio::sync::RwLock::new(ReplayServerState {
        next: recordings.keys().map(|k| (k.clone(), 0)).collect(),
        responses: recordings,
    }));

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .context("bind replay server")?;
    let addr = listener.local_addr().context("replay server local_addr")?;

    let state_clone = state.clone();
    let join = tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/replay/{source_id}",
            axum::routing::get({
                let state = state_clone.clone();
                move |axum::extract::Path(source_id): axum::extract::Path<String>| {
                    let state = state.clone();
                    async move {
                        let mut g = state.write().await;
                        let next = g.next.get_mut(&source_id).copied().unwrap_or(0);
                        let responses = match g.responses.get(&source_id) {
                            Some(r) => r,
                            None => {
                                return axum::response::Response::builder()
                                    .status(axum::http::StatusCode::NOT_FOUND)
                                    .body(axum::body::Body::from("no recordings for source"))
                                    .unwrap();
                            }
                        };
                        if next >= responses.len() {
                            return axum::response::Response::builder()
                                .status(axum::http::StatusCode::NOT_FOUND)
                                .body(axum::body::Body::from("no more recordings for source"))
                                .unwrap();
                        }
                        let rec = responses[next].clone();
                        *g.next.get_mut(&source_id).unwrap() = next + 1;

                        let body_bytes = match rec.body_bytes() {
                            Ok(b) => b,
                            Err(_) => {
                                return axum::response::Response::builder()
                                    .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(axum::body::Body::from("invalid recording body"))
                                    .unwrap();
                            }
                        };

                        let mut response =
                            axum::response::Response::new(axum::body::Body::from(body_bytes));
                        *response.status_mut() = axum::http::StatusCode::from_u16(rec.status)
                            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                        for (k, v) in &rec.headers {
                            if let (Ok(name), Ok(value)) = (
                                axum::http::header::HeaderName::try_from(k.as_str()),
                                axum::http::header::HeaderValue::try_from(v.as_str()),
                            ) {
                                response.headers_mut().insert(name, value);
                            }
                        }
                        response
                    }
                }
            }),
        );
        let _ = axum::serve(listener, app).await;
    });

    Ok((addr, join))
}

/// Rewrite config so each source's URL points to the replay server for that source.
/// Uses sanitized source IDs in the path so they match directory names under the replay dir.
pub fn rewrite_config_for_replay(
    config: &crate::config::Config,
    base_url: &str,
) -> crate::config::Config {
    let mut config = config.clone();
    for (source_id, source) in config.sources.iter_mut() {
        let key = sanitize_source_id(source_id);
        source.url = format!("{}/replay/{}", base_url.trim_end_matches('/'), key);
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_body_bytes_roundtrip() {
        let body = b"hello world";
        let rec = Recording {
            url: "http://x/".to_string(),
            status: 200,
            headers: std::collections::HashMap::new(),
            body_base64: BASE64.encode(body),
        };
        assert_eq!(rec.body_bytes().unwrap(), body);
    }

    #[test]
    fn record_state_save_and_load() {
        let temp = std::env::temp_dir().join("hel_replay_test_save_load");
        let _ = std::fs::remove_dir_all(&temp);
        let state = RecordState::new(&temp).unwrap();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );
        state
            .save("src1", "http://api/", 200, &headers, b"[{\"id\":\"1\"}]")
            .unwrap();
        state
            .save(
                "src1",
                "http://api/page2",
                200,
                &reqwest::header::HeaderMap::new(),
                b"[]",
            )
            .unwrap();
        let loaded = load_recordings(&temp).unwrap();
        assert_eq!(loaded.len(), 1);
        let recs = loaded.get("src1").unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].url, "http://api/");
        assert_eq!(recs[0].status, 200);
        assert_eq!(recs[0].body_bytes().unwrap(), b"[{\"id\":\"1\"}]");
        assert_eq!(recs[1].body_bytes().unwrap(), b"[]");
        let _ = std::fs::remove_dir_all(&temp);
    }

    #[test]
    fn rewrite_config_for_replay_uses_sanitized_source_id() {
        let yaml = r#"
global:
  log_level: error
  state:
    backend: memory
sources:
  my-source:
    url: "https://api.example.com/logs"
    pagination:
      strategy: link_header
      rel: next
  "source with spaces":
    url: "https://other.example.com/events"
    pagination:
      strategy: link_header
      rel: next
"#;
        let config: crate::config::Config = serde_yaml::from_str(yaml).unwrap();
        let rewritten = rewrite_config_for_replay(&config, "http://127.0.0.1:9999");
        assert_eq!(
            rewritten.sources.get("my-source").unwrap().url,
            "http://127.0.0.1:9999/replay/my-source"
        );
        assert_eq!(
            rewritten.sources.get("source with spaces").unwrap().url,
            "http://127.0.0.1:9999/replay/source_with_spaces"
        );
    }
}
