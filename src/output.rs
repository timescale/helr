//! Event output: stdout or file with optional rotation.
//!
//! Line atomicity: each NDJSON line is written with one write + flush so a crash does not split
//! a line in the middle (best-effort; consumer should skip invalid lines).

use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

/// Sink for emitted NDJSON lines (stdout or file).
pub trait EventSink: Send + Sync {
    /// Write one NDJSON line (no trailing newline is added by the sink).
    fn write_line(&self, line: &str) -> anyhow::Result<()>;
    /// Flush buffered output (no-op for stdout).
    fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Emit to stdout. Handles BrokenPipe (e.g. consumer exited) by returning error so caller can exit non-zero.
pub struct StdoutSink;

impl EventSink for StdoutSink {
    fn write_line(&self, line: &str) -> anyhow::Result<()> {
        let mut out = std::io::stdout().lock();
        out.write_all(line.as_bytes())
            .and_then(|()| out.write_all(b"\n"))
            .and_then(|()| out.flush())
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::BrokenPipe {
                    anyhow::anyhow!("broken pipe (SIGPIPE): consumer exited")
                } else {
                    e.into()
                }
            })
    }
}

/// Rotation policy for file output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RotationPolicy {
    /// No rotation; write to a single file.
    None,
    /// Rotate when file size exceeds this many bytes.
    SizeBytes(u64),
    /// Rotate daily; rotated file gets suffix YYYY-MM-DD.
    Daily,
}

/// Parse rotation from CLI: "daily" or "size:N" (N in MB).
pub fn parse_rotation(s: &str) -> anyhow::Result<RotationPolicy> {
    let s = s.trim().to_lowercase();
    if s == "daily" {
        return Ok(RotationPolicy::Daily);
    }
    if let Some(n) = s.strip_prefix("size:") {
        let mb: u64 = n.trim().parse().map_err(|_| anyhow::anyhow!("size must be a number (MB)"))?;
        if mb == 0 {
            anyhow::bail!("size must be > 0");
        }
        return Ok(RotationPolicy::SizeBytes(mb * 1024 * 1024));
    }
    anyhow::bail!("rotation must be 'daily' or 'size:N' (N in MB)");
}

/// Emit to a file with optional rotation.
pub struct FileSink {
    inner: Mutex<FileSinkInner>,
}

struct FileSinkInner {
    path: std::path::PathBuf,
    file: Option<std::fs::File>,
    bytes_written: u64,
    rotation: RotationPolicy,
    open_date: Option<chrono::NaiveDate>,
}

impl FileSink {
    /// Open or create file at `path`. Rotation applies when writing.
    pub fn new(path: &Path, rotation: RotationPolicy) -> anyhow::Result<Self> {
        let path = path.to_path_buf();
        let (file, open_date) = Self::open_file(&path, &rotation)?;
        let bytes_written = file.metadata()?.len();
        Ok(Self {
            inner: Mutex::new(FileSinkInner {
                path,
                file: Some(file),
                bytes_written,
                rotation,
                open_date,
            }),
        })
    }

    fn open_file(
        path: &std::path::Path,
        rotation: &RotationPolicy,
    ) -> anyhow::Result<(std::fs::File, Option<chrono::NaiveDate>)> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let _ = file.metadata()?;
        let open_date = match rotation {
            RotationPolicy::Daily => Some(chrono::Utc::now().date_naive()),
            _ => None,
        };
        Ok((file, open_date))
    }

    fn maybe_rotate(&self, inner: &mut FileSinkInner) -> anyhow::Result<()> {
        let do_rotate = match &inner.rotation {
            RotationPolicy::None => false,
            RotationPolicy::SizeBytes(max) => inner.bytes_written >= *max,
            RotationPolicy::Daily => {
                let today = chrono::Utc::now().date_naive();
                inner
                    .open_date
                    .map(|d| d != today)
                    .unwrap_or(false)
            }
        };
        if !do_rotate {
            return Ok(());
        }
        if let Some(mut f) = inner.file.take() {
            f.flush()?;
            drop(f);
        }
        let stem = inner.path.file_stem().and_then(|s| s.to_str()).unwrap_or("hel");
        let ext = inner.path.extension().and_then(|s| s.to_str()).unwrap_or("ndjson");
        let parent = inner.path.parent().unwrap_or(std::path::Path::new("."));
        let suffix = match &inner.rotation {
            RotationPolicy::Daily => chrono::Utc::now().format("%Y-%m-%d").to_string(),
            _ => chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S").to_string(),
        };
        let rotated = parent.join(format!("{}.{}.{}", stem, suffix, ext));
        if inner.path.exists() {
            std::fs::rename(&inner.path, &rotated)?;
        }
        let (file, open_date) = Self::open_file(&inner.path, &inner.rotation)?;
        inner.file = Some(file);
        inner.bytes_written = 0;
        inner.open_date = open_date;
        Ok(())
    }
}

impl EventSink for FileSink {
    fn write_line(&self, line: &str) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().map_err(|e| anyhow::anyhow!("lock: {}", e))?;
        self.maybe_rotate(&mut inner)?;
        if let Some(ref mut f) = inner.file {
            let buf = format!("{}\n", line);
            f.write_all(buf.as_bytes())?;
            inner.bytes_written += buf.len() as u64;
        }
        Ok(())
    }

    fn flush(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().map_err(|e| anyhow::anyhow!("lock: {}", e))?;
        if let Some(ref mut f) = inner.file {
            f.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn parse_rotation_daily() {
        assert!(matches!(parse_rotation("daily").unwrap(), RotationPolicy::Daily));
        assert!(matches!(parse_rotation("  Daily  ").unwrap(), RotationPolicy::Daily));
    }

    #[test]
    fn parse_rotation_size() {
        let p = parse_rotation("size:1").unwrap();
        assert_eq!(p, RotationPolicy::SizeBytes(1024 * 1024));
        let p = parse_rotation("size:100").unwrap();
        assert_eq!(p, RotationPolicy::SizeBytes(100 * 1024 * 1024));
    }

    #[test]
    fn parse_rotation_invalid() {
        assert!(parse_rotation("").is_err());
        assert!(parse_rotation("size:0").is_err());
        assert!(parse_rotation("size:abc").is_err());
        assert!(parse_rotation("hourly").is_err());
    }

    #[test]
    fn file_sink_append_and_rotate_by_size() {
        let dir = std::env::temp_dir().join("hel_output_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("out.ndjson");
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(dir.join("out.2020-01-01T00-00-00.ndjson")); // any previous rotated

        // Rotate when >= 20 bytes
        let sink = FileSink::new(&path, RotationPolicy::SizeBytes(20)).unwrap();
        sink.write_line("short").unwrap(); // "short\n" = 6 bytes
        sink.write_line("another").unwrap(); // "another\n" = 8 bytes -> 14 total
        sink.write_line("third_line_here").unwrap(); // 16 bytes -> 30 total (>= 20)
        sink.flush().unwrap();
        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("short"));
        assert!(content.contains("another"));
        assert!(content.contains("third_line_here"));

        // Next write_line sees bytes_written >= 20 and rotates
        sink.write_line("after_rotate").unwrap();
        sink.flush().unwrap();
        // Original path is new file with only the line written after rotation
        let after = fs::read_to_string(&path).unwrap();
        assert_eq!(after.trim(), "after_rotate");
        // Rotated file should exist with previous content
        let rotated = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path() != path && e.path().extension().map_or(false, |x| x == "ndjson"));
        assert!(rotated.is_some(), "rotated file should exist");
        let rotated_path = rotated.unwrap().path();
        let rotated_content = fs::read_to_string(&rotated_path).unwrap();
        assert!(rotated_content.contains("short"));
        assert!(rotated_content.contains("another"));
        assert!(rotated_content.contains("third_line_here"));

        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&rotated_path);
    }
}
