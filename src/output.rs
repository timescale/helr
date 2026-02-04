//! Event output: stdout or file with optional rotation.
//!
//! Line atomicity: each NDJSON line is written with one write + flush so a crash does not split
//! a line in the middle (best-effort; consumer should skip invalid lines).
//!
//! Backpressure: optional bounded queue (block / drop / disk_buffer) when downstream can't keep up;
//! memory threshold (RSS), queue depth, and max_queue_age_secs drive when to apply strategy.

use crate::config::{BackpressureConfig, BackpressureStrategyConfig, DropPolicyConfig};
use crate::metrics;
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

/// Sink for emitted NDJSON lines (stdout or file).
pub trait EventSink: Send + Sync {
    /// Write one NDJSON line (no trailing newline is added by the sink).
    fn write_line(&self, line: &str) -> anyhow::Result<()>;

    /// Write one line with optional source label (for backpressure drop metrics). Default calls write_line(line).
    fn write_line_from_source(&self, source: Option<&str>, line: &str) -> anyhow::Result<()> {
        let _ = source;
        self.write_line(line)
    }

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
        let mb: u64 = n
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("size must be a number (MB)"))?;
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
                inner.open_date.map(|d| d != today).unwrap_or(false)
            }
        };
        if !do_rotate {
            return Ok(());
        }
        if let Some(mut f) = inner.file.take() {
            f.flush()?;
            drop(f);
        }
        let stem = inner
            .path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("hel");
        let ext = inner
            .path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("ndjson");
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
    fn write_line_from_source(&self, source: Option<&str>, line: &str) -> anyhow::Result<()> {
        let _ = source;
        self.write_line(line)
    }

    fn write_line(&self, line: &str) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| anyhow::anyhow!("lock: {}", e))?;
        self.maybe_rotate(&mut inner)?;
        if let Some(ref mut f) = inner.file {
            let buf = format!("{}\n", line);
            f.write_all(buf.as_bytes())?;
            inner.bytes_written += buf.len() as u64;
        }
        Ok(())
    }

    fn flush(&self) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| anyhow::anyhow!("lock: {}", e))?;
        if let Some(ref mut f) = inner.file {
            f.flush()?;
        }
        Ok(())
    }
}

// --- Backpressure sink ---

/// Current process RSS in bytes (best-effort; 0 if unavailable).
fn current_process_rss_bytes() -> u64 {
    let mut sys = sysinfo::System::new_all();
    sys.refresh_all();
    let pid = sysinfo::Pid::from(std::process::id() as usize);
    sys.process(pid).map(|p| p.memory()).unwrap_or(0)
}

/// Item in queue: (source, line, enqueue time for age-based drop).
type QueueItem = (Option<String>, String, Instant);

struct BackpressureState {
    queue: VecDeque<QueueItem>,
    cap: usize,
    strategy: BackpressureStrategyConfig,
    drop_policy: DropPolicyConfig,
    pending_per_source: HashMap<String, i64>,
    memory_threshold_mb: Option<u64>,
    max_queue_age_secs: Option<u64>,
    disk_buffer_path: Option<PathBuf>,
    /// Max total disk buffer size in bytes (path + path.old). When exceeded, block until writer drains.
    disk_buffer_max_bytes: Option<u64>,
    /// When current segment file reaches this size, rotate to path.old and start new segment.
    disk_buffer_segment_bytes: Option<u64>,
    /// Total byte size of lines in queue (for stdout_buffer_size cap).
    total_queued_bytes: u64,
    /// When > 0, treat as backpressure when total_queued_bytes + line.len() > this.
    stdout_buffer_size: u64,
}

/// Shared state between producer and writer thread.
struct BackpressureInner {
    state: Mutex<BackpressureState>,
    not_empty: Condvar,
    not_full: Condvar,
    /// Signalled when queue becomes empty (for flush).
    empty_for_flush: Condvar,
    closed: AtomicBool,
    /// Lock for disk buffer file (append vs read+truncate).
    disk_buffer_mutex: Option<Arc<Mutex<()>>>,
}

/// Path for the "previous full segment" when segmenting is used (e.g. spill.ndjson -> spill.ndjson.old).
fn disk_buffer_old_path(path: &Path) -> PathBuf {
    PathBuf::from(path.to_string_lossy().to_string() + ".old")
}

fn drain_disk_buffer(path: &Path, file_lock: &Mutex<()>) -> Vec<String> {
    let _guard = file_lock.lock().unwrap();
    let old_path = disk_buffer_old_path(path);
    let mut lines = Vec::new();
    if old_path.exists() {
        if let Ok(file) = std::fs::File::open(&old_path) {
            let reader = BufReader::new(file);
            for line in reader.lines().filter_map(|r| r.ok()) {
                if !line.trim().is_empty() {
                    lines.push(line);
                }
            }
            let _ = std::fs::remove_file(&old_path);
        }
    }
    if path.exists() {
        if let Ok(file) = std::fs::File::open(path) {
            let reader = BufReader::new(file);
            for line in reader.lines().filter_map(|r| r.ok()) {
                if !line.trim().is_empty() {
                    lines.push(line);
                }
            }
        }
        if let Ok(f) = std::fs::File::create(path) {
            let _ = f.set_len(0);
        }
    }
    lines
}

/// Total size in bytes of disk buffer (path + path.old).
fn disk_buffer_total_size(path: &Path) -> u64 {
    let old_path = disk_buffer_old_path(path);
    let a = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let b = std::fs::metadata(&old_path).map(|m| m.len()).unwrap_or(0);
    a.saturating_add(b)
}

fn backpressure_writer_loop(inner: Arc<dyn EventSink>, shared: Arc<BackpressureInner>) {
    loop {
        let (_source_opt, line, pending_after_pop, drain_disk) = {
            let mut guard = shared.state.lock().unwrap();
            while guard.queue.is_empty() {
                if shared.closed.load(Ordering::SeqCst) {
                    let path = guard.disk_buffer_path.clone();
                    let file_lock = shared.disk_buffer_mutex.clone();
                    drop(guard);
                    if let (Some(p), Some(lock)) = (path, file_lock) {
                        for l in drain_disk_buffer(&p, &lock) {
                            let _ = inner.write_line(&l);
                        }
                    }
                    return;
                }
                guard = shared.not_empty.wait(guard).unwrap();
            }
            if shared.closed.load(Ordering::SeqCst) && guard.queue.is_empty() {
                return;
            }
            let (source_opt, line, _enqueue_at) = guard.queue.pop_front().unwrap();
            guard.total_queued_bytes = guard.total_queued_bytes.saturating_sub(line.len() as u64);
            let pending_after_pop = if let Some(ref s) = source_opt {
                let entry = guard.pending_per_source.get_mut(s).unwrap();
                *entry -= 1;
                Some((s.clone(), *entry))
            } else {
                None
            };
            let path_for_drain = (guard.queue.is_empty()
                && guard.disk_buffer_path.is_some()
                && shared.disk_buffer_mutex.is_some())
            .then(|| guard.disk_buffer_path.clone())
            .flatten();
            if guard.queue.is_empty() && path_for_drain.is_none() {
                shared.empty_for_flush.notify_one();
            }
            shared.not_full.notify_one();
            drop(guard);
            (source_opt, line, pending_after_pop, path_for_drain)
        };
        if let Some((s, count)) = pending_after_pop {
            metrics::set_pending_events(&s, count);
        }
        if let Err(e) = inner.write_line(&line) {
            tracing::warn!(error = %e, "backpressure writer: inner write_line failed");
        }
        if let Some(path) = drain_disk {
            if let Some(ref lock) = shared.disk_buffer_mutex {
                let lines = drain_disk_buffer(&path, lock);
                for l in &lines {
                    let _ = inner.write_line(l);
                }
                if lines.is_empty() {
                    shared.empty_for_flush.notify_one();
                }
            }
        }
    }
}

/// Bounded queue sink: when full, block / drop / disk_buffer per config. Writer thread drains to inner sink.
pub struct BackpressureSink {
    inner: Arc<dyn EventSink>,
    shared: Arc<BackpressureInner>,
    writer_handle: Option<JoinHandle<()>>,
}

impl BackpressureSink {
    /// Build a backpressure sink wrapping `inner`. Uses config detection, strategy, disk_buffer, and age/memory limits.
    pub fn new(inner: Arc<dyn EventSink>, config: &BackpressureConfig) -> anyhow::Result<Self> {
        let cap = config.detection.event_queue_size;
        if cap == 0 {
            anyhow::bail!("backpressure.detection.event_queue_size must be > 0");
        }
        let (disk_buffer_path, disk_buffer_mutex, disk_max_bytes, disk_segment_bytes) =
            match (&config.strategy, &config.disk_buffer) {
                (BackpressureStrategyConfig::DiskBuffer, Some(d)) => {
                    let path = PathBuf::from(&d.path);
                    let max_bytes = d.max_size_mb.saturating_mul(1024).saturating_mul(1024);
                    let segment_bytes = d.segment_size_mb.saturating_mul(1024).saturating_mul(1024);
                    (
                        Some(path),
                        Some(Arc::new(Mutex::new(()))),
                        Some(max_bytes),
                        Some(segment_bytes),
                    )
                }
                _ => (None, None, None, None),
            };
        let shared = Arc::new(BackpressureInner {
            state: Mutex::new(BackpressureState {
                queue: VecDeque::new(),
                cap,
                strategy: config.strategy,
                drop_policy: config.drop_policy,
                pending_per_source: HashMap::new(),
                memory_threshold_mb: config.detection.memory_threshold_mb,
                max_queue_age_secs: config.max_queue_age_secs,
                disk_buffer_path: disk_buffer_path.clone(),
                disk_buffer_max_bytes: disk_max_bytes,
                disk_buffer_segment_bytes: disk_segment_bytes,
                total_queued_bytes: 0,
                stdout_buffer_size: config.detection.stdout_buffer_size,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            empty_for_flush: Condvar::new(),
            closed: AtomicBool::new(false),
            disk_buffer_mutex,
        });
        let inner_clone = inner.clone();
        let shared_clone = shared.clone();
        let writer_handle =
            thread::spawn(move || backpressure_writer_loop(inner_clone, shared_clone));
        Ok(Self {
            inner,
            shared,
            writer_handle: Some(writer_handle),
        })
    }

    fn push_one(
        shared: &BackpressureInner,
        source: Option<&str>,
        line: &str,
    ) -> anyhow::Result<()> {
        let source_owned = source.map(String::from);
        let line_owned = line.to_string();
        let mut guard = shared.state.lock().unwrap();

        // Evict items older than max_queue_age_secs (oldest first).
        if let Some(max_age_secs) = guard.max_queue_age_secs {
            let now = Instant::now();
            while let Some(front) = guard.queue.front() {
                if now.saturating_duration_since(front.2).as_secs() >= max_age_secs {
                    let (s_opt, line, _) = guard.queue.pop_front().unwrap();
                    guard.total_queued_bytes =
                        guard.total_queued_bytes.saturating_sub(line.len() as u64);
                    if let Some(ref s) = s_opt {
                        metrics::record_event_dropped(s, "max_queue_age");
                        if let Some(entry) = guard.pending_per_source.get_mut(s) {
                            *entry -= 1;
                            metrics::set_pending_events(s, *entry);
                        }
                    }
                    shared.not_full.notify_one();
                } else {
                    break;
                }
            }
        }

        // Apply backpressure when memory over threshold (same as queue full).
        let over_memory = guard
            .memory_threshold_mb
            .map(|mb| current_process_rss_bytes() > mb.saturating_mul(1024).saturating_mul(1024))
            .unwrap_or(false);

        let over_stdout_bytes = guard.stdout_buffer_size > 0
            && guard
                .total_queued_bytes
                .saturating_add(line_owned.len() as u64)
                > guard.stdout_buffer_size;

        loop {
            if guard.queue.len() < guard.cap && !over_memory && !over_stdout_bytes {
                break;
            }
            match guard.strategy {
                BackpressureStrategyConfig::Block => {
                    guard = shared.not_full.wait(guard).unwrap();
                }
                BackpressureStrategyConfig::Drop => {
                    let dropped = match guard.drop_policy {
                        DropPolicyConfig::OldestFirst => guard
                            .queue
                            .pop_front()
                            .map(|(s, line, _)| (s, line.len() as u64)),
                        DropPolicyConfig::NewestFirst => {
                            drop(guard);
                            metrics::record_event_dropped(
                                source.unwrap_or("unknown"),
                                "backpressure",
                            );
                            return Ok(());
                        }
                        DropPolicyConfig::Random => {
                            let i = rand::random_range(0..guard.queue.len());
                            guard
                                .queue
                                .remove(i)
                                .map(|(s, line, _)| (s, line.len() as u64))
                        }
                    };
                    if let Some((ref s_opt, len)) = dropped {
                        guard.total_queued_bytes = guard.total_queued_bytes.saturating_sub(len);
                        if let Some(s) = s_opt {
                            metrics::record_event_dropped(s.as_str(), "backpressure");
                            if let Some(entry) = guard.pending_per_source.get_mut(s) {
                                *entry -= 1;
                                metrics::set_pending_events(s, *entry);
                            }
                        }
                    }
                    break;
                }
                BackpressureStrategyConfig::DiskBuffer => {
                    let path = guard.disk_buffer_path.clone();
                    let file_lock = shared.disk_buffer_mutex.clone();
                    let max_bytes = guard.disk_buffer_max_bytes;
                    let segment_bytes = guard.disk_buffer_segment_bytes;
                    if let (Some(p), Some(lock)) = (path, file_lock) {
                        drop(guard);
                        let mut file_guard = lock.lock().unwrap();
                        if let Some(max) = max_bytes {
                            loop {
                                if disk_buffer_total_size(&p) < max {
                                    break;
                                }
                                drop(file_guard);
                                std::thread::sleep(std::time::Duration::from_millis(200));
                                file_guard = lock.lock().unwrap();
                            }
                        }
                        let mut need_rotate = false;
                        if let Some(seg) = segment_bytes {
                            if std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0) >= seg {
                                need_rotate = true;
                            }
                        }
                        if need_rotate {
                            let old_path = disk_buffer_old_path(&p);
                            let _ = std::fs::rename(&p, &old_path);
                        }
                        let mut f = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&p)?;
                        writeln!(f, "{}", line_owned)?;
                        f.sync_all().ok();
                        return Ok(());
                    }
                    guard = shared.not_full.wait(guard).unwrap();
                }
            }
        }
        guard.total_queued_bytes = guard
            .total_queued_bytes
            .saturating_add(line_owned.len() as u64);
        guard
            .queue
            .push_back((source_owned.clone(), line_owned, Instant::now()));
        if let Some(ref s) = source_owned {
            let entry = guard.pending_per_source.entry(s.clone()).or_insert(0);
            *entry += 1;
            let count = *entry;
            drop(guard);
            metrics::set_pending_events(s, count);
        }
        shared.not_empty.notify_one();
        Ok(())
    }
}

impl EventSink for BackpressureSink {
    fn write_line(&self, line: &str) -> anyhow::Result<()> {
        Self::push_one(&self.shared, None, line)
    }

    fn write_line_from_source(&self, source: Option<&str>, line: &str) -> anyhow::Result<()> {
        Self::push_one(&self.shared, source, line)
    }

    fn flush(&self) -> anyhow::Result<()> {
        let mut guard = self.shared.state.lock().unwrap();
        while !guard.queue.is_empty() {
            guard = self.shared.empty_for_flush.wait(guard).unwrap();
        }
        drop(guard);
        self.inner.flush()
    }
}

impl Drop for BackpressureSink {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        self.shared.not_empty.notify_one();
        if let Some(h) = self.writer_handle.take() {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackpressureConfig, BackpressureStrategyConfig, DropPolicyConfig};
    use std::fs;
    use std::sync::Mutex as StdMutex;

    /// Sink that records all lines for tests.
    struct RecordingSink {
        lines: StdMutex<Vec<String>>,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self {
                lines: StdMutex::new(Vec::new()),
            }
        }
        fn lines(&self) -> Vec<String> {
            self.lines.lock().unwrap().clone()
        }
    }

    impl EventSink for RecordingSink {
        fn write_line(&self, line: &str) -> anyhow::Result<()> {
            self.lines.lock().unwrap().push(line.to_string());
            Ok(())
        }
    }

    fn backpressure_config(
        event_queue_size: usize,
        strategy: BackpressureStrategyConfig,
        drop_policy: DropPolicyConfig,
    ) -> BackpressureConfig {
        BackpressureConfig {
            enabled: true,
            detection: crate::config::BackpressureDetectionConfig {
                stdout_buffer_size: 65536,
                event_queue_size,
                memory_threshold_mb: None,
            },
            strategy,
            disk_buffer: None,
            drop_policy,
            max_queue_age_secs: None,
        }
    }

    #[test]
    fn backpressure_block_drains_all() {
        let inner = Arc::new(RecordingSink::new());
        let cfg = backpressure_config(
            3,
            BackpressureStrategyConfig::Block,
            DropPolicyConfig::OldestFirst,
        );
        let sink = BackpressureSink::new(inner.clone(), &cfg).unwrap();
        sink.write_line_from_source(Some("src1"), "a").unwrap();
        sink.write_line_from_source(Some("src1"), "b").unwrap();
        sink.write_line_from_source(Some("src1"), "c").unwrap();
        sink.flush().unwrap();
        let lines = inner.lines();
        assert_eq!(lines, ["a", "b", "c"]);
    }

    #[test]
    fn backpressure_drop_oldest_first() {
        let inner = Arc::new(RecordingSink::new());
        let cfg = backpressure_config(
            2,
            BackpressureStrategyConfig::Drop,
            DropPolicyConfig::OldestFirst,
        );
        let sink = BackpressureSink::new(inner.clone(), &cfg).unwrap();
        sink.write_line_from_source(Some("s"), "first").unwrap();
        sink.write_line_from_source(Some("s"), "second").unwrap();
        sink.write_line_from_source(Some("s"), "third").unwrap(); // drops "first"
        sink.flush().unwrap();
        let lines = inner.lines();
        assert_eq!(lines, ["second", "third"]);
    }

    #[test]
    fn backpressure_drop_newest_first() {
        let inner = Arc::new(RecordingSink::new());
        let cfg = backpressure_config(
            2,
            BackpressureStrategyConfig::Drop,
            DropPolicyConfig::NewestFirst,
        );
        let sink = BackpressureSink::new(inner.clone(), &cfg).unwrap();
        sink.write_line_from_source(Some("s"), "first").unwrap();
        sink.write_line_from_source(Some("s"), "second").unwrap();
        sink.write_line_from_source(Some("s"), "third").unwrap(); // drops "third"
        sink.flush().unwrap();
        let lines = inner.lines();
        assert_eq!(lines, ["first", "second"]);
    }

    #[test]
    fn backpressure_drop_random_one_dropped() {
        let inner = Arc::new(RecordingSink::new());
        let cfg = backpressure_config(
            2,
            BackpressureStrategyConfig::Drop,
            DropPolicyConfig::Random,
        );
        let sink = BackpressureSink::new(inner.clone(), &cfg).unwrap();
        // Push more than capacity so we trigger drops regardless of writer speed.
        for label in ["a", "b", "c", "d", "e"] {
            sink.write_line_from_source(Some("s"), label).unwrap();
        }
        sink.flush().unwrap();
        let lines = inner.lines();
        assert_eq!(
            lines.len(),
            2,
            "capacity 2 and 5 pushes: exactly 2 events remain after 3 drops"
        );
        let valid = ["a", "b", "c", "d", "e"];
        for line in &lines {
            assert!(
                valid.contains(&line.as_str()),
                "line {:?} should be one of {:?}",
                line,
                valid
            );
        }
    }

    #[test]
    fn backpressure_flush_waits_for_drain() {
        let inner = Arc::new(RecordingSink::new());
        let cfg = backpressure_config(
            10,
            BackpressureStrategyConfig::Block,
            DropPolicyConfig::OldestFirst,
        );
        let sink = BackpressureSink::new(inner.clone(), &cfg).unwrap();
        for i in 0..5 {
            sink.write_line_from_source(Some("s"), &format!("line{}", i))
                .unwrap();
        }
        sink.flush().unwrap();
        let lines = inner.lines();
        assert_eq!(lines.len(), 5);
        for i in 0..5 {
            assert_eq!(lines[i], format!("line{}", i));
        }
    }

    #[test]
    fn backpressure_event_queue_size_zero_fails() {
        let inner = Arc::new(RecordingSink::new());
        let mut cfg = backpressure_config(
            10,
            BackpressureStrategyConfig::Block,
            DropPolicyConfig::OldestFirst,
        );
        cfg.detection.event_queue_size = 0;
        match BackpressureSink::new(inner, &cfg) {
            Err(e) => assert!(e.to_string().contains("event_queue_size")),
            Ok(_) => panic!("expected error for event_queue_size 0"),
        }
    }

    #[test]
    fn parse_rotation_daily() {
        assert!(matches!(
            parse_rotation("daily").unwrap(),
            RotationPolicy::Daily
        ));
        assert!(matches!(
            parse_rotation("  Daily  ").unwrap(),
            RotationPolicy::Daily
        ));
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
