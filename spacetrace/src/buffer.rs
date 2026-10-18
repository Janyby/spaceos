//! Trace buffer — high-frequency, non-semantic, separate from Core Event store.

use crate::TraceEvent;
use anyhow::Result;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

/// Ring buffer for trace events with optional persistence.
#[derive(Clone)]
pub struct TraceBuffer {
    inner: Arc<Mutex<Inner>>,
    config: BufferConfig,
}

struct Inner {
    ring: VecDeque<TraceEvent>,
    dropped_count: u64,
    persisted_count: u64,
}

#[derive(Debug, Clone)]
pub struct BufferConfig {
    pub capacity: usize,
    pub persist_enabled: bool,
    pub persist_path: Option<std::path::PathBuf>,
    pub persist_batch_size: usize,
    pub persist_interval_ms: u64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            capacity: 100_000,
            persist_enabled: false,
            persist_path: None,
            persist_batch_size: 1000,
            persist_interval_ms: 1000,
        }
    }
}

impl TraceBuffer {
    /// Create a new trace buffer with default config.
    pub fn new() -> Result<Self> {
        Self::with_config(BufferConfig::default())
    }

    /// Create with custom config.
    pub fn with_config(config: BufferConfig) -> Result<Self> {
        let inner = Arc::new(Mutex::new(Inner {
            ring: VecDeque::with_capacity(config.capacity),
            dropped_count: 0,
            persisted_count: 0,
        }));

        // Start persistence task if enabled
        if config.persist_enabled {
            if let Some(path) = &config.persist_path {
                Self::start_persistence_task(inner.clone(), config.clone(), path.clone());
            }
        }

        Ok(Self { inner, config })
    }

    /// Push a trace event into the buffer.
    /// Returns true if persisted, false if dropped (buffer full).
    pub fn push(&self, event: TraceEvent) -> bool {
        let mut inner = self.inner.lock();
        if inner.ring.len() >= self.config.capacity {
            inner.dropped_count += 1;
            false
        } else {
            inner.ring.push_back(event);
            true
        }
    }

    /// Get all events and clear the buffer (for testing/drain).
    pub fn drain(&self) -> Vec<TraceEvent> {
        let mut inner = self.inner.lock();
        inner.ring.drain(..).collect()
    }

    /// Get current buffer stats.
    pub fn stats(&self) -> BufferStats {
        let inner = self.inner.lock();
        BufferStats {
            current_len: inner.ring.len(),
            capacity: self.config.capacity,
            dropped_count: inner.dropped_count,
            persisted_count: inner.persisted_count,
        }
    }

    /// Query events by correlation_id (linear scan, for testing).
    pub fn query_by_correlation(&self, correlation_id: &str) -> Vec<TraceEvent> {
        let inner = self.inner.lock();
        inner
            .ring
            .iter()
            .filter(|e| e.correlation_id == correlation_id)
            .cloned()
            .collect()
    }

    /// Query events by world_id (linear scan, for testing).
    pub fn query_by_world(&self, world_id: &str) -> Vec<TraceEvent> {
        let inner = self.inner.lock();
        inner
            .ring
            .iter()
            .filter(|e| e.world_id.as_deref() == Some(world_id))
            .cloned()
            .collect()
    }

    /// Query events by agent_id (linear scan, for testing).
    pub fn query_by_agent(&self, agent_id: &str) -> Vec<TraceEvent> {
        let inner = self.inner.lock();
        inner
            .ring
            .iter()
            .filter(|e| e.agent_id.as_deref() == Some(agent_id))
            .cloned()
            .collect()
    }

    fn start_persistence_task(
        inner: Arc<Mutex<Inner>>,
        config: BufferConfig,
        path: std::path::PathBuf,
    ) {
        use std::fs::OpenOptions;
        use std::io::Write;

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(config.persist_interval_ms));
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("Failed to open trace persist file");

            loop {
                interval.tick().await;

                let batch = {
                    let mut inner = inner.lock();
                    let take = std::cmp::min(config.persist_batch_size, inner.ring.len());
                    inner.ring.drain(..take).collect::<Vec<_>>()
                };

                if batch.is_empty() {
                    continue;
                }

                for event in &batch {
                    if let Ok(line) = serde_json::to_string(event) {
                        let _ = writeln!(file, "{}", line);
                    }
                }
                let _ = file.flush();

                {
                    let mut inner = inner.lock();
                    inner.persisted_count += batch.len() as u64;
                }
            }
        });
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BufferStats {
    pub current_len: usize,
    pub capacity: usize,
    pub dropped_count: u64,
    pub persisted_count: u64,
}

impl Default for TraceBuffer {
    fn default() -> Self {
        Self::new().expect("TraceBuffer default")
    }
}
