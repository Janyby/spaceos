use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Semantic subscription: owned by an authenticated subscriber.
/// Filtering is applied server-side BEFORE delivery to the subscriber pipe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: String,
    pub subscriber: String,
    pub world_id: Option<String>,
    #[serde(default)]
    pub event_types: Vec<String>,
    #[serde(default)]
    pub object_ids: Vec<String>,
}

impl Subscription {
    pub fn new(
        subscriber: impl Into<String>,
        world_id: Option<String>,
        event_types: Vec<String>,
        object_ids: Vec<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            subscriber: subscriber.into(),
            world_id,
            event_types,
            object_ids,
        }
    }

    /// Server-side predicate: must hold before an event is written to the
    /// subscriber's pipe. No post-delivery filtering counts as isolation.
    pub fn matches(
        &self,
        event_type: &str,
        event_world_id: &str,
        event_object_ids: &[String],
    ) -> bool {
        if let Some(w) = &self.world_id {
            if event_world_id != w {
                return false;
            }
        }
        if !self.event_types.is_empty() && !self.event_types.iter().any(|t| t == event_type) {
            return false;
        }
        if !self.object_ids.is_empty()
            && !event_object_ids
                .iter()
                .any(|id| self.object_ids.contains(id))
        {
            return false;
        }
        true
    }
}

/// Guard for subscription-aware agents: ignore own events, react at most
/// once per external correlation chain, stay bounded.
pub struct LoopGuard {
    self_id: String,
    seen: HashSet<String>,
    max_tracked: usize,
}

impl LoopGuard {
    pub fn new(self_id: impl Into<String>) -> Self {
        Self {
            self_id: self_id.into(),
            seen: HashSet::new(),
            max_tracked: 1024,
        }
    }

    /// Returns true only if the agent should react: external actor AND
    /// this correlation was not already handled.
    pub fn should_react(&mut self, actor: &str, correlation_id: Option<&str>) -> bool {
        if actor == self.self_id {
            return false;
        }
        if let Some(c) = correlation_id {
            if !self.seen.insert(c.to_string()) {
                return false;
            }
            while self.seen.len() > self.max_tracked {
                if let Some(first) = self.seen.iter().next().cloned() {
                    self.seen.remove(&first);
                } else {
                    break;
                }
            }
        }
        true
    }

    pub fn seen_count(&self) -> usize {
        self.seen.len()
    }
}

/// Test-only reactive trace. NOT part of the durable World Event model.
/// T1 = filesystem mtime (reality), T2 = provider observed_at,
/// T3 = core event.timestamp (commit), T4 = subscriber receipt.
#[derive(Debug, Clone, Default)]
pub struct ReactiveTrace {
    pub correlation_id: String,
    pub t1: Option<std::time::SystemTime>,
    pub t2: Option<std::time::SystemTime>,
    pub t3: Option<std::time::SystemTime>,
    pub t4: Option<std::time::SystemTime>,
}

impl ReactiveTrace {
    pub fn deltas_ms(&self) -> Option<(i64, i64, i64, i64)> {
        let ms = |t: Option<std::time::SystemTime>| -> Option<i64> {
            t?.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|d| d.as_millis() as i64)
        };
        let (t1, t2, t3, t4) = (ms(self.t1)?, ms(self.t2)?, ms(self.t3)?, ms(self.t4)?);
        Some((t2 - t1, t3 - t2, t4 - t3, t4 - t1))
    }
}

pub fn percentile(sorted_ms: &mut [i64], p: f64) -> i64 {
    if sorted_ms.is_empty() {
        return 0;
    }
    sorted_ms.sort_unstable();
    let idx = ((p / 100.0) * (sorted_ms.len() as f64 - 1.0)).round() as usize;
    sorted_ms[idx.min(sorted_ms.len() - 1)]
}
