use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectQuery {
    pub r#type: Option<String>,
    pub provider_id: Option<String>,
    pub path_contains: Option<String>,
    pub world_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RelationQuery {
    pub relation_type: Option<String>,
    pub source_object_id: Option<String>,
    pub world_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EventQuery {
    pub event_type: Option<String>,
    pub provider_id: Option<String>,
    pub limit: Option<usize>,
    pub world_id: Option<String>,
    /// Exact correlation id match (action/trace linkage). Absent in old
    /// callers: `#[serde(default)]` keeps their params valid.
    #[serde(default)]
    pub correlation_id: Option<String>,
    /// Cursor: return only rows with `seq > after_seq`. `None`/0 means
    /// "from the beginning". Added for WS recovery (Phase E); old
    /// callers omit it and see no behavior change besides ordering.
    #[serde(default)]
    pub after_seq: Option<u64>,
}
