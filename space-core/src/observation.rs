//! Provider observations: facts reported to Core, applied by Core.
//!
//! Rule (Phase B): **only Core mutates `objects`/`relations`**, and only
//! inside [`crate::Storage::apply_observation`], which persists the event
//! plus its projection writes in ONE SQLite transaction. Providers never
//! write domain state directly; they report facts and Core validates +
//! applies them. The legacy per-row IPC methods (`create_object`,
//! `update_object`, `create_relation`, `record_event`) keep working
//! unchanged for backward compatibility.
//!
//! ### `world_id` vs `space_id`
//!
//! There is exactly one scope identifier and it is called **`world_id`**:
//! it is the id of the [`crate::Space`] the data belongs to (empty string
//! = unscoped, same as the legacy default). No rename was performed so
//! every existing contract keeps parsing. `space_id` MUST NOT appear as a
//! second parallel field anywhere in this API.
//!
//! ### Field semantics of [`Observation`]
//!
//! - `observation_id`: REQUIRED stable caller-side key (e.g.
//!   `obs-<uuid>` or a deterministic provider key). Redelivery is
//!   recognized by this key, not by the event id.
//! - `world_id`: scope for the whole observation (see above).
//! - `facts`: raw provider-asserted observations (CDP snapshots, notify
//!   kinds, fingerprints). They are NOT a separate table: when the event
//!   payload has no `facts` key, Core stores them as
//!   `payload["facts"]` for audit. Normalized state lives in
//!   `objects`/`relations`, never in `facts`.
//! - `objects`: full [`crate::Object`] records, upserted by id IN ORDER
//!   before relations (so FK targets exist within the transaction).
//! - `relations`: full [`crate::Relation`] records, upserted by id.
//!   Endpoints must exist (as rows or earlier in `objects`), otherwise
//!   the whole transaction rolls back — nothing is partially applied.
//! - `delete_ids`: object ids to hard-delete (Core cascade) AFTER
//!   upserts, still inside the same transaction. Makes confirmed
//!   deletions atomic with their `FileDeleted` event. Additive field:
//!   absent in old payloads = no deletions.
//! - `event`: the domain event to record. Convention (not enforced):
//!   single-event observations reuse `observation_id` as `event.id`.
//!
//! ### Redelivery
//!
//! The first application writes one row into `observation_log`. A repeat
//! with the same `observation_id` returns `{applied: false}` WITHOUT
//! touching objects/relations/events again. A NEW `observation_id` whose
//! `event.id` collides with a stored event is an error (caller bug —
//! fail loudly instead of silently dropping the event while applying
//! the projections).

use serde::{Deserialize, Serialize};

use crate::{Event, Object, Relation};

/// One provider-reported observation batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub observation_id: String,
    #[serde(default)]
    pub world_id: String,
    #[serde(default)]
    pub facts: Vec<serde_json::Value>,
    #[serde(default)]
    pub objects: Vec<Object>,
    #[serde(default)]
    pub relations: Vec<Relation>,
    #[serde(default)]
    pub delete_ids: Vec<String>,
    pub event: Event,
}

/// Outcome of [`crate::Storage::apply_observation`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationOutcome {
    /// False only for recognized redelivery (nothing was written).
    pub applied: bool,
    pub event_id: String,
    /// Durable cursor of the event (`0` when not applied).
    pub seq: u64,
}
