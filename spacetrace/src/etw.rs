//! Windows ETW evidence collector (Stage 3).
//!
//! Minimal real-time consumer of `Microsoft-Windows-Kernel-Process`,
//! process lifecycle only: Start/End (opcodes 1/2; DC rundown excluded).
//! No file-operation coverage — declared gap, not silent.
//!
//! Design notes (see docs/SPACETRACE_ETW_CORRELATION.md §4–§5):
//! - Semantic tracing stays independent: this module never touches Core,
//!   Storage, hooks, or the trace buffer. It yields `EtwProcessRecord`s
//!   convertible to `OsEvidence`; publication stays the caller's job.
//! - Exactly ONE extra thread: `ProcessTrace` is a blocking pump with no
//!   async alternative. No broker, queue, or worker pool.
//! - At most ONE active collector per process (enforced): callbacks are
//!   bare `extern "system"` fns, so routing uses a single guarded slot
//!   whose lifetime is tied to the pump thread join.
//! - Stop affects only our own pid-suffixed session. A live name collision
//!   is reported, never stolen.
//! - Kernel timestamps: `EVENT_HEADER.TimeStamp`, 100ns ticks since
//!   1601-01-01 UTC (system time; RAW_TIMESTAMP mode is NOT set). Do NOT
//!   conflate with QPC, `Instant`, or chrono clocks without conversion.
//! - `UniqueProcessKey` is a boot-scoped instance key, not a wall time:
//!   it defeats PID reuse without pretending to be a timestamp.

use crate::verdict::OsEvidence;
use parking_lot::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{SyncSender, sync_channel},
};
use windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED;
use windows_sys::Win32::System::Diagnostics::Etw::*;
use windows_sys::core::GUID;

// ---- Stable identifiers (documented Windows constants) ----

/// Microsoft-Windows-Kernel-Process provider.
pub const KERNEL_PROCESS_PROVIDER_GUID: GUID = GUID {
    data1: 0x22FB2CD6,
    data2: 0x0E7B,
    data3: 0x422B,
    data4: [0xA0, 0xC7, 0x2F, 0xAD, 0x1F, 0xD0, 0xE7, 0x16],
};

/// `WINEVENT_KEYWORD_PROCESS` — subscribe to process/thread lifecycle only.
pub const KEYWORD_PROCESS: u64 = 0x10;
/// Enable level 4 = Information (inclusive: kernel process Start/End).
pub const ENABLE_LEVEL: u8 = 4;
/// Opcodes accepted as (EventId, Opcode). DC-start/end (3, 4) are session
/// rundown, not real starts — excluded to avoid false starts.
pub const OPCODE_START: u8 = 1;
pub const OPCODE_END: u8 = 2;
const EVENT_ID_START: u16 = 1;
const EVENT_ID_END: u16 = 2;

/// 100ns ticks between 1601-01-01 and 1970-01-01 UTC.
pub const FILETIME_UNIX_EPOCH_DIFF: i64 = 116444736000000000;

/// Kernel `TimeStamp` (100ns, 1601 epoch) -> unix microseconds.
/// `None` only on arithmetic overflow; pre-1970 inputs legitimately yield
/// negative values (1601 → negative), never panics.
pub fn kernel_ts_to_unix_us(ts_100ns: i64) -> Option<i64> {
    ts_100ns
        .checked_sub(FILETIME_UNIX_EPOCH_DIFF)
        .map(|v| v / 10)
}

fn guid_eq(a: &GUID, b: &GUID) -> bool {
    a.data1 == b.data1 && a.data2 == b.data2 && a.data3 == b.data3 && a.data4 == b.data4
}

fn wide_null(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

// ---- Records ----

/// One decoded kernel process lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtwProcessRecord {
    pub pid: u32,
    /// Parent pid; `None` = schema carries none (verified: End events lack
    /// it) — informational only, never identity.
    pub ppid: Option<u32>,
    /// Boot-unique instance key (`ProcessSequenceNumber` on modern schemas).
    /// Opaque: never a wall time, never compared across boots.
    pub unique_process_key: u64,
    /// Process exit status; `None` = schema carries none (verified: the
    /// 16-prop Start/End schema has no exit-code field) — never invented.
    pub exit_status: Option<i32>,
    pub kind: ProcessLifecycle,
    /// Raw kernel timestamp (100ns, 1601 epoch) — original time system.
    pub kernel_time_100ns: i64,
    /// Process creation time as FILETIME (100ns, 1601 epoch), from the
    /// `CreateTime` property; `None` when the schema omits it.
    pub creation_filetime: Option<u64>,
    pub event_id: u16,
    pub opcode: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessLifecycle {
    Start,
    End,
}

impl ProcessLifecycle {
    pub fn op_name(&self) -> &'static str {
        match self {
            ProcessLifecycle::Start => "ProcessStart",
            ProcessLifecycle::End => "ProcessEnd",
        }
    }
}

impl EtwProcessRecord {
    /// Convert to Stage-1 evidence. `CreateTime` (when captured) becomes a
    /// real wall-clock `process_start_time` (unix microseconds, decimal);
    /// the boot-unique sequence travels in `instance_key`. Neither is
    /// invented: absent fields stay absent.
    pub fn to_os_evidence(&self) -> OsEvidence {
        OsEvidence {
            source: "etw".into(),
            pid: self.pid,
            process_start_time: self.creation_filetime.and_then(|ft| {
                ft.checked_sub(FILETIME_UNIX_EPOCH_DIFF as u64)
                    .map(|v| (v / 10).to_string())
            }),
            instance_key: Some(format!("{:016x}", self.unique_process_key)),
            file_key: None,
            path: None,
            op: self.kind.op_name().into(),
            time_us: kernel_ts_to_unix_us(self.kernel_time_100ns),
            backend_ref: format!(
                "etw:{:016x}:{}",
                self.unique_process_key,
                self.kind.op_name()
            ),
        }
    }
}

// ---- State & quality ----

/// Collector lifecycle. `Unavailable` (environment) and `Failed`
/// (unexpected) are distinct by design.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectorState {
    Starting,
    Running,
    Stopped { reason: String },
    Unavailable { reason: String },
    Failed { stage: &'static str, code: u32 },
}

/// Quality snapshot. `etw_lost_events: None` means "no buffers observed
/// yet — counter unavailable", distinct from `Some(0)` = "observed, none
/// lost". Never invent zeros.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QualitySnapshot {
    pub state: String,
    pub received: u64,
    pub etw_lost_events: Option<u64>,
    pub buffers_seen: u64,
    pub decode_errors: u64,
    pub ignored_events: u64,
    pub channel_drops: u64,
    pub window_start_us: Option<i64>,
    pub window_end_us: Option<i64>,
    pub pump_exit_code: Option<u32>,
}

#[derive(Debug, Default)]
struct QualityInner {
    received: u64,
    etw_lost_events: Option<u64>,
    buffers_seen: u64,
    decode_errors: u64,
    ignored_events: u64,
    channel_drops: u64,
    window_start_us: Option<i64>,
    window_end_us: Option<i64>,
    pump_exit_code: Option<u32>,
}

struct Shared {
    tx: SyncSender<EtwProcessRecord>,
    quality: Mutex<QualityInner>,
    state: Mutex<CollectorState>,
    stop_requested: AtomicBool,
    /// Resolved MOF field names per event id (see `resolve_schema`).
    schema_cache: Mutex<std::collections::HashMap<u16, ResolvedNames>>,
}

fn active_shared() -> Option<&'static Shared> {
    // Copy the pointer out under the lock; callbacks only read.
    let p: Option<*const Shared> = (*ACTIVE.lock()).map(|s| s.0);
    let p = p?;
    if p.is_null() {
        return None;
    }
    // SAFETY: the pointer is stored only while an Arc<Shared> is owned by
    // both the Collector and the pump thread, and cleared before either is
    // dropped; callbacks run solely inside ProcessTrace within that window.
    Some(unsafe { &*p })
}

// Single active collector per process (callbacks cannot capture state).
// The slot holds a raw pointer; validity is tied to the pump-thread join
// (see `active_shared`). The wrapper makes the sharing explicit.
#[derive(Clone, Copy)]
struct SlotPtr(*const Shared);
// SAFETY: dereferenced only inside the pump window while both Arcs live.
unsafe impl Send for SlotPtr {}
unsafe impl Sync for SlotPtr {}
static ACTIVE: Mutex<Option<SlotPtr>> = Mutex::new(None);
static ACTIVE_FLAG: AtomicBool = AtomicBool::new(false);

/// Pure mapping of session-setup failures: environmental causes become
/// `Unavailable`, everything else `Failed`. Unit-tested.
pub fn map_setup_error(stage: &'static str, code: u32) -> CollectorState {
    match code {
        ERROR_ACCESS_DENIED => CollectorState::Unavailable {
            reason: format!("{stage}: access denied (code 5) — elevated rights required"),
        },
        // ERROR_ALREADY_EXISTS = 183: a live session owns our name — steal nothing.
        183 => CollectorState::Unavailable {
            reason: format!("{stage}: session name already live (code 183) — not stealing"),
        },
        // ERROR_WMI_GUID_NOT_FOUND = 4201: provider unknown on this build.
        4201 => CollectorState::Unavailable {
            reason: format!("{stage}: provider guid unknown on this build (code 4201)"),
        },
        _ => CollectorState::Failed { stage, code },
    }
}

// ---- TDH decode (thin, error-counted) ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodeError {
    Tdh(u32),
    BadSize(&'static str),
    SchemaUnresolved,
}

/// Strict read with an already-resolved name (no probing).
fn read_prop_strict(
    record: *const EVENT_RECORD,
    name: &str,
    expect: usize,
) -> Result<Vec<u8>, DecodeError> {
    match try_prop_bytes(record, name, expect) {
        Ok(Some(b)) => Ok(b),
        // Resolved names must hit: a miss here means the schema shifted
        // mid-session — counted, never silently re-probed per event.
        Ok(None) => Err(DecodeError::SchemaUnresolved),
        Err(e) => Err(e),
    }
}

fn read_u32_prop(record: *const EVENT_RECORD, name: &str) -> Result<u32, DecodeError> {
    read_prop_strict(record, name, 4).map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_u64_prop(record: *const EVENT_RECORD, name: &str) -> Result<u64, DecodeError> {
    read_prop_strict(record, name, 8)
        .map(|b| u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
}

/// Candidate MOF property names per field, most-likely first. Verified live
/// on Win11 26200 for event id=1: ProcessID / ProcessSequenceNumber /
/// CreateTime(FILETIME) / ParentProcessID. Resolution probes these against
/// the live event and caches the winner per event id; a miss stays honest.
const CAND_KEY: &[&str] = &["ProcessSequenceNumber"];
const CAND_PID: &[&str] = &["ProcessID"];
const CAND_PPID: &[&str] = &["ParentProcessID"];

/// Try one name: `None` = not found (1168, try next), `Err` = real failure.
fn try_prop_bytes(
    record: *const EVENT_RECORD,
    name: &str,
    expect: usize,
) -> Result<Option<Vec<u8>>, DecodeError> {
    unsafe {
        let wname = wide_null(name);
        let desc = PROPERTY_DATA_DESCRIPTOR {
            PropertyName: wname.as_ptr() as u64,
            ArrayIndex: 0,
            Reserved: 0,
        };
        let mut size: u32 = 0;
        let rc = TdhGetPropertySize(record, 0, std::ptr::null(), 1, &desc, &mut size);
        if rc == 1168 {
            return Ok(None); // ERROR_NOT_FOUND: name absent in this schema
        }
        if rc != 0 {
            return Err(DecodeError::Tdh(rc));
        }
        if size as usize != expect {
            return Err(DecodeError::BadSize(name_static(name)));
        }
        let mut buf = vec![0u8; size as usize];
        let rc = TdhGetProperty(
            record,
            0,
            std::ptr::null(),
            1,
            &desc,
            size,
            buf.as_mut_ptr(),
        );
        if rc != 0 {
            return Err(DecodeError::Tdh(rc));
        }
        Ok(Some(buf))
    }
}

/// Dump top-level TDH property names for one event (schema discovery).
/// Public diagnostic helper: answers "what does this build's schema look
/// like" for future backend work (e.g. file operations) without guessing.
/// Returns printable lines; never fails the caller.
/// SAFETY: `record` must be a valid live EVENT_RECORD.
pub unsafe fn dump_schema_names(record: *const EVENT_RECORD) -> Vec<String> {
    let mut out = vec![];
    let mut size: u32 = 0;
    // First call with null buffer must report required size (122).
    // Explicit block: edition 2024 requires it even inside `unsafe fn`.
    let rc = unsafe {
        TdhGetEventInformation(record, 0, std::ptr::null(), std::ptr::null_mut(), &mut size)
    };
    if rc != 122 {
        out.push(format!("schema-info-size-rc={rc}"));
        return out;
    }
    let mut buf = vec![0u8; size as usize];
    let info = buf.as_mut_ptr() as *mut TRACE_EVENT_INFO;
    let rc = unsafe { TdhGetEventInformation(record, 0, std::ptr::null(), info, &mut size) };
    if rc != 0 {
        out.push(format!("schema-info-rc={rc}"));
        return out;
    }
    // SAFETY: `info` points to a TDH-filled buffer of `size` bytes; all
    // reads below stay inside it (indices bounded by counts, names capped).
    // Explicit block: edition 2024 requires it even inside `unsafe fn`.
    unsafe {
        let top = (*info).TopLevelPropertyCount;
        let total = (*info).PropertyCount;
        out.push(format!("schema props top={top} total={total}"));
        let base = info as *const u8;
        let arr = std::ptr::addr_of!((*info).EventPropertyInfoArray) as *const u8;
        let stride = std::mem::size_of::<EVENT_PROPERTY_INFO>();
        for i in 0..top.min(64) {
            let p = arr.add(i as usize * stride) as *const EVENT_PROPERTY_INFO;
            let name_off = (*p).NameOffset;
            let flags = (*p).Flags;
            let intype = (*p).Anonymous1.nonStructType.InType;
            // Read the null-terminated UTF-16 name with a sanity cap.
            let mut name_u16 = vec![];
            let mut cur = base.add(name_off as usize) as *const u16;
            for _ in 0..128 {
                let c = *cur;
                if c == 0 {
                    break;
                }
                name_u16.push(c);
                cur = cur.add(1);
            }
            let name = String::from_utf16_lossy(&name_u16);
            out.push(format!("{name}:{intype}:{flags}"));
        }
    }
    // One crumb line for the whole schema (keeps the cap meaningful).
    if out.len() > 1 {
        let props = out[1..].join(",");
        out.truncate(1);
        out.push(format!("schema all [{props}]"));
    }
    out
}
/// Resolved field names for one event class. `ppid_name` is `None` when the
/// class carries no parent field (verified for End events) — ppid is
/// informational, so resolution still succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedNames {
    key: String,
    pid: String,
    ppid: Option<String>,
}
fn resolve_schema(
    shared: &Shared,
    record: *const EVENT_RECORD,
    event_id: u16,
) -> Option<ResolvedNames> {
    {
        let cache = shared.schema_cache.lock();
        if let Some(names) = cache.get(&event_id) {
            return Some(names.clone());
        }
    }
    // Required identity fields: key + pid. A miss here fails resolution.
    let mut key_hit: Option<String> = None;
    let mut pid_hit: Option<String> = None;
    for (cands, expect, label) in [(CAND_KEY, 8, "key"), (CAND_PID, 4, "pid")] {
        let mut hit: Option<String> = None;
        for name in cands {
            match try_prop_bytes(record, name, expect) {
                Ok(Some(_)) => {
                    hit = Some(name.to_string());
                    break;
                }
                Ok(None) => continue,
                Err(e) => {
                    let _ = e;
                    return None;
                }
            }
        }
        match (label, hit) {
            ("key", Some(h)) => key_hit = Some(h),
            ("pid", Some(h)) => pid_hit = Some(h),
            (_, None) => {
                return None;
            }
            _ => return None,
        }
    }
    // Parent pid is informational: probe, but a total miss yields None
    // instead of failing (verified: End events carry no parent field).
    let mut ppid_hit: Option<String> = None;
    for name in CAND_PPID {
        match try_prop_bytes(record, name, 4) {
            Ok(Some(_)) => {
                ppid_hit = Some(name.to_string());
                break;
            }
            Ok(None) => continue,
            Err(_) => {
                return None;
            }
        }
    }
    let out = ResolvedNames {
        key: key_hit.unwrap(),
        pid: pid_hit.unwrap(),
        ppid: ppid_hit,
    };
    shared.schema_cache.lock().insert(event_id, out.clone());
    Some(out)
}

fn name_static(name: &str) -> &'static str {
    match name {
        "ProcessSequenceNumber" => "ProcessSequenceNumber",
        "ProcessID" => "ProcessID",
        "ParentProcessID" => "ParentProcessID",
        "CreateTime" => "CreateTime",
        _ => "unknown-property",
    }
}

/// Normalize already-parsed fields into a record. Pure: unit-tested with
/// synthetic inputs (no ETW session needed).
#[allow(clippy::too_many_arguments)]
pub fn normalize_process_event(
    pid: u32,
    ppid: Option<u32>,
    unique_key: u64,
    exit_status: Option<i32>,
    kind: ProcessLifecycle,
    kernel_time_100ns: i64,
    creation_filetime: Option<u64>,
    event_id: u16,
    opcode: u8,
) -> EtwProcessRecord {
    EtwProcessRecord {
        pid,
        ppid,
        unique_process_key: unique_key,
        exit_status,
        kind,
        kernel_time_100ns,
        creation_filetime,
        event_id,
        opcode,
    }
}

/// Decode one EVENT_RECORD into a process record, or count why not.
/// Returns `None` for non-process-lifecycle events (caller counts `ignored`).
unsafe fn decode_record(
    shared: &Shared,
    record: *mut EVENT_RECORD,
) -> Result<Option<EtwProcessRecord>, DecodeError> {
    // SAFETY: caller (the ETW callback) guarantees a valid record pointer.
    let h = unsafe { &(*record).EventHeader };
    if !guid_eq(&h.ProviderId, &KERNEL_PROCESS_PROVIDER_GUID) {
        return Ok(None);
    }
    let kind = match (h.EventDescriptor.Id, h.EventDescriptor.Opcode) {
        (EVENT_ID_START, OPCODE_START) => ProcessLifecycle::Start,
        (EVENT_ID_END, OPCODE_END) => ProcessLifecycle::End,
        _ => return Ok(None), // incl. DC-start/end rundown (3,4): not real starts
    };
    let names = match resolve_schema(shared, record, h.EventDescriptor.Id) {
        Some(n) => n,
        None => return Err(DecodeError::SchemaUnresolved),
    };
    let key = read_u64_prop(record, &names.key)?;
    let pid = read_u32_prop(record, &names.pid)?;
    let ppid = match &names.ppid {
        Some(n) => Some(read_u32_prop(record, n)?),
        None => None,
    };
    // CreateTime (FILETIME) shares the resolved schema walk: same names
    // array would need a 4th slot; read it directly (verified intype 17).
    let creation = read_u64_prop(record, "CreateTime").ok();
    Ok(Some(normalize_process_event(
        pid,
        ppid,
        key,
        None, // no exit-code field in this schema family (verified dump)
        kind,
        h.TimeStamp,
        creation,
        h.EventDescriptor.Id,
        h.EventDescriptor.Opcode,
    )))
}

unsafe extern "system" fn event_callback(record: *mut EVENT_RECORD) {
    let Some(shared) = active_shared() else {
        return;
    };
    // Callbacks must never unwind across the OS boundary.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: `record` comes from the ETW callback with a valid pointer.
        match unsafe { decode_record(shared, record) } {
            Ok(Some(ev)) => {
                {
                    let mut q = shared.quality.lock();
                    q.received += 1;
                    if let Some(t) = kernel_ts_to_unix_us(ev.kernel_time_100ns) {
                        q.window_start_us = Some(q.window_start_us.map_or(t, |lo| lo.min(t)));
                        q.window_end_us = Some(q.window_end_us.map_or(t, |hi| hi.max(t)));
                    }
                }
                if shared.tx.try_send(ev).is_err() {
                    shared.quality.lock().channel_drops += 1;
                }
            }
            Ok(None) => {
                shared.quality.lock().ignored_events += 1;
            }
            Err(_) => {
                shared.quality.lock().decode_errors += 1;
            }
        }
    }));
    if outcome.is_err() {
        if let Some(shared) = active_shared() {
            shared.quality.lock().decode_errors += 1;
        }
    }
}

unsafe extern "system" fn buffer_callback(logfile: *mut EVENT_TRACE_LOGFILEW) -> u32 {
    if let Some(shared) = active_shared() {
        let lost = unsafe { (*logfile).EventsLost } as u64;
        let mut q = shared.quality.lock();
        q.buffers_seen += 1;
        q.etw_lost_events = Some(match q.etw_lost_events {
            Some(prev) => prev.max(lost),
            None => lost,
        });
    }
    1 // TRUE: continue processing
}

// ---- Collector ----

/// Real-time ETW process-lifecycle collector. `start()` never fails with
/// `Err`: environmental problems land in `CollectorState::Unavailable`,
/// unexpected ones in `Failed` — always queryable via `state()`.
pub struct Collector {
    shared: Arc<Shared>,
    rx: std::sync::mpsc::Receiver<EtwProcessRecord>,
    pump: Option<std::thread::JoinHandle<()>>,
    done_rx: Option<std::sync::mpsc::Receiver<()>>,
    session_name: Vec<u16>,
    session_handle: u64,
    /// ONE allocation: EVENT_TRACE_PROPERTIES + trailing logger name.
    /// (A bare Box<struct> would leave LoggerNameOffset dangling past the
    /// heap block while Wnode.BufferSize claims the bigger size — heap
    /// overflow on StartTraceW. u64 backing guarantees 8-byte alignment.)
    props_buf: Vec<u64>,
}

/// Byte size of the fixed struct part (logger name appended past it).
fn props_struct_size() -> usize {
    std::mem::size_of::<EVENT_TRACE_PROPERTIES>()
}

impl Collector {
    /// Pointer to the struct at the base of our buffer.
    /// SAFETY: buffer is u64-backed (8-aligned) and sized >= struct.
    fn props_ptr(&mut self) -> *mut EVENT_TRACE_PROPERTIES {
        self.props_buf.as_mut_ptr() as *mut EVENT_TRACE_PROPERTIES
    }

    pub fn start() -> Self {
        let (tx, rx) = sync_channel::<EtwProcessRecord>(4096);
        let shared = Arc::new(Shared {
            tx,
            quality: Mutex::new(QualityInner::default()),
            state: Mutex::new(CollectorState::Starting),
            stop_requested: AtomicBool::new(false),
            schema_cache: Mutex::new(std::collections::HashMap::new()),
        });
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        if ACTIVE_FLAG.swap(true, Ordering::SeqCst) {
            return Self::with_state(
                shared,
                rx,
                CollectorState::Unavailable {
                    reason: "another collector is active in this process".into(),
                },
            );
        }

        let session_name = wide_null(&format!("spaceos-etw-{}", std::process::id()));
        // ONE buffer: struct + trailing logger name. LoggerNameOffset points
        // inside this same allocation (see struct docs).
        let struct_size = props_struct_size();
        let name_bytes = session_name.len() * 2;
        let total = struct_size + name_bytes;
        let mut props_buf: Vec<u64> = vec![0; total.div_ceil(8)];
        // Copy the name into the trailing region (u16 accesses stay aligned:
        // struct_size is even).
        {
            let base = props_buf.as_mut_ptr() as *mut u8;
            let name_src = session_name.as_ptr() as *const u8;
            unsafe { std::ptr::copy_nonoverlapping(name_src, base.add(struct_size), name_bytes) };
        }
        let props = props_buf.as_mut_ptr() as *mut EVENT_TRACE_PROPERTIES;
        unsafe {
            (*props).Wnode.BufferSize = total as u32;
            (*props).Wnode.Flags = WNODE_FLAG_TRACED_GUID;
            (*props).Wnode.ClientContext = 1; // QPC clock resolution
            (*props).BufferSize = 64; // KB per buffer
            (*props).MinimumBuffers = 2;
            (*props).MaximumBuffers = 8;
            (*props).FlushTimer = 1; // seconds
            (*props).LogFileMode = EVENT_TRACE_REAL_TIME_MODE;
            (*props).LoggerNameOffset = struct_size as u32;
        }

        // SAFETY: StartTraceW reads name + props synchronously during the call.
        let mut handle = CONTROLTRACE_HANDLE { Value: 0 };
        let rc = unsafe { StartTraceW(&mut handle, session_name.as_ptr(), props) };
        let session_handle = handle.Value;
        if rc != 0 {
            ACTIVE_FLAG.store(false, Ordering::SeqCst);
            return Self::with_state(shared, rx, map_setup_error("start-trace", rc));
        }

        let mut params = ENABLE_TRACE_PARAMETERS::default();
        params.Version = ENABLE_TRACE_PARAMETERS_VERSION_2;
        let rc = unsafe {
            EnableTraceEx2(
                CONTROLTRACE_HANDLE {
                    Value: session_handle,
                },
                &KERNEL_PROCESS_PROVIDER_GUID,
                EVENT_CONTROL_CODE_ENABLE_PROVIDER,
                ENABLE_LEVEL,
                KEYWORD_PROCESS,
                0,
                0,
                &params,
            )
        };
        if rc != 0 {
            let state = map_setup_error("enable-provider", rc);
            // Release our own session: never leak a running empty session.
            unsafe {
                ControlTraceW(
                    CONTROLTRACE_HANDLE {
                        Value: session_handle,
                    },
                    session_name.as_ptr(),
                    props,
                    EVENT_TRACE_CONTROL_STOP,
                );
            }
            ACTIVE_FLAG.store(false, Ordering::SeqCst);
            return Self::with_state(shared, rx, state);
        }

        let mut logfile: EVENT_TRACE_LOGFILEW =
            unsafe { std::mem::zeroed::<EVENT_TRACE_LOGFILEW>() };
        logfile.LoggerName = session_name.as_ptr() as *mut u16;
        logfile.Anonymous1.ProcessTraceMode =
            PROCESS_TRACE_MODE_REAL_TIME | PROCESS_TRACE_MODE_EVENT_RECORD;
        logfile.Anonymous2.EventRecordCallback = Some(event_callback);
        logfile.BufferCallback = Some(buffer_callback);
        // SAFETY: OpenTraceW reads the struct synchronously.
        let trace_handle = unsafe { OpenTraceW(&mut logfile as *mut EVENT_TRACE_LOGFILEW) };
        if trace_handle.Value == u64::MAX {
            unsafe {
                ControlTraceW(
                    CONTROLTRACE_HANDLE {
                        Value: session_handle,
                    },
                    session_name.as_ptr(),
                    props,
                    EVENT_TRACE_CONTROL_STOP,
                );
            }
            ACTIVE_FLAG.store(false, Ordering::SeqCst);
            *shared.state.lock() = CollectorState::Failed {
                stage: "open-trace",
                code: 0,
            };
            return Self::assemble(shared, rx, None, None, vec![], 0, props_buf);
        }

        // Publish the slot before the pump can invoke callbacks.
        *ACTIVE.lock() = Some(SlotPtr(Arc::as_ptr(&shared)));
        let pump_shared = Arc::clone(&shared);
        let pump = std::thread::Builder::new()
            .name("spaceos-etw-pump".into())
            .spawn(move || {
                *pump_shared.state.lock() = CollectorState::Running;
                // SAFETY: blocking pump; callbacks route via ACTIVE slot.
                let code =
                    unsafe { ProcessTrace(&trace_handle, 1, std::ptr::null(), std::ptr::null()) };
                {
                    let mut st = pump_shared.state.lock();
                    pump_shared.quality.lock().pump_exit_code = Some(code);
                    if !pump_shared.stop_requested.load(Ordering::SeqCst)
                        && matches!(*st, CollectorState::Running | CollectorState::Starting)
                    {
                        *st = CollectorState::Failed {
                            stage: "pump-exit",
                            code,
                        };
                    }
                }
                unsafe { CloseTrace(trace_handle) };
                *ACTIVE.lock() = None;
                ACTIVE_FLAG.store(false, Ordering::SeqCst);
                let _ = done_tx.send(());
            });
        let pump = match pump {
            Ok(h) => Some(h),
            Err(_) => {
                *ACTIVE.lock() = None;
                ACTIVE_FLAG.store(false, Ordering::SeqCst);
                unsafe {
                    ControlTraceW(
                        CONTROLTRACE_HANDLE {
                            Value: session_handle,
                        },
                        session_name.as_ptr(),
                        props,
                        EVENT_TRACE_CONTROL_STOP,
                    );
                    CloseTrace(trace_handle);
                }
                *shared.state.lock() = CollectorState::Failed {
                    stage: "spawn-pump",
                    code: 0,
                };
                return Self::assemble(shared, rx, None, None, vec![], 0, props_buf);
            }
        };

        Self::assemble(
            shared,
            rx,
            pump,
            Some(done_rx),
            session_name,
            session_handle,
            props_buf,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(
        shared: Arc<Shared>,
        rx: std::sync::mpsc::Receiver<EtwProcessRecord>,
        pump: Option<std::thread::JoinHandle<()>>,
        done_rx: Option<std::sync::mpsc::Receiver<()>>,
        session_name: Vec<u16>,
        session_handle: u64,
        props_buf: Vec<u64>,
    ) -> Self {
        Self {
            shared,
            rx,
            pump,
            done_rx,
            session_name,
            session_handle,
            props_buf,
        }
    }

    fn with_state(
        shared: Arc<Shared>,
        rx: std::sync::mpsc::Receiver<EtwProcessRecord>,
        state: CollectorState,
    ) -> Self {
        *shared.state.lock() = state;
        let (_dtx, done_rx) = std::sync::mpsc::channel::<()>();
        Self::assemble(
            shared,
            rx,
            None,
            Some(done_rx),
            vec![],
            0,
            vec![0u64; props_struct_size().div_ceil(8)],
        )
    }

    pub fn state(&self) -> CollectorState {
        self.shared.state.lock().clone()
    }

    pub fn try_recv(&self) -> Option<EtwProcessRecord> {
        self.rx.try_recv().ok()
    }

    pub fn quality(&self) -> QualitySnapshot {
        let q = self.shared.quality.lock();
        QualitySnapshot {
            state: format!("{:?}", self.shared.state.lock()),
            received: q.received,
            etw_lost_events: q.etw_lost_events,
            buffers_seen: q.buffers_seen,
            decode_errors: q.decode_errors,
            ignored_events: q.ignored_events,
            channel_drops: q.channel_drops,
            window_start_us: q.window_start_us,
            window_end_us: q.window_end_us,
            pump_exit_code: q.pump_exit_code,
        }
    }

    /// Stop collection. Only our own session is ever stopped; other
    /// sessions are untouched by construction (we only hold our handle).
    pub fn stop(&mut self) {
        let cur = self.shared.state.lock().clone();
        match cur {
            CollectorState::Starting | CollectorState::Running => {}
            _ => return,
        }
        self.shared.stop_requested.store(true, Ordering::SeqCst);
        // SAFETY: session_handle + name + props belong to our live session.
        unsafe {
            ControlTraceW(
                CONTROLTRACE_HANDLE {
                    Value: self.session_handle,
                },
                self.session_name.as_ptr(),
                self.props_ptr(),
                EVENT_TRACE_CONTROL_STOP,
            );
        }
        if let Some(done) = self.done_rx.take() {
            let _ = done.recv_timeout(std::time::Duration::from_secs(10));
        }
        if let Some(h) = self.pump.take() {
            let _ = h.join();
        }
        *self.shared.state.lock() = CollectorState::Stopped {
            reason: "requested".into(),
        };
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.stop();
    }
}
