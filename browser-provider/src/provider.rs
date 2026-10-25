//! Browser Provider — SpaceOS Provider implementation for CDP-based browser integration.

use anyhow::Result;
use chrono::Utc;
use futures_util::StreamExt;
use serde_json::{json, Value};
use space_core::{
    CoreHandle, Event, Provider, ProviderError, ProviderHandle, ProviderInfo, StopToken,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, Duration};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::cdp::{fetch_targets, CdpClient, CdpSession, CdpTarget, DEFAULT_CDP_BASE};
use crate::objects::{actions, events, BrowserTabObject};

/// Browser Provider configuration.
#[derive(Debug, Clone)]
pub struct BrowserProviderConfig {
    /// CDP endpoint base URL (e.g., "http://127.0.0.1:9222")
    pub cdp_base: String,
    /// Provider identifier (default: "browser")
    pub provider_id: String,
    /// Poll interval for target discovery (default: 5s)
    pub poll_interval_secs: u64,
}

impl Default for BrowserProviderConfig {
    fn default() -> Self {
        Self {
            cdp_base: DEFAULT_CDP_BASE.to_string(),
            provider_id: "browser".to_string(),
            poll_interval_secs: 5,
        }
    }
}

/// Internal state for the browser provider.
struct BrowserProviderState {
    approved_runtime: Option<space_core::provider_observation::BrowserRuntimeConfig>,
    core_handle: Option<CoreHandle>,
    stop_token: Option<StopToken>,
    /// Active CDP sessions per target (shared for concurrent access)
    sessions: HashMap<String, Arc<Mutex<CdpSession>>>,
    /// IPC pipe for standalone mode (no CoreHandle). When set and no
    /// CoreHandle is present, events are sent via `record_event` IPC.
    /// Actor is a core principal so the Core policy gate passes.
    standalone_pipe: Option<String>,
    /// Known tabs
    tabs: HashMap<String, BrowserTabObject>,
    /// Current target being observed
    current_target: Option<String>,
}

/// Browser Provider implementation.
pub struct BrowserProvider {
    config: BrowserProviderConfig,
    state: Arc<RwLock<BrowserProviderState>>,
}

impl BrowserProvider {
    /// Create a new browser provider with default configuration.
    pub fn new() -> Self {
        Self::with_config(BrowserProviderConfig::default())
    }

    /// Create a new browser provider with custom configuration.
    pub fn with_config(config: BrowserProviderConfig) -> Self {
        let state = BrowserProviderState {
            approved_runtime: None,
            core_handle: None,
            stop_token: None,
            sessions: HashMap::new(),
            tabs: HashMap::new(),
            current_target: None,
            standalone_pipe: None,
        };
        Self {
            config,
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Get the provider ID.
    pub fn provider_id(&self) -> &str {
        &self.config.provider_id
    }

    /// Get the CDP base URL.
    pub fn cdp_base(&self) -> &str {
        &self.config.cdp_base
    }

    /// Emit an event via `record_event` IPC (standalone mode, no CoreHandle).
    /// Actor is "browser" (a core principal) so the Core policy gate
    /// passes; `provider_id` stays this provider's id for attribution.
    async fn emit_ipc(&self, pipe: &str, event_type: &str, payload: Value) -> Result<()> {
        let mut ev = Event::new(event_type, "browser", payload);
        ev.provider_id = Some(self.config.provider_id.clone());
        space_core::ipc::send_provider_principal_request(pipe, "browser", "record_event", serde_json::json!(ev)).await?;
        Ok(())
    }

    /// Emit a full observation via `report_observation` IPC (standalone).
    /// The object + event land atomically in Core; redelivery is safe
    /// (stable observation_id → applied:false, no dupes).
    /// Actor binding follows the `emit_ipc` precedent: providers report
    /// as the "browser" core principal (same-machine envelope auth);
    /// `event.actor` + object `provider_id` keep true attribution.
    async fn emit_observation(&self, pipe: &str, obs: &space_core::Observation) -> Result<()> {
        let mut params = serde_json::to_value(obs).unwrap();
        params["actor"] = json!("browser");
        let resp = space_core::ipc::send_provider_principal_request(pipe, "browser", "report_observation", params).await?;
        if resp.get("ok").and_then(|v| v.as_bool()) != Some(true) {
            anyhow::bail!("report_observation rejected: {resp}");
        }
        Ok(())
    }

    /// Standalone runner for the `browser-provider` binary: registers via
    /// `register_provider` IPC, serves observations by polling CDP and
    /// emitting `record_event` IPC. Used when no in-process CoreHandle
    /// exists (the `Provider::start` path needs one and is unchanged).
    pub async fn run_standalone(&self, core_pipe: &str) -> Result<()> {
        if std::env::var_os("SPACEOS_BROWSER_OBSERVATION_SOURCE").is_some() {
            let raw=std::env::var("SPACEOS_BROWSER_OBSERVATION_SOURCE").map_err(|_|anyhow::anyhow!("invalid browser observation source"))?;
            let source:space_core::provider_observation::BrowserRuntimeConfig=serde_json::from_str(&raw).map_err(|_|anyhow::anyhow!("invalid browser observation source"))?;
            source.validate()?;space_core::ipc::provider_instance_credential()?;
            loop {
                if let Err(_) = self.verified_observation_cycle(core_pipe,&source).await {
                    let mut state=self.state.write().await;state.tabs.clear();state.sessions.clear();state.current_target=None;
                    warn!("verified browser observation cycle unavailable or denied");
                }
                tokio::time::sleep(Duration::from_secs(self.config.poll_interval_secs.max(1))).await;
            }
        }
        {
            let mut state = self.state.write().await;
            state.standalone_pipe = Some(core_pipe.to_string());
        }
        // Register (retry while Core is coming up).
        let info = self.info();
        let mut registered = false;
        for _ in 0..60 {
            match space_core::ipc::send_provider_principal_request(
                core_pipe,
                "browser",
                "register_provider",
                serde_json::json!(info),
            )
            .await
            {
                Ok(_) => { registered = true; break; }
                _ => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
        anyhow::ensure!(registered, "browser Provider registration unavailable");
        let mut observed: HashSet<String> = HashSet::new();
        let mut tick = interval(Duration::from_secs(self.config.poll_interval_secs));
        loop {
            tick.tick().await;
            // Best-effort discovery; browser may be absent. Errors are
            // logged, never fatal: the next tick retries.
            if let Err(e) = self.discover_tabs().await {
                warn!("browser provider: discover tabs error: {e}");
            }
            let targets = match fetch_targets(&self.config.cdp_base).await {
                Ok(t) => t,
                Err(e) => {
                    warn!("browser provider: fetch targets error: {e}");
                    continue;
                }
            };
            // Drop observations for closed targets.
            let live: HashSet<String> = targets.iter().map(|t| t.id.clone()).collect();
            observed.retain(|id| live.contains(id));
            for target in &targets {
                if self.state.read().await.sessions.contains_key(&target.id) {
                    observed.insert(target.id.clone());
                    continue;
                }
                if self.attach_target(target).await.is_err() {
                    continue;
                }
                observed.insert(target.id.clone());
                let this = self.clone();
                let target_id = target.id.clone();
                let session = self.state.read().await.sessions.get(&target_id).cloned();
                if let Some(session) = session {
                    tokio::spawn(async move {
                        if let Err(e) = this.observe_session(target_id, session).await {
                            warn!("browser provider session error: {e}");
                        }
                    });
                }
            }
        }
    }

    /// One metadata-only discovery cycle. Cache is adopted only from Core's
    /// canonical response; repeat cycles/restarts use deterministic identities.
    pub async fn verified_observation_cycle(&self,pipe:&str,source:&space_core::provider_observation::BrowserRuntimeConfig)->Result<usize> {
        use space_core::{ipc,provider_observation::browser_tab_observation};
        source.validate()?;anyhow::ensure!(self.config.provider_id=="browser"&&self.config.cdp_base==source.cdp_origin,"browser source configuration mismatch");
        self.state.write().await.approved_runtime=Some(source.clone());
        let (instance,token)=ipc::provider_instance_credential()?;
        ipc::send_provider_principal_request(pipe,"browser","register_provider",serde_json::to_value(self.info())?).await?;
        ipc::send_provider_principal_request(pipe,"browser","register_provider_instance",json!({"actor":"browser","instance_id":instance,"instance_token":token,"source_id":source.source_id,"runtime_epoch":source.runtime_epoch,"browser_debugger_id":source.expected_browser_debugger_id})).await?;
        let targets=CdpClient::new(&source.cdp_origin)?.pinned_targets(source).await?;
        let mut tabs=HashMap::new();
        for t in targets {
            // Unsupported page URLs are excluded, not coerced into another source.
            if space_core::provider_observation::sanitized_browser_url(&t.url).is_err(){continue;}
            let obs=browser_tab_observation(&instance,source,&t.id,&t.url,t.title.as_deref())?;
            let mut params=serde_json::to_value(obs)?;params["actor"]=json!("browser");params["instance_id"]=json!(instance);params["instance_token"]=json!(token);
            let response=ipc::send_provider_principal_request(pipe,"browser","report_observation",params).await.map_err(|_|anyhow::anyhow!("verified browser observation rejected"))?;
            let outcome:space_core::ObservationOutcome=serde_json::from_value(response["result"].clone())?;
            anyhow::ensure!(!outcome.event_id.is_empty()&&if outcome.applied{outcome.seq>0}else{outcome.seq==0},"invalid browser observation outcome");
            let object:space_core::Object=serde_json::from_value(response["result"]["object"].clone())?;
            let tab=BrowserTabObject::from_canonical(&object)?;tabs.insert(tab.target_id.clone(),tab);
        }
        let count=tabs.len();let mut state=self.state.write().await;state.tabs=tabs;state.current_target=Self::pick_current(&state);Ok(count)
    }

    /// Register a tab as a SpaceOS object and emit event.
    /// Uses report_observation so the object + event land atomically in Core.
    async fn register_tab(&self, tab: &BrowserTabObject) -> Result<()> {
        let obj = tab.to_space_object();
        let payload = json!({
            "tab_id": tab.id,
            "target_id": tab.target_id,
            "url": tab.url,
            "title": tab.title,
        });
        let event = Event::new(
            events::BROWSER_TAB_CREATED,
            &self.config.provider_id,
            payload.clone(),
        );
        let obs = space_core::Observation {
            observation_id: format!("{}-tab-{}", self.config.provider_id, tab.target_id),
            world_id: "".to_string(),
            facts: vec![payload.clone()],
            objects: vec![obj],
            relations: vec![],
            delete_ids: vec![],
            event,
        };
        if let Some(core) = &self.state.read().await.core_handle {
            core.record_event(Event::new(
                events::BROWSER_TAB_CREATED,
                &self.config.provider_id,
                payload,
            ))
            .await?;
            return Ok(());
        }
        if let Some(pipe) = self.state.read().await.standalone_pipe.clone() {
            self.emit_observation(&pipe, &obs).await?;
        }
        Ok(())
    }

    /// Emit tab closed event.
    async fn emit_tab_closed(&self, tab: &BrowserTabObject) -> Result<()> {
        let payload = json!({
            "tab_id": tab.id,
            "target_id": tab.target_id,
            "url": tab.url,
        });
        if let Some(core) = &self.state.read().await.core_handle {
            core.record_event(Event::new(
                events::BROWSER_TAB_CLOSED,
                &self.config.provider_id,
                payload,
            ))
            .await?;
            return Ok(());
        }
        if let Some(pipe) = self.state.read().await.standalone_pipe.clone() {
            self.emit_ipc(&pipe, events::BROWSER_TAB_CLOSED, payload)
                .await?;
        }
        Ok(())
    }

    /// Emit navigation event.
    async fn emit_navigated(&self, target_id: &str, url: &str) -> Result<()> {
        let payload = json!({
            "target_id": target_id,
            "url": url,
        });
        if let Some(core) = &self.state.read().await.core_handle {
            core.record_event(Event::new(
                events::BROWSER_NAVIGATED,
                &self.config.provider_id,
                payload,
            ))
            .await?;
            return Ok(());
        }
        if let Some(pipe) = self.state.read().await.standalone_pipe.clone() {
            self.emit_ipc(&pipe, events::BROWSER_NAVIGATED, payload)
                .await?;
        }
        Ok(())
    }

    /// Emit page loaded event.
    async fn emit_loaded(&self, target_id: &str, url: &str) -> Result<()> {
        let payload = json!({
            "target_id": target_id,
            "url": url,
        });
        if let Some(core) = &self.state.read().await.core_handle {
            core.record_event(Event::new(
                events::BROWSER_LOADED,
                &self.config.provider_id,
                payload,
            ))
            .await?;
            return Ok(());
        }
        if let Some(pipe) = self.state.read().await.standalone_pipe.clone() {
            self.emit_ipc(&pipe, events::BROWSER_LOADED, payload)
                .await?;
        }
        Ok(())
    }

    /// Discover and register all available tabs.
    async fn discover_tabs(&self) -> Result<()> {
        let targets = fetch_targets(&self.config.cdp_base).await?;
        let mut state = self.state.write().await;

        // Track which targets are still alive
        let mut seen_ids = Vec::new();

        for target in &targets {
            seen_ids.push(target.id.clone());

            if state.tabs.contains_key(&target.id) {
                continue;
            }

            let tab = BrowserTabObject::new(
                &self.config.provider_id,
                &target.id,
                &target.url,
                target.title.clone(),
            );
            state.tabs.insert(target.id.clone(), tab.clone());
            drop(state);
            // Registration must not poison discovery: on failure (Core
            // not ready yet is the classic race) drop the tab from state
            // so the next poll retries. Observation ids are stable, so a
            // later retry is idempotent, never a duplicate.
            if let Err(e) = self.register_tab(&tab).await {
                warn!(
                    "browser provider: register tab {} failed, will retry: {e}",
                    target.id
                );
                self.state.write().await.tabs.remove(&target.id);
            }
            state = self.state.write().await;
        }

        // Check for closed tabs
        let known_ids: Vec<String> = state.tabs.keys().cloned().collect();
        for id in known_ids {
            if !seen_ids.contains(&id) {
                if let Some(tab) = state.tabs.remove(&id) {
                    state.sessions.remove(&id);
                    if state.current_target.as_deref() == Some(&id) {
                        state.current_target = Self::pick_current(&state);
                    }
                    drop(state);
                    self.emit_tab_closed(&tab).await?;
                    state = self.state.write().await;
                }
            }
        }

        Ok(())
    }

    /// True for the Dashboard's own pages: navigation must never retarget
    /// the tab the user drives SpaceOS from.
    fn is_dashboard_url(url: &str) -> bool {
        let u = url.to_lowercase();
        u.starts_with("http://127.0.0.1:8765")
            || u.starts_with("http://localhost:8765")
            || u.starts_with("https://127.0.0.1:8765")
            || u.starts_with("https://localhost:8765")
    }

    /// Pick the action target: the first non-dashboard tab, falling back to
    /// any tab when only the dashboard is open. Keeps navigation away from
    /// the Dashboard UI itself.
    fn pick_current(state: &BrowserProviderState) -> Option<String> {
        let mut fallback = None;
        for (id, tab) in &state.tabs {
            if !Self::is_dashboard_url(&tab.url) {
                return Some(id.clone());
            }
            if fallback.is_none() {
                fallback = Some(id.clone());
            }
        }
        fallback.or_else(|| state.sessions.keys().next().cloned())
    }

    /// Attach to a target and start observing events.
    async fn attach_target(&self, target: &CdpTarget) -> Result<()> {
        let mut state = self.state.write().await;

        if state.sessions.contains_key(&target.id) {
            return Ok(());
        }

        let client = CdpClient::new(&self.config.cdp_base)?;
        let session = client.connect_target(&target.ws_url).await?;
        {
            let mut session_guard = session.lock().await;
            session_guard.enable_page().await?;
        }

        state.sessions.insert(target.id.clone(), session);
        state.current_target = Self::pick_current(&state);

        Ok(())
    }

    /// Observe events from a CDP session.
    async fn observe_session(
        &self,
        target_id: String,
        session: Arc<Mutex<CdpSession>>,
    ) -> Result<()> {
        loop {
            // Check stop token
            let stop_token = self.state.read().await.stop_token.clone();
            if let Some(stop) = stop_token {
                stop.wait().await;
                break;
            }

            // Get next message with a bounded wait: the session lock must
            // be released regularly so action commands (navigate/reload/
            // get_tabs) can use the same session. Holding the guard across
            // an unbounded `ws.next().await` starves every action.
            let next = {
                let mut session_guard = session.lock().await;
                tokio::time::timeout(Duration::from_secs(1), session_guard.ws.next()).await
            };

            // Timeout: no message within 1s. Loop around so the session
            // lock is released and action commands can proceed.
            let msg_opt = match next {
                Err(_) => continue,
                Ok(m) => m,
            };
            match msg_opt {
                Some(Ok(msg)) => {
                    if let Message::Text(text) = msg {
                        let v: Value = match serde_json::from_str(text.as_str()) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("cdp message parse error: {}", e);
                                continue;
                            }
                        };

                        if let Some(method) = v.get("method").and_then(|m| m.as_str()) {
                            match method {
                                "Page.frameNavigated" => {
                                    let frame = &v["params"]["frame"];
                                    let url = frame["url"].as_str().unwrap_or("");
                                    let _ = self.emit_navigated(&target_id, url).await;

                                    // Update tab URL
                                    let mut state = self.state.write().await;
                                    if let Some(tab) = state.tabs.get_mut(&target_id) {
                                        tab.url = url.to_string();
                                        tab.updated_at = Utc::now();
                                    }
                                }
                                "Page.loadEventFired" => {
                                    let state = self.state.read().await;
                                    let url = state
                                        .tabs
                                        .get(&target_id)
                                        .map(|t| t.url.as_str())
                                        .unwrap_or("");
                                    let _ = self.emit_loaded(&target_id, url).await;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    warn!("cdp ws error for target {}: {}", target_id, e);
                    break;
                }
                None => {
                    warn!("cdp ws closed for target {}", target_id);
                    break;
                }
            }
        }

        // Clean up session
        let mut state = self.state.write().await;
        state.sessions.remove(&target_id);
        if state.current_target.as_deref() == Some(&target_id) {
            state.current_target = Self::pick_current(&state);
        }

        Ok(())
    }

    /// Main observation loop.
    async fn observation_loop(&self) -> Result<()> {
        let mut poll_interval = interval(Duration::from_secs(self.config.poll_interval_secs));

        loop {
            tokio::select! {
                _ = async {
                    let stop_token = self.state.read().await.stop_token.clone();
                    if let Some(stop) = stop_token {
                        stop.wait().await;
                    }
                } => {
                    info!("browser provider: stop signal received");
                    break;
                }
                _ = poll_interval.tick() => {
                    if let Err(e) = self.discover_tabs().await {
                        warn!("browser provider: discover tabs error: {}", e);
                    }

                    // Attach to all available targets that don't have sessions yet
                    let targets = match fetch_targets(&self.config.cdp_base).await {
                        Ok(t) => t,
                        Err(e) => {
                            warn!("browser provider: fetch targets error: {}", e);
                            continue;
                        }
                    };

                    let targets_to_attach: Vec<CdpTarget> = {
                        let state = self.state.read().await;
                        targets
                            .into_iter()
                            .filter(|t| !state.sessions.contains_key(&t.id))
                            .collect()
                    };

                    for target in targets_to_attach {
                        if let Err(e) = self.attach_target(&target).await {
                            warn!("browser provider: attach target {} error: {}", target.id, e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Resolve the current target's live CDP session. Session-bound
    /// actions (navigate/reload/content/tabs control) need it; state
    /// reads like get_tabs must NOT require it (a missing session must
    /// never hide the known tab list).
    ///
    /// Self-healing: when the cached target is gone (e.g. its tab was
    /// closed before a session attached), re-pick from known tabs once
    /// instead of failing forever on a stale id.
    async fn live_session(&self) -> Result<(String, Arc<Mutex<CdpSession>>)> {
        let target_id = {
            let state = self.state.read().await;
            state.current_target.clone()
        };
        let target_id = target_id
            .ok_or_else(|| ProviderError::InvalidConfig("No active browser target".to_string()))?;
        let session = {
            let state = self.state.read().await;
            state.sessions.get(&target_id).cloned()
        };
        if let Some(session) = session {
            return Ok((target_id, session));
        }
        // Stale cache: drop it, re-pick, retry once.
        let retried = {
            let mut state = self.state.write().await;
            if state.current_target.as_deref() == Some(&target_id) {
                state.current_target = None;
            }
            Self::pick_current(&state)
        };
        let retried =
            retried.ok_or_else(|| ProviderError::InvalidConfig("No active session".to_string()))?;
        {
            let mut state = self.state.write().await;
            state.current_target = Some(retried.clone());
        }
        let session = {
            let state = self.state.read().await;
            state.sessions.get(&retried).cloned()
        };
        session
            .map(|s| (retried.clone(), s))
            .ok_or_else(|| ProviderError::InvalidConfig("No active session".to_string()).into())
    }

    /// Resolve a CDP session for an explicitly named target. Attaches on
    /// demand (a freshly created tab may not have a session yet); errors
    /// are returned honestly instead of falling back to another tab.
    async fn session_for_target(
        &self,
        target_id: &str,
    ) -> Result<(String, Arc<Mutex<CdpSession>>)> {
        if let Some(session) = self.state.read().await.sessions.get(target_id).cloned() {
            return Ok((target_id.to_string(), session));
        }
        let targets = fetch_targets(&self.config.cdp_base).await?;
        let target = targets
            .into_iter()
            .find(|t| t.id == target_id)
            .ok_or_else(|| ProviderError::InvalidConfig(format!("unknown target {target_id}")))?;
        self.attach_target(&target).await?;
        let session = self.state.read().await.sessions.get(target_id).cloned();
        session.map(|s| (target_id.to_string(), s)).ok_or_else(|| {
            ProviderError::InvalidConfig(format!("no session for target {target_id}")).into()
        })
    }

    /// True for transport-level CDP failures (dead WebSocket, reset or
    /// aborted TCP connection, timeouts). Such errors say nothing about
    /// whether the command itself was valid: the session they came from
    /// must be evicted and the action retried on a fresh session.
    /// Anything else (bad params, refused close) is a real answer and
    /// must NOT be retried.
    fn is_connection_error(e: &anyhow::Error) -> bool {
        if e.chain().any(|c| c.is::<std::io::Error>()) {
            return true;
        }
        let m = format!("{e:#}").to_lowercase();
        [
            "connection",
            "closed",
            "reset",
            "abort",
            "broken pipe",
            "eof",
            "timed out",
            "timeout",
            "10053",
            "10054",
        ]
        .iter()
        .any(|s| m.contains(s))
    }

    /// Execute a browser action.
    async fn execute_action(&self, action: &str, target: &Value) -> Result<Value> {
        let approved=self.state.read().await.approved_runtime.clone();
        if let Some(source)=approved {let session=CdpClient::new(&source.cdp_origin)?.pinned_browser(&source).await?;session.lock().await.ws.close(None).await?;}
        // State-only read: served even with zero live sessions.
        if action == actions::GET_TABS {
            let state = self.state.read().await;
            let tabs: Vec<Value> = state
                .tabs
                .values()
                .map(|t| {
                    json!({
                        "tab_id": t.id,
                        "target_id": t.target_id,
                        "url": t.url,
                        "title": t.title,
                    })
                })
                .collect();
            return Ok(json!({"tabs": tabs}));
        }

        // Optional explicit target: the World browser panel streams/acts
        // on a tab the user selected without retargeting the dashboard.
        // Absent/empty means the classic behaviour (current target).
        let explicit = target
            .get("target_id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let (target_id, session) = match explicit {
            Some(tid) => self.session_for_target(tid).await?,
            None => self.live_session().await?,
        };
        match self
            .execute_on_session(action, target, &target_id, &session)
            .await
        {
            Ok(v) => Ok(v),
            Err(e) if Self::is_connection_error(&e) => {
                // The session died under us (browser restart, target
                // churn during Chrome startup, tab closed externally):
                // evict it, re-discover immediately, and retry once on
                // the SAME target only. A different target means the tab
                // is really gone — return the original error honestly
                // instead of acting on the wrong tab.
                self.retry_after_reconnect(action, target, &target_id, e)
                    .await
            }
            Err(e) => Err(e),
        }
    }

    /// Re-discover targets, re-attach a live session for `target_id`,
    /// and run the action once more. Same-target-only.
    async fn retry_after_reconnect(
        &self,
        action: &str,
        target: &Value,
        target_id: &str,
        first_err: anyhow::Error,
    ) -> Result<Value> {
        {
            let mut state = self.state.write().await;
            state.sessions.remove(target_id);
            if state.current_target.as_deref() == Some(target_id) {
                state.current_target = Self::pick_current(&state);
            }
        }
        // Immediate re-discovery instead of waiting for the poll tick.
        // Best-effort: if CDP itself is down, fall through to the
        // original error below.
        let mut attached = false;
        if self.discover_tabs().await.is_ok() {
            if let Ok(targets) = crate::cdp::fetch_targets(&self.config.cdp_base).await {
                if let Some(t) = targets.into_iter().find(|t| t.id == target_id) {
                    attached = self.attach_target(&t).await.is_ok();
                }
            }
        }
        if !attached {
            return Err(first_err);
        }
        let (tid2, session2) = self.live_session().await?;
        if tid2 != target_id {
            return Err(first_err);
        }
        self.execute_on_session(action, target, &tid2, &session2)
            .await
    }

    /// Run the action against one live CDP session. Callers handle
    /// transport failures (see `execute_action`); this method never
    /// retries by itself.
    async fn execute_on_session(
        &self,
        action: &str,
        target: &Value,
        target_id: &str,
        session: &Arc<Mutex<CdpSession>>,
    ) -> Result<Value> {
        let mut session_guard = session.lock().await;

        match action {
            actions::NAVIGATE => {
                let url = target["url"]
                    .as_str()
                    .ok_or_else(|| ProviderError::InvalidConfig("Missing url".to_string()))?;
                session_guard.navigate(url).await?;
                self.emit_navigated(target_id, url).await?;
                Ok(json!({"navigated": url}))
            }
            actions::RELOAD => {
                session_guard.reload().await?;
                Ok(json!({"reloaded": true}))
            }
            actions::GET_PAGE_CONTENT => {
                // DOM.getDocument yields a node tree, not markup: resolve
                // the root node id and fetch its outer HTML so callers get
                // a real `document` string (empty only on genuine failure).
                let tree = session_guard.get_document().await?;
                let node_id = tree
                    .get("root")
                    .and_then(|r| r.get("nodeId"))
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| {
                        ProviderError::InvalidConfig("DOM has no root nodeId".to_string())
                    })?;
                let html = session_guard.get_outer_html(node_id).await?;
                Ok(json!({"document": html}))
            }
            actions::SCREENSHOT => {
                // Live visual state of this session's page: a single CDP
                // frame, base64-encoded. Read-only; no input is injected.
                let format = target["format"].as_str().unwrap_or("jpeg");
                let mut params = json!({ "format": format });
                if format == "jpeg" {
                    // Lower default quality keeps the 1.5s panel stream
                    // small on loopback while staying legible.
                    params["quality"] = json!(target["quality"].as_u64().unwrap_or(65));
                }
                let r = session_guard
                    .send_command("Page.captureScreenshot", Some(params))
                    .await?;
                let data = r
                    .get("data")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                if data.is_empty() {
                    return Err(ProviderError::InvalidConfig(
                        "screenshot: CDP returned no image data".to_string(),
                    )
                    .into());
                }
                Ok(json!({ "format": format, "data": data }))
            }
            // Target-domain methods are browser-wide: any live page
            // session can issue them (no hidden parallel path — this is
            // the same Core-routed provider action pipeline).
            actions::NEW_TAB => {
                let url = target["url"].as_str().unwrap_or("about:blank");
                let r = session_guard
                    .send_command("Target.createTarget", Some(json!({"url": url})))
                    .await?;
                let new_id = r.get("targetId").and_then(|v| v.as_str()).unwrap_or("");
                Ok(json!({"target_id": new_id, "url": url}))
            }
            actions::ACTIVATE_TAB => {
                let id = target["target_id"]
                    .as_str()
                    .ok_or_else(|| ProviderError::InvalidConfig("Missing target_id".to_string()))?;
                if id.trim().is_empty() {
                    anyhow::bail!("empty target_id");
                }
                session_guard
                    .send_command("Target.activateTarget", Some(json!({"targetId": id})))
                    .await?;
                Ok(json!({"activated": id}))
            }
            actions::CLOSE_TAB => {
                let id = target["target_id"]
                    .as_str()
                    .ok_or_else(|| ProviderError::InvalidConfig("Missing target_id".to_string()))?;
                if id.trim().is_empty() {
                    anyhow::bail!("empty target_id");
                }
                // Explicit user-confirmed closes only (the UI confirms);
                // the provider never auto-closes anything.
                let r = session_guard
                    .send_command("Target.closeTarget", Some(json!({"targetId": id})))
                    .await?;
                let ok = r.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
                if !ok {
                    anyhow::bail!("browser refused to close target {id}");
                }
                // Synchronous state cleanup: the tab is gone in the
                // browser as of this response, so drop it from state now
                // instead of waiting for the next discovery poll (up to
                // poll_interval_secs away). The poll diff is keyed on
                // state.tabs, so it will not emit a duplicate close.
                drop(session_guard);
                let closed_tab = {
                    let mut state = self.state.write().await;
                    state.sessions.remove(id);
                    if state.current_target.as_deref() == Some(id) {
                        state.current_target = Self::pick_current(&state);
                    }
                    state.tabs.remove(id)
                };
                if let Some(tab) = closed_tab {
                    self.emit_tab_closed(&tab).await?;
                }
                Ok(json!({"closed": id}))
            }
            actions::EXECUTE_SCRIPT => {
                let expression = target["expression"].as_str().ok_or_else(|| {
                    ProviderError::InvalidConfig("Missing expression".to_string())
                })?;
                let result = session_guard.evaluate(expression).await?;
                Ok(json!({"result": result}))
            }
            other => Err(ProviderError::UnsupportedCapability(other.to_string()).into()),
        }
    }

    /// Public method for IPC server to execute browser actions.
    /// Called only after the existing IPC dispatch verifier validates the MAC.
    pub async fn execute_world_action(&self, req: &space_core::action::DispatchRequest) -> Result<Value> {
        use space_core::{action::BrowserWorldTarget, provider_observation::browser_tab_observation};
        anyhow::ensure!(req.actor == space_core::service_registration::BROWSER_WORLD_ACTOR
            && req.provider_id == "browser" && matches!(req.action.as_str(), "get_tabs" | "screenshot"), "unsupported scoped Browser action");
        let target: BrowserWorldTarget = serde_json::from_value(req.target.clone())?;
        target.options.validate()?;
        let (instance, _) = space_core::ipc::provider_instance_credential()?;
        anyhow::ensure!(target.scope_version == 1 && target.instance_id == instance
            && self.state.read().await.approved_runtime.as_ref() == Some(&target.runtime)
            && self.config.cdp_base == target.runtime.cdp_origin, "Browser execution authority mismatch");
        let live = CdpClient::new(&target.runtime.cdp_origin)?.pinned_targets(&target.runtime).await?;
        let mut canonical = Vec::new();
        {
            let state = self.state.read().await;
            for tab in state.tabs.values() {
                if live.iter().filter(|t|t.id == tab.target_id).count() != 1 { continue; }
                let obs = browser_tab_observation(&instance, &target.runtime, &tab.target_id, &tab.url, tab.title.as_deref())?;
                let mut object = obs.objects[0].clone();
                anyhow::ensure!(object.id == tab.id && object.external_id == tab.external_id && tab.provider_id == "browser", "unverified Browser cache");
                object.created_at = tab.created_at; object.updated_at = tab.updated_at;
                canonical.push(object);
            }
        }
        if req.action == "get_tabs" {
            anyhow::ensure!(target.object.is_none(), "get_tabs object denied");
            return Ok(json!({"authority":target,"tabs":canonical.iter().map(|o|json!({"canonical":o})).collect::<Vec<_>>()}));
        }
        let object = target.object.as_ref().ok_or_else(||anyhow::anyhow!("canonical screenshot object required"))?;
        anyhow::ensure!(canonical.iter().filter(|o|serde_json::to_value(o).ok() == serde_json::to_value(object).ok()).count() == 1,
            "canonical Browser handle unavailable or changed");
        let tid = object.properties["target_id"].as_str().ok_or_else(||anyhow::anyhow!("missing Browser target"))?;
        let (actual, session) = self.session_for_target(tid).await?;
        anyhow::ensure!(actual == tid, "substituted Browser target");
        // Re-pin after session attachment. No retry-after-reconnect/current-tab fallback.
        let current = CdpClient::new(&target.runtime.cdp_origin)?.pinned_targets(&target.runtime).await?;
        anyhow::ensure!(current.iter().filter(|t|t.id == tid).count() == 1, "Browser target disappeared");
        let result = self.execute_on_session("screenshot", &json!({"format":target.options.format,"quality":target.options.quality}), tid, &session).await?;
        Ok(json!({"authority":target,"data":result["data"]}))
    }

    pub async fn execute_action_for_ipc(&self, action: &str, target: &Value) -> Result<Value> {
        self.execute_action(action, target).await
    }
}

impl Default for BrowserProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl Provider for BrowserProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            id: self.config.provider_id.clone(),
            name: "browser-provider".to_string(),
            capabilities: vec!["observe".to_string(), "navigate".to_string()],
            version: Some("0.1.0".to_string()),
        }
    }

    async fn start(
        &self,
        core: CoreHandle,
        stop: StopToken,
    ) -> Result<ProviderHandle, ProviderError> {
        info!(
            "browser provider: starting with CDP at {}",
            self.config.cdp_base
        );

        // Initialize state with core handle and stop token
        {
            let mut state = self.state.write().await;
            state.core_handle = Some(core.clone());
            state.stop_token = Some(stop.clone());
        }

        // Initial target discovery
        self.discover_tabs().await.map_err(ProviderError::Ipc)?;

        // Attach to all available targets
        let targets = fetch_targets(&self.config.cdp_base)
            .await
            .map_err(ProviderError::Ipc)?;
        for target in &targets {
            self.attach_target(target)
                .await
                .map_err(ProviderError::Ipc)?;
        }

        // Spawn observation loop
        let provider = self.clone();
        tokio::spawn(async move {
            if let Err(e) = provider.observation_loop().await {
                warn!("browser provider observation loop error: {}", e);
            }
        });

        // Spawn event processing for attached sessions
        let state = self.state.clone();
        let provider = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));
            loop {
                interval.tick().await;

                // Get sessions that need observation
                let sessions_to_observe: Vec<(String, Arc<Mutex<CdpSession>>)> = {
                    let state = state.read().await;
                    state
                        .sessions
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                };

                for (target_id, session) in sessions_to_observe {
                    let provider = provider.clone();
                    tokio::spawn(async move {
                        if let Err(e) = provider.observe_session(target_id, session).await {
                            warn!("browser provider session error: {}", e);
                        }
                    });
                }

                // Check stop
                if let Some(stop) = &state.read().await.stop_token {
                    if stop.is_stopped() {
                        break;
                    }
                }
            }
        });

        Ok(ProviderHandle::new(stop, self.config.provider_id.clone()))
    }

    async fn stop(&self) -> Result<(), ProviderError> {
        info!("browser provider: stopping");

        let mut state = self.state.write().await;
        state.stop_token.as_ref().map(|s| s.trigger());

        // Close all sessions by dropping the Arcs (WebSocket closes on drop)
        state.sessions.clear();
        state.tabs.clear();
        state.current_target = None;

        Ok(())
    }
}

// Implement Clone for BrowserProvider to allow spawning tasks
impl Clone for BrowserProvider {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            state: self.state.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_provider_info() {
        let provider = BrowserProvider::new();
        let info = provider.info();
        assert_eq!(info.id, "browser");
        assert_eq!(info.name, "browser-provider");
        assert!(info.capabilities.contains(&"observe".to_string()));
        assert!(info.capabilities.contains(&"navigate".to_string()));
    }

    #[tokio::test]
    async fn get_tabs_served_without_live_session() {
        // Regression: get_tabs is a state read and must not fail with
        // "No active session" when no CDP session exists (e.g. right
        // after a tab was closed).
        let provider = BrowserProvider::new();
        let out = provider
            .execute_action_for_ipc(actions::GET_TABS, &json!({}))
            .await
            .expect("get_tabs never needs a session");
        assert!(out.get("tabs").and_then(|v| v.as_array()).is_some());
    }

    #[tokio::test]
    async fn session_bound_actions_fail_openly_without_session() {
        let provider = BrowserProvider::new();
        let err = provider
            .execute_action_for_ipc(actions::NAVIGATE, &json!({"url": "https://example.com/"}))
            .await
            .expect_err("navigate without a session must fail");
        assert!(err.to_string().contains("No active browser target"));
    }

    #[test]
    fn test_config_default() {
        let config = BrowserProviderConfig::default();
        assert_eq!(config.cdp_base, DEFAULT_CDP_BASE);
        assert_eq!(config.provider_id, "browser");
        assert_eq!(config.poll_interval_secs, 5);
    }

    #[test]
    fn test_tab_object_creation() {
        let tab = BrowserTabObject::new(
            "browser",
            "target-1",
            "https://example.com",
            Some("Example".to_string()),
        );
        assert_eq!(tab.provider_id, "browser");
        assert_eq!(tab.target_id, "target-1");
        assert_eq!(tab.url, "https://example.com");
        assert_eq!(tab.title, Some("Example".to_string()));
    }

    #[test]
    fn dashboard_tab_is_never_preferred() {
        assert!(BrowserProvider::is_dashboard_url("http://127.0.0.1:8765/"));
        assert!(BrowserProvider::is_dashboard_url(
            "http://localhost:8765/api/health"
        ));
        assert!(!BrowserProvider::is_dashboard_url("https://example.com/"));
        assert!(!BrowserProvider::is_dashboard_url("chrome://newtab/"));
        let mut state = BrowserProviderState {
            approved_runtime: None,
            core_handle: None,
            stop_token: None,
            sessions: HashMap::new(),
            tabs: HashMap::new(),
            current_target: None,
            standalone_pipe: None,
        };
        state.tabs.insert(
            "d".to_string(),
            BrowserTabObject::new("browser", "d", "http://127.0.0.1:8765/", None),
        );
        state.tabs.insert(
            "n".to_string(),
            BrowserTabObject::new("browser", "n", "https://example.com/", None),
        );
        assert_eq!(BrowserProvider::pick_current(&state).as_deref(), Some("n"));
        state.tabs.remove("n");
        // Only the dashboard left: honest fallback instead of nothing.
        assert_eq!(BrowserProvider::pick_current(&state).as_deref(), Some("d"));
        state.tabs.clear();
        assert_eq!(BrowserProvider::pick_current(&state), None);
    }
}
