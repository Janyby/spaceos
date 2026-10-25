//! Core-owned authenticated filesystem instance and source/World bindings.
//! Registration is ephemeral; credentials and authority are server-configured.
use crate::{principal::PrincipalRegistry, Observation, ProviderInfo, Storage};
use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::Deserialize;
use std::{
    collections::HashSet,
    path::{Component, Path, PathBuf},
    sync::Mutex,
};
use subtle::ConstantTimeEq;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfiguredBinding {
    provider: String,
    instance_id: String,
    instance_token: String,
    root: PathBuf,
    world_id: String,
}

struct Binding {
    instance_id: String,
    secret: [u8; 32],
    root: PathBuf,
    world_id: String,
}

#[derive(Default)]
pub struct ObservationBindings {
    filesystem: Vec<Binding>,
    registered: Mutex<HashSet<String>>,
    browser: Vec<BrowserBinding>,
    browser_registered: Mutex<HashSet<String>>,
}

/// Not a wire DTO: only this module can construct the result of admission.
/// The IPC caller must still check the principal's grant in the resolved World.
pub(crate) struct VerifiedFilesystemAdmission {
    observation: Observation,
    instance_id: String,
    source_root: PathBuf,
    binding_digest: String,
}

impl VerifiedFilesystemAdmission {
    pub(crate) fn observation(&self) -> &Observation { &self.observation }
    pub(crate) fn source(&self) -> (&str, &Path, &str) {
        (&self.instance_id, &self.source_root, &self.binding_digest)
    }
}

impl ObservationBindings {
    pub fn from_env(principals: &PrincipalRegistry, transport_token: &str) -> Self {
        if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
            return Self::default();
        }
        let raw = std::env::var("SPACEOS_PROVIDER_INSTANCE_BINDINGS").unwrap_or_else(|_| "[]".into());
        let mut bindings = match Self::from_json(&raw).and_then(|bindings| {
            bindings.validate_distinct_principals(principals, transport_token)?;
            Ok(bindings)
        }) {
            Ok(bindings) => bindings,
            Err(error) => {
                // Never log raw configuration or credentials.
                tracing::error!("invalid Provider instance binding configuration: {error:#}");
                Self::default()
            }
        };
        if let Ok(raw) = std::env::var("SPACEOS_BROWSER_OBSERVATION_BINDINGS") {
            if bindings.configure_browser(&raw).and_then(|_|bindings.validate_distinct_principals(principals,transport_token)).is_err() {
                tracing::error!("invalid browser observation configuration");
                bindings.browser.clear();
            }
        }
        bindings
    }

    pub fn from_json(raw: &str) -> Result<Self> {
        let entries: Vec<ConfiguredBinding> = serde_json::from_str(raw)
            .map_err(|_| anyhow::anyhow!("invalid Provider instance binding JSON"))?;
        let mut filesystem: Vec<Binding> = Vec::new();
        for entry in entries {
            if entry.provider != "filesystem"
                || entry.instance_id.is_empty()
                || entry.instance_id.len() > 64
                || !entry
                    .instance_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                bail!("invalid filesystem Provider instance identity");
            }
            if entry.world_id.trim().is_empty() || entry.world_id != entry.world_id.trim() {
                bail!("observation binding requires an explicit World ID");
            }
            let decoded = URL_SAFE_NO_PAD
                .decode(&entry.instance_token)
                .map_err(|_| anyhow::anyhow!("invalid instance credential"))?;
            let secret: [u8; 32] = decoded
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid instance credential"))?;
            if !entry.root.is_absolute()
                || entry
                    .root
                    .components()
                    .any(|c| matches!(c, Component::ParentDir))
            {
                bail!("observation root must be absolute and traversal-free");
            }
            let root = std::fs::canonicalize(&entry.root)
                .with_context(|| format!("resolving observation root {:?}", entry.root))?;
            if !root.is_dir() {
                bail!("observation root must be a directory");
            }
            for previous in &filesystem {
                if previous.instance_id == entry.instance_id
                    && !bool::from(previous.secret.ct_eq(&secret))
                {
                    bail!("conflicting credentials for Provider instance");
                }
                if previous.instance_id != entry.instance_id
                    && bool::from(previous.secret.ct_eq(&secret))
                {
                    bail!("Provider instance credential is shared");
                }
                if within(&root, &previous.root) || within(&previous.root, &root) {
                    bail!("overlapping or ambiguous Provider sources");
                }
            }
            filesystem.push(Binding {
                instance_id: entry.instance_id,
                secret,
                root,
                world_id: entry.world_id,
            });
        }
        Ok(Self {
            filesystem,
            registered: Mutex::new(HashSet::new()),
            browser: Vec::new(),
            browser_registered: Mutex::new(HashSet::new()),
        })
    }

    pub fn validate_distinct_principals(
        &self,
        principals: &PrincipalRegistry,
        transport_token: &str,
    ) -> Result<()> {
        if self
            .filesystem
            .iter()
            .any(|binding| principals.contains_secret(&binding.secret))
        {
            bail!("instance credential conflicts with Core principal credential");
        }
        if self
            .filesystem
            .iter()
            .any(|binding| URL_SAFE_NO_PAD.encode(binding.secret) == transport_token)
        {
            bail!("instance credential conflicts with Core transport credential");
        }
        for b in &self.browser {
            anyhow::ensure!(!principals.contains_secret(&b.secret) && URL_SAFE_NO_PAD.encode(b.secret)!=transport_token,
                "browser credential conflicts with another credential");
        }
        Ok(())
    }

    fn authenticated(&self, instance_id: &str, token: &str) -> bool {
        let Ok(decoded) = URL_SAFE_NO_PAD.decode(token) else {
            return false;
        };
        if decoded.len() != 32 {
            return false;
        }
        self.filesystem.iter().any(|binding| {
            binding.instance_id == instance_id
                && bool::from(binding.secret.as_slice().ct_eq(decoded.as_slice()))
        })
    }

    pub fn register_instance(&self, instance_id: &str, token: &str) -> bool {
        if !self.authenticated(instance_id, token) {
            return false;
        }
        self.registered
            .lock()
            .unwrap()
            .insert(instance_id.to_owned());
        true
    }

    /// A reconciliation read uses the same immutable source binding, plus a
    /// separately checked Core-principal filesystem.reconcile.read capability. It never
    /// grants generic World reads or another instance's source.
    pub fn read_source(&self, instance: &str, token: &str, root: &Path) -> Result<(PathBuf, String)> {
        if !self.authenticated(instance, token) || !self.registered.lock().unwrap().contains(instance) {
            bail!("authorization_denied");
        }
        if !root.is_absolute() || root.components().any(|c| matches!(c, Component::ParentDir)) {
            bail!("invalid_binding");
        }
        if !self.filesystem.iter().any(|b| b.instance_id == instance && within(root, &b.root)) { bail!("invalid_binding"); }
        let actual = std::fs::canonicalize(root).map_err(|_| anyhow::anyhow!("invalid_binding"))?;
        let bindings: Vec<_> = self.filesystem.iter().filter(|b| b.instance_id == instance && within(&actual, &b.root)).collect();
        if bindings.len() != 1 { bail!("invalid_binding"); }
        Ok((actual, bindings[0].world_id.clone()))
    }

    pub fn source_owns_object(root: &Path, world: &str, object: &crate::Object) -> bool {
        if object.provider_id != "filesystem"
            || object.properties.get("world_id").and_then(serde_json::Value::as_str) != Some(world) { return false; }
        let Some(external) = object.external_id.as_deref() else { return false; };
        let path = Path::new(external);
        if object.properties.get("path").and_then(serde_json::Value::as_str) != Some(external)
            || !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir))
            || !within(path, root) || within(root, path) { return false; }
        // Missing files still need reconciliation; validate the nearest existing
        // ancestor so a replaced junction cannot turn a stored path into escape.
        let mut ancestor = path;
        loop {
            if ancestor.exists() {
                return std::fs::canonicalize(ancestor).is_ok_and(|p| within(&p, root));
            }
            let Some(parent) = ancestor.parent() else { return false; };
            ancestor = parent;
        }
    }

    /// Stable, non-secret authority pin in the existing Provider table.
    /// Re-registration after a Core restart must match this exact digest.
    pub fn registration_record(&self, instance_id: &str, token: &str) -> Option<ProviderInfo> {
        if !self.authenticated(instance_id, token) {
            return None;
        }
        self.instance_record(instance_id)
    }

    fn instance_record(&self, instance_id: &str) -> Option<ProviderInfo> {
        let mut scopes: Vec<String> = self
            .filesystem
            .iter()
            .filter(|b| b.instance_id == instance_id)
            .map(|b| {
                format!(
                    "{}\0{}",
                    b.root.to_string_lossy().to_lowercase(),
                    b.world_id
                )
            })
            .collect();
        scopes.sort();
        let encoded = scopes.join("\0");
        let hash = ring::digest::digest(&ring::digest::SHA256, encoded.as_bytes());
        let fingerprint: String = hash.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        Some(ProviderInfo {
            id: format!("observation-instance:{instance_id}"),
            name: "filesystem-observation-instance".into(),
            capabilities: vec![format!("binding-sha256:{fingerprint}")],
            version: None,
        })
    }

    pub(crate) fn dispatch_source(&self, instance:&str, path:&Path) -> Result<crate::action::CompletionBinding> {
        anyhow::ensure!(self.registered.lock().unwrap().contains(instance), "unregistered scan instance");
        anyhow::ensure!(path.is_absolute() && !path.components().any(|c|matches!(c,Component::ParentDir)),"invalid scan path");
        let actual=std::fs::canonicalize(path)?;
        let bindings:Vec<_>=self.filesystem.iter().filter(|b|b.instance_id==instance && within(&actual,&b.root)).collect();
        anyhow::ensure!(bindings.len()==1,"missing or ambiguous scan source");
        let b=bindings[0];
        let record=self.instance_record(instance).ok_or_else(||anyhow::anyhow!("unknown scan instance"))?;
        Ok(crate::action::CompletionBinding { binding_id:uuid::Uuid::new_v4().to_string(),instance_id:instance.into(),
            source_root:b.root.to_str().ok_or_else(||anyhow::anyhow!("non-Unicode scan source"))?.into(),world_id:b.world_id.clone(),
            source_digest:record.capabilities[0].clone() })
    }

    pub fn admit_file_created(
        &self,
        instance_id: &str,
        token: &str,
        obs: &mut Observation,
        storage: &Storage,
    ) -> Result<String> {
        self.admit_filesystem(instance_id, token, obs, storage, false)
    }

    fn admit_filesystem(
        &self, instance_id: &str, token: &str, obs: &mut Observation,
        storage: &Storage, changed: bool,
    ) -> Result<String> {
        if !self.authenticated(instance_id, token)
            || !self.registered.lock().unwrap().contains(instance_id)
        {
            bail!("Provider instance is unknown, unauthenticated or unregistered");
        }
        if storage.get_provider("filesystem")?.is_none() {
            bail!("filesystem Provider is not registered");
        }
        if obs.event.event_type != if changed { "FileChanged" } else { "FileCreated" }
            || obs.objects.len() != 1
            || !obs.relations.is_empty()
            || !obs.delete_ids.is_empty()
        {
            bail!("unsupported filesystem observation shape");
        }
        let object = &obs.objects[0];
        if !(object.r#type == "File" || (!changed && object.r#type == "Directory"))
            || object.provider_id != "filesystem"
            || obs.event.actor != "filesystem"
            || obs.event.provider_id.as_deref() != Some("filesystem")
            || obs.event.object_ids != [object.id.as_str()]
            || obs.event.payload.get("object_id").and_then(|v| v.as_str()) != Some(&object.id)
        {
            bail!("filesystem observation identity mismatch");
        }
        let external = object
            .external_id
            .as_deref()
            .context("missing external resource identity")?;
        if object.properties.get("path").and_then(|v| v.as_str()) != Some(external)
            || obs.event.payload.get("path").and_then(|v| v.as_str()) != Some(external)
        {
            bail!("filesystem observation path mismatch");
        }
        let path = Path::new(external);
        if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
            bail!("filesystem observation path is not absolute or contains traversal");
        }
        let actual = std::fs::canonicalize(path).context("resolving observed resource")?;
        if (object.r#type == "File" && !actual.is_file())
            || (object.r#type == "Directory" && !actual.is_dir())
        {
            bail!("observed resource type mismatch");
        }
        let matches: Vec<_> = self
            .filesystem
            .iter()
            .filter(|binding| {
                binding.instance_id == instance_id
                    && within(&actual, &binding.root)
                    && actual != binding.root
            })
            .collect();
        if matches.len() != 1 {
            bail!("filesystem observation has missing or ambiguous instance source binding");
        }
        let world = &matches[0].world_id;
        if (!obs.world_id.is_empty() && obs.world_id != *world)
            || (!obs.event.world_id.is_empty() && obs.event.world_id != *world)
            || object
                .properties
                .get("world_id")
                .is_some_and(|v| v.as_str() != Some(world))
            || obs
                .event
                .payload
                .get("world_id")
                .is_some_and(|v| v.as_str() != Some(world))
        {
            bail!("filesystem observation World conflicts with trusted binding");
        }
        if let Some(existing) = storage.get_object(&object.id)? {
            if existing.provider_id != "filesystem"
                || existing.external_id.as_deref() != Some(external)
                || existing.properties.get("world_id").and_then(|v| v.as_str()) != Some(world)
                || (!changed && storage
                    .observation_event_id(&obs.observation_id)?
                    .as_deref()
                    != Some(&obs.event.id))
            {
                bail!("filesystem observation attempts to overwrite another object");
            }
        }
        if let Some(existing) = storage.get_object_by_external("filesystem", external)? {
            if existing.id != object.id
                || existing.properties.get("world_id").and_then(|v| v.as_str()) != Some(world)
            {
                bail!("filesystem resource already has a different semantic object or World");
            }
        }
        obs.world_id = world.clone();
        obs.event.world_id = world.clone();
        obs.objects[0].properties["world_id"] = serde_json::json!(world);
        if changed {
            let current = storage.get_object(&obs.objects[0].id)?.context("FileChanged requires existing File")?;
            anyhow::ensure!(storage.has_file_admission(&current.id)?, "FileChanged requires valid File admission receipt");
            validate_file_change(&current, &obs.objects[0])?;
        }
        Ok(world.clone())
    }

    pub(crate) fn verify_file_created(
        &self, instance_id: &str, token: &str, mut obs: Observation, storage: &Storage,
    ) -> Result<VerifiedFilesystemAdmission> {
        let world = self.admit_file_created(instance_id, token, &mut obs, storage)?;
        self.verified_context(instance_id, token, obs, &world)
    }

    pub(crate) fn verify_file_changed(
        &self, instance_id: &str, token: &str, mut obs: Observation, storage: &Storage,
    ) -> Result<VerifiedFilesystemAdmission> {
        let world = self.admit_filesystem(instance_id, token, &mut obs, storage, true)?;
        self.verified_context(instance_id, token, obs, &world)
    }

    fn verified_context(&self, instance_id: &str, token: &str, obs: Observation, world: &str) -> Result<VerifiedFilesystemAdmission> {
        let actual = std::fs::canonicalize(obs.objects[0].external_id.as_ref().unwrap())?;
        let source = self.filesystem.iter().find(|b| b.instance_id == instance_id
            && b.world_id == world && within(&actual, &b.root) && actual != b.root)
            .context("validated source disappeared")?;
        let record = self.registration_record(instance_id, token).context("invalid instance authority")?;
        let digest = record.capabilities[0].strip_prefix("binding-sha256:")
            .context("invalid authority fingerprint")?.to_owned();
        Ok(VerifiedFilesystemAdmission {
            observation: obs, instance_id: instance_id.into(), source_root: source.root.clone(),
            binding_digest: digest,
        })
    }
}

/// FileChanged is not a generic object replacement. Rechecked in the write transaction.
pub(crate) fn validate_file_change(current: &crate::Object, incoming: &crate::Object) -> Result<()> {
    anyhow::ensure!(current.r#type == "File" && current.provider_id == "filesystem"
        && incoming.id == current.id && incoming.r#type == current.r#type
        && incoming.provider_id == current.provider_id && incoming.external_id == current.external_id
        && incoming.created_at == current.created_at, "FileChanged identity mismatch");
    let mut old = current.properties.as_object().context("invalid canonical File properties")?.clone();
    let mut proposed = incoming.properties.as_object().context("invalid FileChanged properties")?.clone();
    old.remove("meta");
    let meta = proposed.remove("meta").context("FileChanged metadata is required")?;
    anyhow::ensure!(old == proposed, "FileChanged modifies unapproved properties");
    let fields = meta.as_object().context("FileChanged metadata must be an object")?;
    const KEYS: [&str; 8] = ["path","file_type","size","extension","mime","created","modified","accessed"];
    anyhow::ensure!(fields.len() == KEYS.len() && KEYS.iter().all(|k| fields.contains_key(*k)), "unsupported FileChanged metadata fields");
    anyhow::ensure!(meta["path"].as_str() == current.external_id.as_deref()
        && meta["file_type"] == "File" && meta["size"].as_u64().is_some(), "invalid FileChanged metadata identity or size");
    anyhow::ensure!(meta["mime"].as_str().is_some_and(|s| !s.is_empty() && s.len() <= 255)
        && (meta["extension"].is_null() || meta["extension"].as_str().is_some_and(|s| s.len() <= 1024)), "invalid FileChanged metadata format");
    for key in ["created","modified","accessed"] {
        anyhow::ensure!(meta[key].is_null() || meta[key].as_str().is_some_and(|s|
            s.len() <= 64 && chrono::DateTime::parse_from_rfc3339(s).is_ok()), "invalid FileChanged metadata timestamp");
    }
    Ok(())
}

fn within(path: &Path, root: &Path) -> bool {
    #[cfg(windows)]
    {
        let p = path.to_string_lossy().trim_start_matches(r"\\?\").to_lowercase();
        let r = root.to_string_lossy().trim_start_matches(r"\\?\").to_lowercase();
        p == r || p.starts_with(&(r + std::path::MAIN_SEPARATOR_STR))
    }
    #[cfg(not(windows))]
    {
        path.starts_with(root)
    }
}

// Browser creation is a separate concrete contract, not a generic observer router.
#[derive(Debug, Clone, serde::Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrowserRuntimeConfig {
    pub source_id: String,
    pub world_id: String,
    pub cdp_origin: String,
    pub runtime_epoch: String,
    pub expected_browser_debugger_id: String,
}

impl BrowserRuntimeConfig {
    pub fn validate(&self) -> Result<()> {
        for value in [&self.source_id,&self.runtime_epoch] {
            anyhow::ensure!(uuid::Uuid::parse_str(value).is_ok_and(|u|u.to_string()==*value), "invalid browser source or epoch");
        }
        anyhow::ensure!(!self.world_id.trim().is_empty() && self.world_id==self.world_id.trim() && self.world_id.len()<=200, "invalid browser World");
        let u=url::Url::parse(&self.cdp_origin).map_err(|_|anyhow::anyhow!("invalid browser CDP origin"))?;
        let port=u.port().context("explicit browser port required")?;
        anyhow::ensure!(port>1024 && self.cdp_origin==format!("http://127.0.0.1:{port}"), "invalid browser CDP origin");
        anyhow::ensure!(!self.expected_browser_debugger_id.is_empty() && self.expected_browser_debugger_id.len()<=128
            && self.expected_browser_debugger_id.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'-'||b==b'_'),"invalid browser runtime guard");
        if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some(){anyhow::ensure!(port!=9222 && port!=8765,"unsafe isolated browser port");}
        Ok(())
    }
}

struct BrowserBinding { instance_id:String, secret:[u8;32], runtime:BrowserRuntimeConfig }
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserEntry {
    provider:String, instance_id:String, instance_token:String, source_id:String, world_id:String,
    cdp_origin:String, runtime_epoch:String, expected_browser_debugger_id:String,
}
pub(crate) struct BrowserAuthority { pub instance_id:String, pub runtime:BrowserRuntimeConfig }
pub(crate) struct VerifiedBrowserAdmission { pub authority:BrowserAuthority, pub observation:Observation }

fn browser_digest(parts:&[&str])->String {
    let mut encoded=Vec::new();for p in parts {encoded.extend_from_slice(&(p.len() as u32).to_be_bytes());encoded.extend_from_slice(p.as_bytes());}
    ring::digest::digest(&ring::digest::SHA256,&encoded).as_ref().iter().map(|b|format!("{b:02x}")).collect()
}
pub fn reserved_browser_pin(id:&str)->bool {
    ["observation-instance:browser:","observation-source:browser:","observation-runtime:browser:"].iter().any(|p|id.starts_with(p))
}
impl BrowserAuthority {
    pub(crate) fn pins(&self)->[(String,String);3] {
        let r=&self.runtime;
        let hash=browser_digest(&["browser-observation-v1","browser",&self.instance_id,&r.source_id,&r.world_id,&r.cdp_origin]);
        [(format!("observation-instance:browser:{}",self.instance_id),hash.clone()),
         (format!("observation-source:browser:{}",r.source_id),hash),
         (format!("observation-runtime:browser:{}:{}",r.source_id,r.runtime_epoch),browser_digest(&["browser-runtime-v1","browser",&self.instance_id,&r.source_id,&r.world_id,&r.cdp_origin,&r.runtime_epoch,&r.expected_browser_debugger_id]))]
    }
}
#[derive(Debug,Clone)]
pub struct BrowserTabIdentity {pub object_id:String,pub external_id:String,pub observation_id:String,pub event_id:String}
pub fn browser_tab_identity(instance:&str,r:&BrowserRuntimeConfig,target:&str)->Result<BrowserTabIdentity> {
    r.validate()?;
    anyhow::ensure!(!instance.is_empty()&&instance.len()<=64&&instance.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'-'||b==b'_'),"invalid browser instance");
    anyhow::ensure!(!target.is_empty()&&target.len()<=256&&target.bytes().all(|b|b.is_ascii_graphic()),"invalid browser target");
    let d=browser_digest(&["spaceos/browser-tab/v1","browser",instance,&r.source_id,&r.runtime_epoch,target]);
    Ok(BrowserTabIdentity{object_id:format!("browser-tab-v1-{d}"),external_id:format!("browser-tab-v1:{d}"),observation_id:format!("browser-tab-created-v1:{d}"),event_id:format!("browser-tab-created-event-v1:{d}")})
}
pub fn sanitized_browser_url(raw:&str)->Result<String> {
    anyhow::ensure!(raw.len()<=8192 && !raw.chars().any(char::is_control),"invalid browser URL");
    let mut u=url::Url::parse(raw).map_err(|_|anyhow::anyhow!("invalid browser URL"))?;
    anyhow::ensure!((u.scheme()=="http"||u.scheme()=="https")&&u.host_str().is_some()||raw=="about:blank","unsupported browser URL");
    anyhow::ensure!(u.username().is_empty()&&u.password().is_none(),"browser URL userinfo denied");
    u.set_query(None);u.set_fragment(None);let s=u.to_string();anyhow::ensure!(s.len()<=4096,"browser URL too long");Ok(s)
}
pub fn browser_tab_observation(instance:&str,r:&BrowserRuntimeConfig,target:&str,raw_url:&str,title:Option<&str>)->Result<Observation> {
    let id=browser_tab_identity(instance,r,target)?;let url=sanitized_browser_url(raw_url)?;
    anyhow::ensure!(title.is_none_or(|t|t.len()<=1024),"browser title too long");
    let props=serde_json::json!({"source_id":r.source_id,"instance_id":instance,"runtime_epoch":r.runtime_epoch,"target_id":target,"target_type":"page","url":url,"title":title,"world_id":r.world_id});
    let mut object=crate::Object::new("BrowserTab","browser",props.clone());object.id=id.object_id.clone();object.external_id=Some(id.external_id);
    let mut payload=props;payload["tab_id"]=serde_json::json!(object.id);
    let mut event=crate::Event::new("BrowserTabCreated","browser",payload);event.id=id.event_id.clone();event.correlation_id=Some(id.event_id);event.provider_id=Some("browser".into());event.world_id=r.world_id.clone();event.object_ids=vec![object.id.clone()];
    Ok(Observation{observation_id:id.observation_id,world_id:r.world_id.clone(),facts:vec![],objects:vec![object],relations:vec![],delete_ids:vec![],event})
}
impl ObservationBindings {
    pub fn configure_browser(&mut self,raw:&str)->Result<()> {
        let entries:Vec<BrowserEntry>=serde_json::from_str(raw).map_err(|_|anyhow::anyhow!("invalid browser binding JSON"))?;
        let mut bindings:Vec<BrowserBinding>=Vec::new();
        for e in entries {
            anyhow::ensure!(e.provider=="browser","unsupported browser binding provider");
            let runtime=BrowserRuntimeConfig{source_id:e.source_id,world_id:e.world_id,cdp_origin:e.cdp_origin,runtime_epoch:e.runtime_epoch,expected_browser_debugger_id:e.expected_browser_debugger_id};
            browser_tab_identity(&e.instance_id,&runtime,"validation")?;
            let secret:[u8;32]=URL_SAFE_NO_PAD.decode(e.instance_token).map_err(|_|anyhow::anyhow!("invalid browser instance credential"))?.try_into().map_err(|_|anyhow::anyhow!("invalid browser instance credential"))?;
            anyhow::ensure!(!bindings.iter().any(|b|b.instance_id==e.instance_id||b.runtime.source_id==runtime.source_id||b.runtime.cdp_origin==runtime.cdp_origin||bool::from(b.secret.ct_eq(&secret)))
                && !self.filesystem.iter().any(|b|bool::from(b.secret.ct_eq(&secret))),"ambiguous browser binding or credential substitution");
            bindings.push(BrowserBinding{instance_id:e.instance_id,secret,runtime});
        }
        self.browser=bindings;self.browser_registered.lock().unwrap().clear();Ok(())
    }
    pub(crate) fn browser_authority(&self,instance:&str,token:&str)->Result<BrowserAuthority> {
        let decoded=URL_SAFE_NO_PAD.decode(token).unwrap_or_default();
        let binding=self.browser.iter().find(|b|b.instance_id==instance&&bool::from(b.secret.as_slice().ct_eq(decoded.as_slice()))).context("unknown or unauthenticated browser instance")?;
        Ok(BrowserAuthority{instance_id:instance.into(),runtime:binding.runtime.clone()})
    }
    pub(crate) fn register_browser(&self,instance:&str){self.browser_registered.lock().unwrap().insert(instance.into());}
    pub(crate) fn browser_action_authority(&self, world: &str) -> Result<BrowserAuthority> {
        let matches: Vec<_> = self.browser.iter().filter(|b| b.runtime.world_id == world).collect();
        anyhow::ensure!(matches.len() == 1, "missing or ambiguous Browser source");
        let b = matches[0];
        anyhow::ensure!(self.browser_registered.lock().unwrap().contains(&b.instance_id), "Browser instance not registered");
        Ok(BrowserAuthority { instance_id: b.instance_id.clone(), runtime: b.runtime.clone() })
    }
    pub(crate) fn credential_matches_instance(&self, encoded: &str) -> bool {
        let Ok(decoded) = URL_SAFE_NO_PAD.decode(encoded) else { return false; };
        self.browser.iter().any(|b|bool::from(b.secret.as_slice().ct_eq(&decoded)))
            || self.filesystem.iter().any(|b|bool::from(b.secret.as_slice().ct_eq(&decoded)))
    }
    pub(crate) fn verify_browser_tab_created(&self,instance:&str,token:&str,mut obs:Observation)->Result<VerifiedBrowserAdmission> {
        let authority=self.browser_authority(instance,token)?;
        anyhow::ensure!(self.browser_registered.lock().unwrap().contains(instance),"browser instance not registered");
        anyhow::ensure!(obs.event.event_type=="BrowserTabCreated"&&obs.objects.len()==1&&obs.facts.is_empty()&&obs.relations.is_empty()&&obs.delete_ids.is_empty(),"unsupported browser observation shape");
        let object=&obs.objects[0];let p=&object.properties;
        let expected=browser_tab_observation(instance,&authority.runtime,p["target_id"].as_str().unwrap_or(""),p["url"].as_str().context("invalid browser URL")?,
            if p["title"].is_null(){None}else{Some(p["title"].as_str().context("invalid browser title")?)})?;
        let mut normalized=p.clone();normalized["url"]=expected.objects[0].properties["url"].clone();
        let mut raw_payload=p.clone();raw_payload["tab_id"]=serde_json::json!(object.id);
        anyhow::ensure!(normalized==expected.objects[0].properties&&object.id==expected.objects[0].id&&object.external_id==expected.objects[0].external_id
            &&object.provider_id=="browser"&&object.r#type=="BrowserTab"&&obs.world_id==authority.runtime.world_id
            &&obs.observation_id==expected.observation_id&&obs.event.id==expected.event.id&&obs.event.correlation_id==expected.event.correlation_id
            &&obs.event.actor=="browser"&&obs.event.provider_id.as_deref()==Some("browser")&&obs.event.object_ids==expected.event.object_ids
            &&obs.event.world_id==authority.runtime.world_id&&obs.event.payload==raw_payload,"browser identity, World or metadata mismatch");
        obs.objects[0].properties=expected.objects[0].properties.clone();obs.event.payload=expected.event.payload;
        anyhow::ensure!(serde_json::to_vec(&obs)?.len()<=16384,"browser observation too large");
        Ok(VerifiedBrowserAdmission{authority,observation:obs})
    }
}
