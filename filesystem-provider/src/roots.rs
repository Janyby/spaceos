//! Allowed filesystem roots + path validation (server-side gate).
//!
//! Roots policy (secure by default):
//! - Effective roots = `SPACEOS_ALLOWED_ROOTS` entries (`;`-separated)
//!   plus the daemon's own serve root (if usable). Only paths inside
//!   those roots are served: `Restricted(roots)`.
//! - With neither configured, the daemon is fail-closed (`Restricted([])`:
//!   every path is refused) — there is no silent open mode.
//! - `SPACEOS_ALLOW_LEGACY_OPEN=1` explicitly opts into the legacy
//!   `Unrestricted` mode (isolated tests / conscious compat only).
//!
//! Both sides of every comparison are canonicalized first, so mixed
//! separators, junctions, symlinks and UNC forms cannot smuggle a path
//! past the prefix check. Check and use stay adjacent in the caller;
//! TOCTOU races on a local machine are out of scope and documented.

use anyhow::{Context, Result};
use std::path::{Component, Path, PathBuf};

/// Effective roots policy of one daemon instance.
#[derive(Debug, Clone)]
pub enum RootsPolicy {
    /// No path restriction: explicit legacy/test mode only, set via
    /// `SPACEOS_ALLOW_LEGACY_OPEN=1`. Never the default.
    Unrestricted,
    /// Only paths under one of these canonical roots are served.
    /// An empty vec denies everything (fail-closed misconfiguration).
    Restricted(Vec<PathBuf>),
}

/// Build the policy from environment + the daemon's serve root.
pub fn effective_roots(serve_root: Option<&Path>) -> RootsPolicy {
    let mut roots: Vec<PathBuf> = Vec::new();
    if let Ok(list) = std::env::var("SPACEOS_ALLOWED_ROOTS") {
        for part in list.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            match canonical(part) {
                Ok(c) => roots.push(c),
                Err(e) => tracing::warn!("ignoring unusable allowed root {part:?}: {e:#}"),
            }
        }
    }
    if let Some(root) = serve_root {
        match canonical(root) {
            Ok(c) => {
                if !roots.iter().any(|r| r == &c) {
                    roots.push(c);
                }
            }
            Err(e) => tracing::warn!(
                "serve root not usable as allowed root {:?}: {e:#}",
                root.to_string_lossy()
            ),
        }
    }
    if roots.is_empty() {
        if std::env::var("SPACEOS_ALLOW_LEGACY_OPEN")
            .map(|v| v == "1")
            .unwrap_or(false)
        {
            tracing::warn!("roots: explicit legacy unrestricted mode");
            return RootsPolicy::Unrestricted;
        }
        tracing::warn!("roots: no usable roots configured, denying all paths");
    }
    RootsPolicy::Restricted(roots)
}

/// Human-readable roots for the `roots` provider method / dashboard.
pub fn describe(policy: &RootsPolicy) -> serde_json::Value {
    match policy {
        RootsPolicy::Unrestricted => serde_json::json!({"mode": "unrestricted"}),
        RootsPolicy::Restricted(roots) => serde_json::json!({
            "mode": "restricted",
            "roots": roots.iter().map(|r| r.to_string_lossy()).collect::<Vec<_>>(),
        }),
    }
}

fn canonical(p: impl AsRef<Path>) -> Result<PathBuf> {
    std::fs::canonicalize(p.as_ref())
        .with_context(|| format!("resolving allowed root {:?}", p.as_ref().to_string_lossy()))
}

/// Reject `..` lexically: even before canonicalization, an escape attempt
/// is an explicit error, not something to normalize away silently.
fn no_escape(raw: &str) -> Result<()> {
    let has_parent = Path::new(raw)
        .components()
        .any(|c| matches!(c, Component::ParentDir));
    if has_parent {
        anyhow::bail!("path escapes allowed root: {raw:?}");
    }
    if raw.trim().is_empty() {
        anyhow::bail!("empty path");
    }
    Ok(())
}

fn contained(canonical_path: &Path, roots: &[PathBuf]) -> bool {
    // Windows paths are case-insensitive; compare lowered.
    let p = canonical_path.to_string_lossy().to_lowercase();
    roots.iter().any(|r| {
        let r = r.to_string_lossy().to_lowercase();
        p == r || p.starts_with(&(r + std::path::MAIN_SEPARATOR_STR))
    })
}

/// Validate an existing path (read/write/delete/scan/stat targets).
pub fn check_path(policy: &RootsPolicy, raw: &str) -> Result<PathBuf> {
    no_escape(raw)?;
    let canon = std::fs::canonicalize(raw).with_context(|| format!("resolving path {raw:?}"))?;
    match policy {
        RootsPolicy::Unrestricted => Ok(canon),
        RootsPolicy::Restricted(roots) => {
            if contained(&canon, roots) {
                Ok(canon)
            } else {
                anyhow::bail!("path outside allowed roots: {raw:?}")
            }
        }
    }
}

/// Validate a not-yet-existing path (create target): canonicalize the
/// parent directory, then re-attach the file name.
pub fn check_new_path(policy: &RootsPolicy, raw: &str) -> Result<PathBuf> {
    no_escape(raw)?;
    let p = Path::new(raw);
    let parent = p.parent().filter(|x| !x.as_os_str().is_empty());
    let name = p
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("missing file name in {raw:?}"))?;
    let canon_parent = match parent {
        Some(par) => {
            std::fs::canonicalize(par).with_context(|| format!("resolving parent of {raw:?}"))?
        }
        None => std::env::current_dir().context("resolving current dir")?,
    };
    match policy {
        RootsPolicy::Unrestricted => Ok(canon_parent.join(name)),
        RootsPolicy::Restricted(roots) => {
            if contained(&canon_parent, roots) {
                Ok(canon_parent.join(name))
            } else {
                anyhow::bail!("path outside allowed roots: {raw:?}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_escape_rejected_lexically() {
        let policy = RootsPolicy::Restricted(vec![PathBuf::from(r"C:\spaceos\data")]);
        assert!(check_path(&policy, r"C:\spaceos\data\..\secret.txt").is_err());
        assert!(check_new_path(&policy, r"C:\spaceos\data\..\x.txt").is_err());
        assert!(check_path(&policy, "").is_err());
    }

    #[test]
    fn unrestricted_allows_anything_resolvable() {
        let policy = RootsPolicy::Unrestricted;
        // This source file itself must resolve (relative to the crate dir).
        let me = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/roots.rs");
        assert!(check_path(&policy, &me.to_string_lossy()).is_ok());
    }

    #[test]
    fn effective_roots_secure_by_default() {
        // Save/restore: env is process-global, keep it sequential-safe.
        let saved_allowed = std::env::var("SPACEOS_ALLOWED_ROOTS").ok();
        let saved_legacy = std::env::var("SPACEOS_ALLOW_LEGACY_OPEN").ok();
        std::env::remove_var("SPACEOS_ALLOWED_ROOTS");
        std::env::remove_var("SPACEOS_ALLOW_LEGACY_OPEN");
        let root = std::env::temp_dir().join("spaceos-roots-default");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        // No env + usable serve root -> restricted to it (not open).
        match effective_roots(Some(&root)) {
            RootsPolicy::Restricted(rs) => assert_eq!(rs.len(), 1),
            RootsPolicy::Unrestricted => panic!("default must be restricted"),
        }
        // Nothing configured at all -> fail-closed deny-all.
        match effective_roots(None) {
            RootsPolicy::Restricted(rs) => assert!(rs.is_empty()),
            RootsPolicy::Unrestricted => panic!("must be fail-closed"),
        }
        // Explicit legacy flag only.
        std::env::set_var("SPACEOS_ALLOW_LEGACY_OPEN", "1");
        assert!(matches!(effective_roots(None), RootsPolicy::Unrestricted));
        // Restore.
        match saved_allowed {
            Some(v) => std::env::set_var("SPACEOS_ALLOWED_ROOTS", v),
            None => std::env::remove_var("SPACEOS_ALLOWED_ROOTS"),
        }
        match saved_legacy {
            Some(v) => std::env::set_var("SPACEOS_ALLOW_LEGACY_OPEN", v),
            None => std::env::remove_var("SPACEOS_ALLOW_LEGACY_OPEN"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unc_and_missing_paths_rejected() {
        let policy = RootsPolicy::Restricted(vec![PathBuf::from(r"C:\spaceos\data")]);
        // UNC path to nowhere: unresolvable -> denied, not normalized open.
        assert!(check_path(&policy, r"\\no-such-host\share\x.txt").is_err());
        // Missing local path: unresolvable -> denied.
        assert!(check_path(&policy, r"C:\no-such-dir-zzz\a.txt").is_err());
        // Mixed separators still contained when inside.
        let root = std::env::temp_dir().join("spaceos-roots-mixed");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("sub")).unwrap();
        let policy = RootsPolicy::Restricted(vec![canonical(&root).unwrap()]);
        let mixed = root.to_string_lossy().replace('\\', "/") + "/sub";
        assert!(check_path(&policy, &mixed).is_ok());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn restricted_containment() {
        let root = std::env::temp_dir().join("spaceos-roots-test");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("sub")).unwrap();
        let policy = RootsPolicy::Restricted(vec![canonical(&root).unwrap()]);
        // Inside: ok.
        assert!(check_path(&policy, &root.join("sub").to_string_lossy()).is_ok());
        // Outside: denied.
        assert!(check_path(&policy, &std::env::temp_dir().to_string_lossy()).is_err());
        // New file inside: ok; outside: denied.
        assert!(check_new_path(&policy, &root.join("sub").join("n.txt").to_string_lossy()).is_ok());
        assert!(check_new_path(
            &policy,
            &std::env::temp_dir().join("n.txt").to_string_lossy()
        )
        .is_err());
        let _ = std::fs::remove_dir_all(&root);
    }
}
