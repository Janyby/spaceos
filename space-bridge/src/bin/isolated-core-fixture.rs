//! Laboratory-only Core process for cross-process registration verification.
//! It refuses all production paths and uses only synthetic credentials.
#![cfg(windows)]

use space_core::{
    ipc,
    principal::PrincipalRegistry,
    provider_observation::ObservationBindings,
    service_registration::{ServiceRegistrarRegistry, SEMANTIC_PROFILE, WORLD_READ_PROFILE},
    semantic::{CreateSemanticObject, CreateSemanticRelation}, Object, Storage,
};
use serde_json::json;

fn i4x_fixture_path(name: &str, allowed: &std::path::Path) -> anyhow::Result<std::path::PathBuf> {
    let path = std::path::PathBuf::from(std::env::var(name)?);
    let parent = std::fs::canonicalize(path.parent().ok_or_else(|| anyhow::anyhow!("fixture path missing parent"))?)?;
    anyhow::ensure!(parent.starts_with(allowed) && parent != allowed, "unsafe I.4-X fixture path");
    Ok(path)
}

fn seed_i4x(storage: &mut Storage, world: &str, allowed: &std::path::Path) -> anyhow::Result<()> {
    anyhow::ensure!(world == "synthetic-process-world", "unsafe I.4-X World A");
    let world_b = "synthetic-process-world-b";
    anyhow::ensure!(storage.get_object("i4x-legacy-a")?.is_none(), "I.4-X database was already seeded");
    let mut ids = Vec::new();
    for (scope, world_id) in [("a", world), ("b", world_b)] {
        let mut pair = Vec::new();
        for index in 1..=2 {
            let created = storage.create_semantic_object(&CreateSemanticObject {
                world_id: world_id.into(), object_type: "Note".into(),
                properties: json!({"name": format!("I4X-{scope}-{index}")}),
                correlation_id: format!("i4x-{scope}-object-{index}"),
            })?;
            pair.push(created.object.id);
        }
        storage.create_semantic_relation(&CreateSemanticRelation {
            world_id: world_id.into(), source_id: pair[0].clone(), target_id: pair[1].clone(),
            relation_type: "related_to".into(), properties: json!({}),
            correlation_id: format!("i4x-{scope}-relation"),
        })?;
        ids.push((scope, world_id, pair));
    }
    for (id, world_id) in [("i4x-provider-a", world), ("i4x-provider-b", world_b)] {
        let mut object = Object::new("File", "filesystem", json!({"world_id": world_id, "name": id}));
        object.id = id.into();
        object.external_id = Some(format!("i4x://{id}"));
        storage.create_object(&object)?;
    }
    for id in ["i4x-legacy-a", "i4x-legacy-private"] {
        let mut object = Object::new("Note", "filesystem", json!({"name": id}));
        object.id = id.into();
        storage.create_object(&object)?;
    }
    let public_path = i4x_fixture_path("SPACEOS_FIXTURE_I4X_PUBLIC", allowed)?;
    std::fs::write(public_path, serde_json::to_vec_pretty(&json!({
        "marker":"SPACEOS-I4X-SYNTHETIC", "world_a":world, "world_b":world_b,
        "semantic_objects": ids, "provider_objects":["i4x-provider-a","i4x-provider-b"],
        "approved_legacy":"i4x-legacy-a", "unapproved_legacy":"i4x-legacy-private"
    }))?)?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let root = std::env::var("SPACEOS_TEST_ISOLATION_ROOT")?;
    anyhow::ensure!(
        root == r"C:\spaceos",
        "fixture outside lab"
    );
    let pipe = std::env::var("SPACEOS_FIXTURE_CORE_PIPE")?;
    anyhow::ensure!(
        pipe.starts_with(r"\\.\pipe\spaceos-4-3a4-process-"),
        "unsafe fixture pipe"
    );
    let db = std::path::PathBuf::from(std::env::var("SPACEOS_FIXTURE_DB")?);
    let allowed = std::fs::canonicalize(format!(r"{root}\test-tmp"))?;
    let parent = std::fs::canonicalize(
        db.parent()
            .ok_or_else(|| anyhow::anyhow!("fixture database has no parent"))?,
    )?;
    anyhow::ensure!(parent.starts_with(&allowed), "unsafe fixture database");
    let world = std::env::var("SPACEOS_FIXTURE_WORLD")?;
    anyhow::ensure!(world == "synthetic-process-world", "unsafe fixture world");
    anyhow::ensure!(
        std::env::var_os("SPACEOS_CORE_TOKEN").is_some(),
        "missing synthetic transport token"
    );
    let full_stack = std::env::var("SPACEOS_FIXTURE_FULL_STACK").ok().as_deref() == Some("1");
    let mut principals = PrincipalRegistry::default();
    let filesystem_pipe = if full_stack {
        ipc::configured_provider_pipe("SPACEOS_FILESYSTEM_PROVIDER_PIPE", ipc::PROVIDER_PIPE)?
    } else {
        format!(r"\\.\pipe\spaceos-4-3a4-fs-unused-{}", std::process::id())
    };
    let browser_pipe = if full_stack {
        ipc::configured_provider_pipe("SPACEOS_BROWSER_PROVIDER_PIPE", ipc::BROWSER_PROVIDER_PIPE)?
    } else {
        format!(
            r"\\.\pipe\spaceos-4-3a4-browser-unused-{}",
            std::process::id()
        )
    };
    if full_stack {
        principals
            .insert(
                "filesystem",
                [92u8; 32],
                [
                    "provider.register",
                    "observation.report",
                    "event.record",
                    "object.update",
                    "filesystem.reconcile.read",
                ],
                ["synthetic-process-world", "synthetic-process-world-b"],
            )
            .map_err(anyhow::Error::msg)?;
        principals
            .insert(
                "browser",
                [93u8; 32],
                ["provider.register", "observation.report", "event.record"],
                [""],
            )
            .map_err(anyhow::Error::msg)?;
        principals
            .insert(
                "system",
                [94u8; 32],
                [
                    "agent.register",
                    "agent.grant.action.stat",
                    "agent.grant.action.get_tabs",
                    "agent.grant.semantic.read",
                    "agent.grant.event.record",
                ],
                ["synthetic-process-world", "synthetic-process-world-b"],
            )
            .map_err(anyhow::Error::msg)?;
        principals
            .insert("cli", [111u8; 32], ["semantic.read"], ["synthetic-process-world"])
            .map_err(anyhow::Error::msg)?;
    }
    let mut registrars = ServiceRegistrarRegistry::default();
    registrars
        .insert(
            SEMANTIC_PROFILE,
            [91u8; 32],
            &principals,
            &ipc::core_token(),
        )
        .map_err(anyhow::Error::msg)?;
    if std::env::var("SPACEOS_FIXTURE_READER_MODE").ok().as_deref() != Some("deny") {
    registrars
        .insert(
            WORLD_READ_PROFILE,
            [110u8; 32],
            &principals,
            &ipc::core_token(),
        )
        .map_err(anyhow::Error::msg)?;
    }
    let i4x = std::env::var("SPACEOS_FIXTURE_I4X").ok().as_deref() == Some("1");
    registrars.insert(space_core::service_registration::MCP_READ_PROFILE, [112u8;32], &principals, &ipc::core_token()).map_err(anyhow::Error::msg)?;
    let mut storage = Storage::open(&db)?;
    if i4x {
        if std::env::var("SPACEOS_FIXTURE_RESTART").ok().as_deref() == Some("1") {
            let public = i4x_fixture_path("SPACEOS_FIXTURE_I4X_PUBLIC", &allowed)?;
            let marker: serde_json::Value = serde_json::from_slice(&std::fs::read(public)?)?;
            anyhow::ensure!(marker["marker"] == "SPACEOS-I4X-SYNTHETIC" && marker["world_a"] == world
                && storage.get_object("i4x-legacy-a")?.is_some(), "restart fixture marker mismatch");
        } else { seed_i4x(&mut storage, &world, &allowed)?; }
    }
    let mut state = ipc::CoreState::new_with_semantic_world_and_identities(
        storage,
        Some(&world),
        &filesystem_pipe,
        &browser_pipe,
        principals,
        registrars,
        ipc::core_token(),
    );
    if i4x {
        state.set_isolated_legacy_read_ids(&["i4x-legacy-a"])?;
        let actor_a = state.policy.register("i4x-reader-a", "system", vec!["semantic.read".into()], vec![world.clone()])
            .ok_or_else(|| anyhow::anyhow!("I.4-X A actor registration failed"))?;
        let actor_b = state.policy.register("i4x-reader-b", "system", vec!["semantic.read".into()], vec!["synthetic-process-world-b".into()])
            .ok_or_else(|| anyhow::anyhow!("I.4-X B actor registration failed"))?;
        let insufficient = state.policy.register("i4x-no-read", "system", vec![], vec![world.clone()])
            .ok_or_else(|| anyhow::anyhow!("I.4-X insufficient actor registration failed"))?;
        let credentials = i4x_fixture_path("SPACEOS_FIXTURE_I4X_CREDENTIALS", &allowed)?;
        std::fs::write(credentials, serde_json::to_vec_pretty(&json!({
            "i4x-reader-a":actor_a, "i4x-reader-b":actor_b, "i4x-no-read":insufficient
        }))?)?;
    }
    if full_stack {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        let source = std::path::PathBuf::from(std::env::var("SPACEOS_FIXTURE_FS_ROOT")?);
        let canonical = std::fs::canonicalize(&source)?;
        anyhow::ensure!(
            canonical.starts_with(&allowed) && canonical != allowed,
            "unsafe observation source"
        );
        let mode = std::env::var("SPACEOS_FIXTURE_BINDING_MODE").unwrap_or_else(|_| "valid".into());
        anyhow::ensure!(
            matches!(mode.as_str(), "valid" | "missing" | "changed" | "rotated"),
            "unsafe fixture binding mode"
        );
        let mut bindings = Vec::new();
        if mode != "missing" {
            bindings.push(serde_json::json!({
                "provider": "filesystem", "instance_id":"fixture-fs-A",
                "instance_token":URL_SAFE_NO_PAD.encode(if mode == "rotated" { [84u8;32] } else { [81u8;32] }),
                "root": canonical,
                "world_id": if mode == "changed" { "synthetic-process-world-b" } else { world.as_str() }
            }));
        }
        if let Ok(other) = std::env::var("SPACEOS_FIXTURE_FS_ROOT_B") {
            let other = std::fs::canonicalize(other)?;
            anyhow::ensure!(
                other.starts_with(&allowed) && other != allowed,
                "unsafe second observation source"
            );
            bindings.push(serde_json::json!({
                "provider": "filesystem", "instance_id":"fixture-fs-B",
                "instance_token":URL_SAFE_NO_PAD.encode([82u8;32]),
                "root":other, "world_id":"synthetic-process-world-b"
            }));
        }
        std::sync::Arc::get_mut(&mut state)
            .unwrap()
            .observation_bindings =
            ObservationBindings::from_json(&serde_json::Value::Array(bindings).to_string())?;
    }
    ipc::run_server(state, &pipe).await
}
