//! V1 world model tests against real Storage.
use space_core::query::{EventQuery, ObjectQuery, RelationQuery};
use space_core::{Event, Object, Relation, Space, Storage};

fn temp_db(name: &str) -> String {
    let dir = std::env::temp_dir().join("spaceos-v1-tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{name}.db")).to_string_lossy().to_string()
}

fn storage(name: &str) -> Storage {
    let p = temp_db(name);
    let _ = std::fs::remove_file(&p);
    Storage::open(&p).unwrap()
}

#[test]
fn test1_create_space() {
    let s = storage("t1");
    let space = Space::new("SpaceOS Development", serde_json::json!({}));
    assert!(s.create_space(&space).is_ok());
    let got = s.get_space(&space.id).unwrap().unwrap();
    assert_eq!(got.name, "SpaceOS Development");
    assert_eq!(s.list_spaces().unwrap().len(), 1);
}

#[test]
fn test2_create_object() {
    let s = storage("t2");
    let o = Object::new(
        "Project",
        "agent-demo",
        serde_json::json!({"name": "SpaceOS V1"}),
    );
    s.create_object(&o).unwrap();
    assert!(s.get_object(&o.id).unwrap().is_some());
}

#[test]
fn test3_create_relation() {
    let s = storage("t3");
    // Phase B: FK constraints require both endpoints to exist.
    for id in ["a", "b"] {
        let mut o = Object::new("Node", "v1", serde_json::json!({}));
        o.id = id.to_string();
        s.create_object(&o).unwrap();
    }
    let r = Relation::new("a", "works_on", "b");
    s.create_relation(&r).unwrap();
    let got = s.get_relation(&r.id).unwrap().unwrap();
    assert_eq!(got.relation_type, "works_on");
}

#[test]
fn test4_create_agent() {
    let s = storage("t4");
    let mut a = Object::new(
        "Agent",
        "agent-demo",
        serde_json::json!({"name": "SpaceOS Demo Agent"}),
    );
    a.properties["status"] = serde_json::json!("running");
    a.properties["mission"] = serde_json::json!("Prepare result");
    a.properties["capabilities"] = serde_json::json!(["create_object"]);
    s.create_object(&a).unwrap();
    let got = s.get_object(&a.id).unwrap().unwrap();
    assert_eq!(got.properties["status"], "running");
}

#[test]
fn test5_agent_creates_result() {
    let s = storage("t5");
    let agent = Object::new("Agent", "agent-demo", serde_json::json!({"name": "A"}));
    let result = Object::new(
        "Result",
        "agent-demo",
        serde_json::json!({"name": "V1 Result"}),
    );
    s.create_object(&agent).unwrap();
    s.create_object(&result).unwrap();
    let rel = Relation::new(&agent.id, "produces", &result.id);
    s.create_relation(&rel).unwrap();
    let objs = s
        .query_objects(&ObjectQuery {
            r#type: Some("Result".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(objs.len(), 1);
    assert_eq!(objs[0].id, result.id);
}

#[test]
fn test6_event_recorded() {
    let s = storage("t6");
    let e = Event::new("AgentStarted", "agent-demo", serde_json::json!({}));
    s.record_event(&e).unwrap();
    let evs = s
        .query_events(&EventQuery {
            event_type: Some("AgentStarted".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(evs.len(), 1);
}

#[test]
fn test7_query_world() {
    let s = storage("t7");
    s.create_space(&Space::new("SpaceOS Development", serde_json::json!({})))
        .unwrap();
    s.create_object(&Object::new("Project", "p", serde_json::json!({})))
        .unwrap();
    // Phase B: FK — both relation endpoints must exist.
    for id in ["x", "y"] {
        let mut o = Object::new("File", "p", serde_json::json!({}));
        o.id = id.into();
        s.create_object(&o).unwrap();
    }
    s.create_relation(&Relation::new("x", "contains", "y"))
        .unwrap();
    assert!(!s.query_objects(&ObjectQuery::default()).unwrap().is_empty());
    assert!(!s
        .query_relations(&RelationQuery::default())
        .unwrap()
        .is_empty());
    assert!(!s.list_spaces().unwrap().is_empty());
}

#[test]
fn test8_tree_graph_same_data() {
    // Both views consume the same Core world query; the contract is:
    // object/relation sets readable from Storage are identical and
    // contain exactly the relations that reference existing objects.
    let s = storage("t8");
    let agent = Object::new("Agent", "a", serde_json::json!({"name": "A"}));
    let project = Object::new("Project", "a", serde_json::json!({"name": "P"}));
    s.create_object(&agent).unwrap();
    s.create_object(&project).unwrap();
    s.create_relation(&Relation::new(&agent.id, "works_on", &project.id))
        .unwrap();
    let objs = s.query_objects(&ObjectQuery::default()).unwrap();
    let rels = s.query_relations(&RelationQuery::default()).unwrap();
    let ids: std::collections::HashSet<_> = objs.iter().map(|o| o.id.clone()).collect();
    assert!(rels
        .iter()
        .all(|r| ids.contains(&r.source_object_id) && ids.contains(&r.target_object_id)));
    assert_eq!(objs.len(), 2);
    assert_eq!(rels.len(), 1);
}

#[test]
fn test9_mutation_visible_in_world() {
    let s = storage("t9");
    let before = s.query_objects(&ObjectQuery::default()).unwrap().len();
    s.create_object(&Object::new("Result", "a", serde_json::json!({})))
        .unwrap();
    let after = s.query_objects(&ObjectQuery::default()).unwrap().len();
    assert_eq!(after, before + 1);
}

#[test]
fn test10_no_duplicate_object() {
    let s = storage("t10");
    let o = Object::new("Project", "a", serde_json::json!({"name": "SpaceOS V1"}));
    s.create_object(&o).unwrap();
    s.create_object(&o).unwrap(); // same id upsert
    let objs = s.query_objects(&ObjectQuery::default()).unwrap();
    let v1: Vec<_> = objs
        .iter()
        .filter(|x| x.properties["name"] == "SpaceOS V1")
        .collect();
    assert_eq!(v1.len(), 1);
}
