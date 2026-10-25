use serde_json::json;
use space_core::{Object, ObjectContextQuery, Relation, Storage};
use std::time::{Duration, Instant};

const WORLD: &str = "stage4-performance-world";

fn fixture(object_count: usize, relation_count: usize) -> Storage {
    let storage = Storage::open(":memory:").unwrap();
    for index in 0..object_count {
        let mut object = Object::new(
            "Note",
            "performance-fixture",
            json!({"world_id": WORLD, "index": index}),
        );
        object.id = format!("object-{index:05}");
        storage.create_object(&object).unwrap();
    }
    for index in 0..relation_count {
        let neighbor = 1 + (index % (object_count - 1));
        let mut relation = if index % 4 == 0 {
            Relation::new(
                "object-00000",
                format!("type-{}", index % 7),
                format!("object-{neighbor:05}"),
            )
        } else if index % 4 == 1 {
            Relation::new(
                format!("object-{neighbor:05}"),
                format!("type-{}", index % 7),
                "object-00000",
            )
        } else {
            let other = 1 + ((index * 17 + 3) % (object_count - 1));
            Relation::new(
                format!("object-{neighbor:05}"),
                format!("type-{}", index % 7),
                format!("object-{other:05}"),
            )
        };
        relation.id = format!("relation-{index:06}");
        relation.world_id = WORLD.to_string();
        storage.create_relation(&relation).unwrap();
    }
    storage
}

fn measure(object_count: usize, relation_count: usize) -> (Duration, Duration, Duration) {
    let mut storage = fixture(object_count, relation_count);
    let query = ObjectContextQuery {
        object_id: "object-00000".to_string(),
        world_id: WORLD.to_string(),
        limit: Some(100),
    };
    let warm = storage.get_object_context(&query).unwrap().unwrap();
    assert_eq!(warm.relations.len(), 100);
    assert!(warm.truncated);

    let mut samples = Vec::with_capacity(50);
    for _ in 0..50 {
        let started = Instant::now();
        let context = storage.get_object_context(&query).unwrap().unwrap();
        samples.push(started.elapsed());
        assert_eq!(context.relations.len(), 100);
    }
    samples.sort_unstable();
    let median = samples[samples.len() / 2];
    let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)];
    let max = *samples.last().unwrap();
    (median, p95, max)
}

fn directional_fixture(
    object_count: usize,
    incoming: usize,
    outgoing: usize,
    unrelated: usize,
    parallel_neighbor: bool,
) -> Storage {
    let storage = Storage::open(":memory:").unwrap();
    for index in 0..object_count {
        let mut object = Object::new(
            "Note",
            "performance-fixture",
            json!({"world_id": WORLD, "index": index}),
        );
        object.id = format!("object-{index:05}");
        storage.create_object(&object).unwrap();
    }
    let neighbor_for = |index: usize| {
        if parallel_neighbor {
            1
        } else {
            1 + (index % (object_count - 1))
        }
    };
    let mut relation_index = 0;
    for index in 0..incoming {
        let neighbor = neighbor_for(index);
        let mut relation = Relation::new(
            format!("object-{neighbor:05}"),
            format!("incoming-{}", index % 3),
            "object-00000",
        );
        relation.id = format!("directional-{relation_index:07}");
        relation.world_id = WORLD.to_string();
        storage.create_relation(&relation).unwrap();
        relation_index += 1;
    }
    for index in 0..outgoing {
        let neighbor = neighbor_for(index);
        let mut relation = Relation::new(
            "object-00000",
            format!("outgoing-{}", index % 3),
            format!("object-{neighbor:05}"),
        );
        relation.id = format!("directional-{relation_index:07}");
        relation.world_id = WORLD.to_string();
        storage.create_relation(&relation).unwrap();
        relation_index += 1;
    }
    for index in 0..unrelated {
        let source = 1 + (index % (object_count - 1));
        let target = 1 + ((index * 17 + 3) % (object_count - 1));
        let mut relation = Relation::new(
            format!("object-{source:05}"),
            "unrelated",
            format!("object-{target:05}"),
        );
        relation.id = format!("directional-{relation_index:07}");
        relation.world_id = WORLD.to_string();
        storage.create_relation(&relation).unwrap();
        relation_index += 1;
    }
    storage
}

fn measure_existing(
    storage: &mut Storage,
    iterations: usize,
) -> (usize, bool, Duration, Duration, Duration) {
    let query = ObjectContextQuery {
        object_id: "object-00000".to_string(),
        world_id: WORLD.to_string(),
        limit: Some(100),
    };
    let warm = storage.get_object_context(&query).unwrap().unwrap();
    let relation_count = warm.relations.len();
    let truncated = warm.truncated;
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let context = storage.get_object_context(&query).unwrap().unwrap();
        samples.push(started.elapsed());
        assert_eq!(context.relations.len(), relation_count);
        assert_eq!(context.truncated, truncated);
    }
    samples.sort_unstable();
    let median = samples[samples.len() / 2];
    let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)];
    let max = *samples.last().unwrap();
    (relation_count, truncated, median, p95, max)
}

#[test]
fn bounded_context_measurements() {
    for (objects, relations) in [(100, 500), (1_000, 5_000)] {
        let (median, p95, max) = measure(objects, relations);
        println!(
            "stage4 context: objects={objects} relations={relations} limit=100 iterations=50 median_us={} p95_us={} max_us={}",
            median.as_micros(),
            p95.as_micros(),
            max.as_micros()
        );
    }
}

#[test]
fn directional_and_larger_fixture_measurements() {
    let scenarios = [
        ("zero", 100, 0, 0, 0, false, 100),
        ("few", 100, 2, 2, 100, false, 100),
        ("mostly-incoming", 2_000, 9_000, 1_000, 5_000, false, 30),
        ("mostly-outgoing", 2_000, 1_000, 9_000, 5_000, false, 30),
        ("mixed", 2_000, 5_000, 5_000, 5_000, false, 30),
        ("parallel-neighbor", 100, 5_000, 5_000, 0, true, 30),
        ("large-mixed", 10_000, 12_500, 12_500, 25_000, false, 20),
    ];
    for (name, objects, incoming, outgoing, unrelated, parallel, iterations) in scenarios {
        let mut storage = directional_fixture(objects, incoming, outgoing, unrelated, parallel);
        let (returned, truncated, median, p95, max) = measure_existing(&mut storage, iterations);
        println!(
            "stage4 context scenario={name} objects={objects} relations={} incoming={incoming} outgoing={outgoing} unrelated={unrelated} returned={returned} truncated={truncated} limit=100 iterations={iterations} median_us={} p95_us={} max_us={}",
            incoming + outgoing + unrelated,
            median.as_micros(),
            p95.as_micros(),
            max.as_micros()
        );
    }
}

#[test]
fn context_query_plan_uses_existing_endpoint_indexes() {
    let db = std::env::temp_dir().join(format!("spaceos-stage4-plan-{}.db", uuid::Uuid::new_v4()));
    drop(Storage::open(&db).unwrap());
    let connection = rusqlite::Connection::open(&db).unwrap();
    let mut statement = connection
        .prepare(
            "EXPLAIN QUERY PLAN
             WITH adjacent AS (
                SELECT id, source_object_id, relation_type, target_object_id,
                       created_at, world_id, target_object_id AS neighbor_id,
                       CASE WHEN target_object_id = source_object_id
                            THEN 'self_relation' ELSE 'outgoing' END AS direction
                  FROM relations INDEXED BY idx_relations_source
                 WHERE source_object_id = ?1 AND world_id = ?2
                UNION ALL
                SELECT id, source_object_id, relation_type, target_object_id,
                       created_at, world_id, source_object_id AS neighbor_id,
                       'incoming' AS direction
                  FROM relations INDEXED BY idx_relations_target
                 WHERE target_object_id = ?1
                   AND source_object_id <> ?1
                   AND world_id = ?2
             )
             SELECT a.id, a.source_object_id, a.relation_type,
                    a.target_object_id, a.created_at, a.world_id, a.direction,
                    n.id, n.type, n.provider_id
               FROM adjacent a
               LEFT JOIN objects n ON n.id = a.neighbor_id
              WHERE n.id IS NULL OR json_extract(n.properties, '$.world_id') = ?2
              ORDER BY a.created_at ASC, a.id ASC
              LIMIT ?3",
        )
        .unwrap();
    let details = statement
        .query_map(rusqlite::params!["center", WORLD, 101_i64], |row| {
            row.get::<_, String>(3)
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    println!("stage4 context query plan: {}", details.join(" | "));
    let joined = details.join("\n");
    assert!(joined.contains("idx_relations_source"), "{joined}");
    assert!(joined.contains("idx_relations_target"), "{joined}");
    assert!(joined.contains("sqlite_autoindex_objects_1"), "{joined}");
}
