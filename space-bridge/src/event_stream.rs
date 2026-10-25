//! Durable cursor discipline and bounded, payload-free operational diagnostics.
use space_core::Event;
use std::time::{Duration, Instant};

pub fn query_failure(error: &anyhow::Error) -> &'static str {
    let description = error.to_string();
    if description.contains("denied") || description.contains("registrar") || description.contains("configured") {
        "authorization_failure"
    } else { "query_failure" }
}

pub struct EventPump {
    pub cursor: u64,
    last_log: Option<(&'static str, Instant)>,
}
impl EventPump {
    pub fn initialize(result: anyhow::Result<Vec<Event>>) -> anyhow::Result<Self> {
        let events = result?;
        Ok(Self { cursor: events.last().map_or(0, |e| e.seq), last_log: None })
    }
    pub fn poll(&mut self, result: anyhow::Result<Vec<Event>>, mut send: impl FnMut(Event) -> bool) -> Result<(), &'static str> {
        let events = result.map_err(|e| query_failure(&e))?;
        let mut previous = self.cursor;
        for event in &events {
            if event.seq <= previous { return Err("query_failure"); }
            previous = event.seq;
        }
        for event in events {
            let seq = event.seq;
            if (event.payload.get("semantic_operation").is_some()
                || matches!(event.event_type.as_str(), "FileCreated" | "FileChanged" | "BrowserTabCreated")) && !send(event) {
                return Err("delivery_failure");
            }
            self.cursor = seq;
        }
        Ok(())
    }
    pub fn should_log(&mut self, class: &'static str) -> bool {
        let now = Instant::now();
        if self.last_log.is_some_and(|(_, at)| now.duration_since(at) < Duration::from_secs(30)) { return false; }
        self.last_log = Some((class, now)); true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(seq: u64) -> Event { let mut e=Event::new("ObjectCreated","fixture",serde_json::json!({"semantic_operation":"create_semantic_object"})); e.seq=seq; e }
    #[test]
    fn initialization_denial_is_not_zero_cursor() {
        assert!(EventPump::initialize(Err(anyhow::anyhow!("Core event read denied"))).is_err());
        assert_eq!(EventPump::initialize(Ok(vec![])).unwrap().cursor, 0);
    }
    #[test]
    fn failed_query_preserves_cursor_and_recovery_delivers_once_in_order() {
        let mut p=EventPump::initialize(Ok(vec![event(4)])).unwrap();
        assert_eq!(p.poll(Err(anyhow::anyhow!("Core event read denied")), |_| panic!()),Err("authorization_failure"));
        assert_eq!(p.cursor,4);
        assert_eq!(p.poll(Err(anyhow::anyhow!("temporary transport outage")), |_| panic!()),Err("query_failure"));
        let mut delivered=vec![];
        p.poll(Ok(vec![event(5),event(7)]), |e| {delivered.push(e.seq);true}).unwrap();
        assert_eq!(delivered,vec![5,7]); assert_eq!(p.cursor,7);
        assert_eq!(p.poll(Ok(vec![event(7)]), |_| panic!()),Err("query_failure"));
        assert_eq!(p.cursor,7);
    }
    #[test]
    fn delivery_failure_and_log_rate_are_bounded() {
        let mut p=EventPump::initialize(Ok(vec![event(1)])).unwrap();
        assert_eq!(p.poll(Ok(vec![event(2)]), |_| false),Err("delivery_failure")); assert_eq!(p.cursor,1);
        assert!(p.should_log("query_failure"));
        for _ in 0..100 {assert!(!p.should_log("authorization_failure"));}
    }
    #[test]
    fn provider_creation_and_change_events_deliver_without_semantic_marker() {
        let mut p=EventPump::initialize(Ok(vec![])).unwrap();
        let rows=["FileCreated","FileChanged","BrowserTabCreated","UnrelatedDiagnostic"].into_iter().enumerate()
            .map(|(i,kind)|{let mut e=Event::new(kind,"provider",serde_json::json!({}));e.seq=i as u64+1;e}).collect();
        let mut sent=vec![];p.poll(Ok(rows),|e|{sent.push(e.event_type);true}).unwrap();
        assert_eq!(sent,vec!["FileCreated","FileChanged","BrowserTabCreated"]);assert_eq!(p.cursor,4);
        p.poll(Ok(vec![]),|_|panic!("empty query cannot notify")).unwrap();assert_eq!(p.cursor,4);
    }
    #[test]
    fn provider_delivery_failure_preserves_cursor_for_retry() {
        for kind in ["FileChanged","BrowserTabCreated"] {
            let mut p=EventPump::initialize(Ok(vec![])).unwrap();let mut e=Event::new(kind,"provider",serde_json::json!({}));e.seq=1;
            assert_eq!(p.poll(Ok(vec![e.clone()]),|_|false),Err("delivery_failure"));assert_eq!(p.cursor,0);
            p.poll(Ok(vec![e]),|_|true).unwrap();assert_eq!(p.cursor,1);
        }
    }
}
