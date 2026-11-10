use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::value::RawValue;

use crate::client::{Client, Tuning};
use crate::event::Event;
use crate::send::{Action, Outcome, classify};
use crate::tests::{Fake, config, normalize, props};

fn fixture<T: DeserializeOwned>(name: &str) -> T {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../conformance")
        .join(name);
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

#[derive(Deserialize)]
struct EventCase {
    name: String,
    event: Event,
    #[serde(default)]
    props_size: usize,
    valid: bool,
}

/// Keeps each value as raw JSON so that the unpaired surrogates in the cases
/// reach validate. A Rust string can't hold them.
fn filter_raw(raw: &RawValue) -> Result<Option<Box<RawValue>>, serde_json::Error> {
    let values: BTreeMap<String, Box<RawValue>> = serde_json::from_str(raw.get())?;
    let allowed: HashSet<&str> = ["p", "p\0", "cold"].into();
    let kept: BTreeMap<_, _> = values
        .into_iter()
        .filter(|(k, _)| allowed.contains(k.as_str()))
        .collect();
    if kept.is_empty() {
        return Ok(None);
    }
    serde_json::to_string(&kept)
        .and_then(RawValue::from_string)
        .map(Some)
}

#[test]
fn conformance_events() {
    let cases: Vec<EventCase> = fixture("events.json");
    assert!(!cases.is_empty());
    for case in cases {
        let EventCase {
            name,
            mut event,
            props_size,
            valid,
        } = case;
        if props_size > 0 {
            let filler = "a".repeat(props_size - r#"{"p":""}"#.len());
            event.props = Some(RawValue::from_string(format!(r#"{{"p":"{filler}"}}"#)).unwrap());
        }
        let got = match event.props.take().map(|raw| filter_raw(&raw)).transpose() {
            Ok(filtered) => {
                event.props = filtered.flatten();
                event.validate().is_ok()
            }
            Err(_) => false,
        };
        assert_eq!(got, valid, "{name}");
    }
}

#[derive(Deserialize)]
struct ResponseCase {
    name: String,
    status: u16,
    retry_after: Option<String>,
    body: String,
    action: Action,
    #[serde(default)]
    index: i64,
    #[serde(default)]
    wait_seconds: u64,
}

#[test]
fn conformance_responses() {
    let cases: Vec<ResponseCase> = fixture("responses.json");
    assert!(!cases.is_empty());
    for case in cases {
        let got = classify(
            case.status,
            case.retry_after.as_deref(),
            case.body.as_bytes(),
        );
        let want = Outcome {
            action: case.action,
            index: case.index,
            wait: Duration::from_secs(case.wait_seconds),
        };
        assert_eq!(got, want, "{}", case.name);
    }
}

// The client must produce the body in batch.json, which the server's test
// posts to the real handler.
#[test]
fn conformance_batch() {
    let want: Value = fixture("batch.json");

    let fake = Fake::new();
    let (mut cfg, _dir) = config(&fake);
    cfg.app_version = "1.4.0".into();
    cfg.platform = "macos".into();
    cfg.os_version = "15.1".into();
    cfg.allow_props = vec!["cold".into()];
    let ids = AtomicUsize::new(0);
    let tuning = Tuning {
        new_id: Arc::new(move || Ok(format!("id-{}", ids.fetch_add(1, Ordering::Relaxed) + 1))),
        now: Arc::new(|| UNIX_EPOCH + Duration::from_secs(1_794_139_140)),
        ..crate::tests::tuning()
    };
    let client = Client::with_tuning(cfg, tuning).unwrap();

    client.set_user_id("tester@example.com");
    client.start_session().unwrap();
    client
        .track_value("sync.done", 812.0, props(&[("cold", Value::Bool(true))]))
        .unwrap();
    client.close().unwrap();

    let body = fake.wait(1).remove(0);
    let got: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(normalize(got), normalize(want));
}
