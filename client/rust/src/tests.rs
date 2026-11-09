use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flate2::read::GzDecoder;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::client::{Client, Config, Error, MAX_BATCH_BYTES, Tuning};
use crate::event::{MAX_MESSAGE, MAX_PROPS, Props, format_time};
use crate::queue::{Queue, read_lines};
use crate::send::{Request, Response, Transport, TransportError, backoff_limit, jitter};

type Respond = Box<dyn Fn(usize, &[u8]) -> Option<(u16, String)> + Send>;

#[derive(Default)]
struct FakeState {
    bodies: Vec<Vec<u8>>,
    calls: usize,
    respond: Option<Respond>,
}

/// Fake records the bodies it accepts. respond, when set, picks the response
/// to each request by number, starting at 0; None means accept.
pub(crate) struct Fake {
    state: Mutex<FakeState>,
    changed: Condvar,
}

impl Fake {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::default(),
            changed: Condvar::new(),
        })
    }

    fn respond(&self, respond: impl Fn(usize, &[u8]) -> Option<(u16, String)> + Send + 'static) {
        self.state.lock().unwrap().respond = Some(Box::new(respond));
    }

    fn accept_all(&self) {
        self.state.lock().unwrap().respond = None;
    }

    /// Returns the accepted bodies once there are at least n.
    pub fn wait(&self, n: usize) -> Vec<Vec<u8>> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut state = self.state.lock().unwrap();
        while state.bodies.len() < n {
            let timeout = deadline.saturating_duration_since(Instant::now());
            assert!(
                !timeout.is_zero(),
                "got {} bodies, want {n}",
                state.bodies.len()
            );
            state = self.changed.wait_timeout(state, timeout).unwrap().0;
        }
        state.bodies.clone()
    }

    fn calls(&self) -> usize {
        self.state.lock().unwrap().calls
    }
}

impl Transport for Fake {
    fn post(&self, request: Request) -> Result<Response, TransportError> {
        let header = |name: &str| {
            request
                .headers
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| v.as_str())
        };
        let good_shape = request.url == "http://beacon.test/v1/events"
            && header("Authorization") == Some("Bearer key")
            && header("Content-Encoding") == Some("gzip")
            && header("Content-Type") == Some("application/json")
            && !request.timeout.is_zero();
        if !good_shape {
            return Ok(response(418, r#"{"error":"bad request shape"}"#));
        }
        let mut body = Vec::new();
        GzDecoder::new(&request.body[..]).read_to_end(&mut body)?;

        let mut state = self.state.lock().unwrap();
        let call = state.calls;
        state.calls += 1;
        let answer = state.respond.as_ref().and_then(|r| r(call, &body));
        let (status, text) = answer.unwrap_or((202, r#"{"accepted":1}"#.into()));
        if status == 202 {
            state.bodies.push(body);
        }
        self.changed.notify_all();
        Ok(response(status, &text))
    }
}

fn response(status: u16, body: &str) -> Response {
    Response {
        status,
        retry_after: None,
        body: body.as_bytes().to_vec(),
    }
}

pub(crate) fn config(fake: &Arc<Fake>) -> (Config, TempDir) {
    let dir = TempDir::new().unwrap();
    let transport: Arc<dyn Transport> = Arc::clone(fake) as Arc<dyn Transport>;
    let cfg = Config::new("http://beacon.test/", "key", dir.path(), transport);
    (cfg, dir)
}

pub(crate) fn tuning() -> Tuning {
    Tuning {
        flush_every: Duration::from_secs(3600),
        heartbeat_every: Duration::from_secs(3600),
        min_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(5),
        ..Tuning::default()
    }
}

fn start(cfg: &Config, tuning: Tuning) -> Client {
    Client::with_tuning(cfg.clone(), tuning).unwrap()
}

pub(crate) fn props(pairs: &[(&str, Value)]) -> Props {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

/// Rewrites every number as a float, since a client writes 812.0 where the
/// fixture has 812 and both are the same JSON number.
pub(crate) fn normalize(value: Value) -> Value {
    match value {
        Value::Number(n) => json!(n.as_f64().unwrap()),
        Value::Array(items) => Value::Array(items.into_iter().map(normalize).collect()),
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, normalize(v))).collect())
        }
        other => other,
    }
}

fn events(bodies: &[Vec<u8>]) -> Vec<Value> {
    bodies
        .iter()
        .flat_map(|b| {
            let batch: Value = serde_json::from_slice(b).unwrap();
            batch["events"].as_array().unwrap().clone()
        })
        .collect()
}

fn names(events: &[Value]) -> String {
    let names: Vec<_> = events.iter().map(|e| e["name"].as_str().unwrap()).collect();
    names.join(" ")
}

#[test]
fn sends_when_batch_is_full() {
    let fake = Fake::new();
    let (cfg, _dir) = config(&fake);
    let client = start(
        &cfg,
        Tuning {
            batch_events: 3,
            ..tuning()
        },
    );
    for name in ["a", "b", "c"] {
        client.track(name, Props::new()).unwrap();
    }
    assert_eq!(names(&events(&fake.wait(1))), "a b c");
}

#[test]
fn sends_on_interval() {
    let fake = Fake::new();
    let (cfg, _dir) = config(&fake);
    let client = start(
        &cfg,
        Tuning {
            flush_every: Duration::from_millis(5),
            ..tuning()
        },
    );
    client.track("a", Props::new()).unwrap();
    assert_eq!(names(&events(&fake.wait(1))), "a");
}

#[test]
fn retries_after_server_error() {
    let fake = Fake::new();
    fake.respond(|call, _| (call < 3).then(|| (500, r#"{"error":"internal error"}"#.into())));
    let (cfg, _dir) = config(&fake);
    let client = start(&cfg, tuning());
    client.track("a", Props::new()).unwrap();
    client.flush();
    assert_eq!(names(&events(&fake.wait(1))), "a");
    assert_eq!(fake.calls(), 4);
}

struct FailingTransport {
    fake: Arc<Fake>,
    remaining: AtomicU32,
}

impl Transport for FailingTransport {
    fn post(&self, request: Request) -> Result<Response, TransportError> {
        if self.remaining.fetch_sub(1, Ordering::SeqCst) > 0 {
            return Err("connection refused".into());
        }
        self.fake.post(request)
    }
}

#[test]
fn retries_after_transport_error() {
    let fake = Fake::new();
    let (mut cfg, _dir) = config(&fake);
    cfg.transport = Arc::new(FailingTransport {
        fake: Arc::clone(&fake),
        remaining: AtomicU32::new(2),
    });
    let client = start(&cfg, tuning());
    client.track("a", Props::new()).unwrap();
    client.flush();
    assert_eq!(names(&events(&fake.wait(1))), "a");
}

#[test]
fn bad_event_is_dropped_and_rest_resent() {
    let fake = Fake::new();
    fake.respond(|call, _| (call == 0).then(|| (400, r#"{"error":"bad","index":1}"#.into())));
    let (cfg, _dir) = config(&fake);
    let client = start(
        &cfg,
        Tuning {
            batch_events: 3,
            ..tuning()
        },
    );
    for name in ["a", "b", "c"] {
        client.track(name, Props::new()).unwrap();
    }
    assert_eq!(names(&events(&fake.wait(1))), "a c");
}

#[test]
fn bad_batch_is_dropped() {
    let fake = Fake::new();
    fake.respond(|call, _| (call == 0).then(|| (400, r#"{"error":"invalid JSON"}"#.into())));
    let (cfg, _dir) = config(&fake);
    let client = start(&cfg, tuning());
    client.track("a", Props::new()).unwrap();
    client.flush();
    let deadline = Instant::now() + Duration::from_secs(10);
    while client.inner.queue.oldest().is_some() {
        assert!(Instant::now() < deadline, "batch still queued");
        thread::sleep(Duration::from_millis(1));
    }
    client.track("b", Props::new()).unwrap();
    client.flush();
    assert_eq!(names(&events(&fake.wait(1))), "b");
}

#[test]
fn queue_survives_restart() {
    let fake = Fake::new();
    fake.respond(|_, _| Some((503, String::new())));
    let (cfg, _dir) = config(&fake);
    let client = start(&cfg, tuning());
    client.track("a", Props::new()).unwrap();
    client.close().unwrap();
    assert!(matches!(
        client.track("b", Props::new()),
        Err(Error::Closed)
    ));

    fake.accept_all();
    let _again = start(&cfg, tuning());
    assert_eq!(names(&events(&fake.wait(1))), "a");
}

struct HangingTransport;

impl Transport for HangingTransport {
    fn post(&self, _: Request) -> Result<Response, TransportError> {
        thread::sleep(Duration::from_secs(3));
        Err("timed out".into())
    }
}

#[test]
fn close_does_not_wait_for_a_hanging_transport() {
    let fake = Fake::new();
    let (mut cfg, _dir) = config(&fake);
    cfg.transport = Arc::new(HangingTransport);
    let client = start(
        &cfg,
        Tuning {
            close_wait: Duration::from_millis(50),
            ..tuning()
        },
    );
    client.track("a", Props::new()).unwrap();
    let started = Instant::now();
    client.close().unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn device_id_persists() {
    let fake = Fake::new();
    let (cfg, _dir) = config(&fake);
    let a = start(&cfg, tuning());
    a.close().unwrap();
    let b = start(&cfg, tuning());
    assert!(!a.device_id().is_empty());
    assert_eq!(a.device_id(), b.device_id());
}

#[test]
fn session_events() {
    let fake = Fake::new();
    let (cfg, _dir) = config(&fake);
    let seconds = Arc::new(AtomicU64::new(1_790_000_000));
    let clock = Arc::clone(&seconds);
    let client = start(
        &cfg,
        Tuning {
            heartbeat_every: Duration::from_millis(5),
            now: Arc::new(move || {
                UNIX_EPOCH + Duration::from_secs(clock.fetch_add(1, Ordering::SeqCst))
            }),
            ..tuning()
        },
    );
    client.track("before", Props::new()).unwrap();
    client.start_session().unwrap();
    client.start_session().unwrap();
    thread::sleep(Duration::from_millis(30));
    client.end_session().unwrap();
    client.track("after", Props::new()).unwrap();
    client.close().unwrap();

    let events = events(&fake.wait(1));
    let got = names(&events);
    assert!(
        got.starts_with("before session.start session.heartbeat")
            && got.ends_with("session.end after"),
        "events {got:?}"
    );
    let (start_event, end_event) = (&events[1], &events[events.len() - 2]);
    assert!(events[0].get("session_id").is_none());
    assert!(events[events.len() - 1].get("session_id").is_none());
    assert!(start_event["session_id"].is_string());
    assert_eq!(start_event["session_id"], end_event["session_id"]);
    assert!(end_event["value"].as_f64().unwrap() > 0.0);
}

#[test]
fn app_opened_once() {
    let fake = Fake::new();
    let (mut cfg, _dir) = config(&fake);
    cfg.process_start = Some(UNIX_EPOCH + Duration::from_millis(1000));
    let client = start(
        &cfg,
        Tuning {
            now: Arc::new(|| UNIX_EPOCH + Duration::from_millis(1800)),
            ..tuning()
        },
    );
    client.app_opened().unwrap();
    client.app_opened().unwrap();
    client.close().unwrap();

    let events = events(&fake.wait(1));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["name"], "app.open");
    assert_eq!(events[0]["value"].as_f64(), Some(800.0));
    assert_eq!(events[0]["props"], json!({"cold": true}));
}

#[test]
fn props_must_be_allowed() {
    let fake = Fake::new();
    let (mut cfg, _dir) = config(&fake);
    cfg.allow_props = vec!["code".into()];
    let client = start(&cfg, tuning());
    client
        .track(
            "error.sync",
            props(&[("code", json!(42)), ("message", json!("secret"))]),
        )
        .unwrap();
    client
        .track("x", props(&[("message", json!("secret"))]))
        .unwrap();
    client
        .track("big", props(&[("code", json!(9_007_199_254_740_993_i64))]))
        .unwrap();
    assert!(matches!(
        client.track("Bad Name", Props::new()),
        Err(Error::Invalid(_))
    ));
    client.close().unwrap();

    let body = fake.wait(1).remove(0);
    assert!(
        String::from_utf8_lossy(&body).contains(r#""props":{"code":9007199254740993}"#),
        "large integers keep their digits"
    );
    let events = events(&[body]);
    assert_eq!(events.len(), 3);
    assert_eq!(events[0]["props"], json!({"code": 42}));
    assert!(events[1].get("props").is_none());
}

#[test]
fn non_finite_value_is_refused() {
    let fake = Fake::new();
    let (cfg, _dir) = config(&fake);
    let client = start(&cfg, tuning());
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(matches!(
            client.track_value("x", value, Props::new()),
            Err(Error::Invalid(_))
        ));
    }
}

#[test]
fn track_error() {
    let fake = Fake::new();
    let (mut cfg, _dir) = config(&fake);
    cfg.allow_props = vec!["code".into(), "message".into()];
    let client = start(&cfg, tuning());
    client
        .track_error(
            "engine",
            "boom",
            props(&[
                ("code", json!(42)),
                ("message", json!("old")),
                ("other", json!(1)),
            ]),
        )
        .unwrap();
    client.track_error("sync", "", Props::new()).unwrap();
    client.track_error("auth", "denied", Props::new()).unwrap();
    for area in ["", "Bad Area"] {
        assert!(client.track_error(area, "m", Props::new()).is_err());
    }
    client.close().unwrap();

    let events = events(&fake.wait(1));
    assert_eq!(events.len(), 3);
    assert_eq!(events[0]["name"], "error.engine");
    assert_eq!(events[0]["props"], json!({"code": 42, "message": "boom"}));
    assert_eq!(events[1]["name"], "error.sync");
    assert!(events[1].get("props").is_none());
    assert_eq!(events[2]["name"], "error.auth");
    assert_eq!(events[2]["props"], json!({"message": "denied"}));
}

#[test]
fn track_error_message_must_be_allowed() {
    let fake = Fake::new();
    let (mut cfg, _dir) = config(&fake);
    let logged = Arc::new(AtomicU32::new(0));
    let count = Arc::clone(&logged);
    cfg.log = Some(Arc::new(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
    }));
    let client = start(&cfg, tuning());
    client
        .track_error("engine", "secret", Props::new())
        .unwrap();
    client.close().unwrap();
    let events = events(&fake.wait(1));
    assert_eq!(events[0]["name"], "error.engine");
    assert!(events[0].get("props").is_none());
    assert_eq!(logged.load(Ordering::SeqCst), 0);
}

#[test]
fn track_error_cleans_message() {
    let fake = Fake::new();
    let (mut cfg, _dir) = config(&fake);
    cfg.allow_props = vec!["message".into()];
    let client = start(&cfg, tuning());
    let cases = [
        ("a\0b".to_string(), "ab".to_string()),
        ("€".repeat(5000), "€".repeat(682)),
        ("\u{1}".repeat(5000), "\u{1}".repeat(MAX_MESSAGE)),
    ];
    for (input, _) in &cases {
        client.track_error("x", input, Props::new()).unwrap();
    }
    client
        .track_error("only_nul", "\0", props(&[("message", json!("kept"))]))
        .unwrap();
    client.close().unwrap();

    let events = events(&fake.wait(1));
    assert_eq!(events.len(), cases.len() + 1);
    for (event, (_, want)) in events.iter().zip(&cases) {
        assert_eq!(event["props"]["message"].as_str().unwrap(), want);
    }
    assert_eq!(events[cases.len()]["props"], json!({"message": "kept"}));
}

#[test]
fn panicking_logger_does_not_stop_delivery() {
    let fake = Fake::new();
    let (mut cfg, _dir) = config(&fake);
    let message = Arc::new(Mutex::new(String::new()));
    let seen = Arc::clone(&message);
    cfg.log = Some(Arc::new(move |m| {
        *seen.lock().unwrap() = m.to_string();
        panic!("logger");
    }));
    let client = start(&cfg, tuning());
    client.track("a", props(&[("secret", json!(1))])).unwrap();
    client.close().unwrap();

    assert_eq!(names(&events(&fake.wait(1))), "a");
    assert_eq!(
        *message.lock().unwrap(),
        "beacon: a: dropped props not allowed: secret"
    );
}

#[test]
fn platform_defaults_to_the_operating_system() {
    let fake = Fake::new();
    let (cfg, _dir) = config(&fake);
    let client = start(&cfg, tuning());
    client.track("a", Props::new()).unwrap();
    client.close().unwrap();
    assert_eq!(events(&fake.wait(1))[0]["platform"], std::env::consts::OS);
}

#[test]
fn user_id_is_opt_in() {
    let fake = Fake::new();
    let (cfg, _dir) = config(&fake);
    let client = start(&cfg, tuning());
    client.track("a", Props::new()).unwrap();
    client.set_user_id("tester");
    client.track("b", Props::new()).unwrap();
    client.close().unwrap();
    let events = events(&fake.wait(1));
    assert!(events[0].get("user_id").is_none());
    assert_eq!(events[1]["user_id"], "tester");
}

#[test]
fn rejects_bad_config() {
    let fake = Fake::new();
    let (good, _dir) = config(&fake);
    let cases: [fn(&mut Config); 7] = [
        |c| c.endpoint = String::new(),
        |c| c.endpoint = "ftp://x".into(),
        |c| c.endpoint = "http://".into(),
        |c| c.endpoint = "http:// x".into(),
        |c| c.key = String::new(),
        |c| c.dir = "".into(),
        |c| c.app_version = "a".repeat(65),
    ];
    for (i, change) in cases.iter().enumerate() {
        let mut cfg = good.clone();
        change(&mut cfg);
        assert!(Client::new(cfg).is_err(), "case {i} accepted");
    }
}

fn open_queue(dir: &Path, max_bytes: u64, max_events: usize) -> (Queue, Arc<AtomicU64>) {
    let dropped = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&dropped);
    let on_drop = Box::new(move |n: usize| {
        counter.fetch_add(n as u64, Ordering::SeqCst);
    });
    let queue = Queue::open(dir, max_bytes, max_events, MAX_BATCH_BYTES, on_drop).unwrap();
    (queue, dropped)
}

fn add(queue: &Queue, line: &str) {
    queue.add(line.as_bytes()).1.unwrap();
}

#[test]
fn queue_drops_oldest_past_cap() {
    let dir = TempDir::new().unwrap();
    let (queue, dropped) = open_queue(dir.path(), 100, 2);
    for i in 0..20 {
        add(&queue, &format!(r#"{{"name":"e{i:02}"}}"#));
    }
    assert!(dropped.load(Ordering::SeqCst) > 0);
    assert!(queue.total() <= 100);
    let lines = read_lines(&queue.oldest().unwrap().path).unwrap();
    assert!(!String::from_utf8_lossy(&lines[0]).contains("e00"));
}

#[test]
fn queue_skips_torn_line() {
    let dir = TempDir::new().unwrap();
    let (queue, _) = open_queue(dir.path(), 1 << 20, 10);
    add(&queue, r#"{"name":"a"}"#);
    queue.seal();
    let seg = queue.oldest().unwrap();
    let mut file = OpenOptions::new().append(true).open(&seg.path).unwrap();
    file.write_all(br#"{"name":"b"#).unwrap();
    drop(file);

    let lines = read_lines(&seg.path).unwrap();
    assert_eq!(lines, vec![br#"{"name":"a"}"#.to_vec()]);
}

#[test]
fn queue_reopens_oldest_first() {
    let root = TempDir::new().unwrap();
    let dir = root.path().join("q");
    let (queue, _) = open_queue(&dir, 1 << 20, 1);
    for name in ["a", "b"] {
        add(&queue, &format!(r#"{{"name":"{name}"}}"#));
    }
    let leftover = dir.join("leftover.tmp");
    fs::write(&leftover, "x").unwrap();

    let (again, _) = open_queue(&dir, 1 << 20, 1);
    assert_eq!(again.sizes().len(), 2);
    let lines = read_lines(&again.oldest().unwrap().path).unwrap();
    assert_eq!(lines[0], br#"{"name":"a"}"#);
    add(&again, r#"{"name":"c"}"#);
    assert_eq!(again.sizes().len(), 3);
    assert!(!leftover.exists());
}

#[test]
fn batch_fits_body_limit() {
    let dir = TempDir::new().unwrap();
    let (queue, _) = open_queue(dir.path(), 100 << 20, 100);
    let big = format!(
        r#"{{"name":"x","props":{{"p":"{}"}}}}"#,
        "a".repeat(MAX_PROPS)
    );
    for _ in 0..300 {
        add(&queue, &big);
    }
    let sizes = queue.sizes();
    assert!(sizes.len() >= 2);
    assert!(sizes.iter().all(|&s| s <= MAX_BATCH_BYTES));
}

#[test]
fn backoff_grows_and_caps() {
    let (min, max) = (Duration::from_secs(1), Duration::from_secs(8));
    for (failures, want) in [1, 2, 4, 8, 8, 8].into_iter().enumerate() {
        let limit = backoff_limit(failures as u32, min, max);
        assert_eq!(limit, Duration::from_secs(want));
    }
    assert_eq!(backoff_limit(100, min, max), max);
    assert_eq!(backoff_limit(u32::MAX, min, max), max);
    for _ in 0..100 {
        let wait = jitter(max);
        assert!(wait >= max / 2 && wait <= max, "{wait:?}");
    }
}

#[test]
fn times_are_rfc3339() {
    let at = |ms: u64| format_time(UNIX_EPOCH + Duration::from_millis(ms));
    assert_eq!(at(0), "1970-01-01T00:00:00Z");
    assert_eq!(at(1_794_139_140_000), "2026-11-08T11:59:00Z");
    assert_eq!(at(1_794_139_140_120), "2026-11-08T11:59:00.120Z");
    assert_eq!(at(1_709_164_800_000), "2024-02-29T00:00:00Z");
    assert_eq!(at(1_735_689_599_000), "2024-12-31T23:59:59Z");
    assert_eq!(format_time(SystemTime::UNIX_EPOCH), "1970-01-01T00:00:00Z");
}
