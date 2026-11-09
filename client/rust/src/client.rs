use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Once, PoisonError};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use serde_json::Value;
use serde_json::value::RawValue;

use crate::event::{Event, Props, clean_message, filter_props, format_time};
use crate::lock;
use crate::queue::{Queue, SegmentRef, create_private_dir, read_lines, write_file_atomic};
use crate::send::{
    Action, Outcome, Request, Transport, backoff_limit, classify, encode_batch, jitter,
};

const DEFAULT_MAX_QUEUE_BYTES: u64 = 10 << 20;
const DEFAULT_BATCH_EVENTS: usize = 100;
const DEFAULT_FLUSH_EVERY: Duration = Duration::from_secs(30);
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(60);
const DEFAULT_MIN_BACKOFF: Duration = Duration::from_secs(1);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);
const DEFAULT_CLOSE_WAIT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// Keeps a batch under the server's 1 MiB limit even when gzip gains nothing.
pub(crate) const MAX_BATCH_BYTES: u64 = 900 << 10;

/// LogFn receives messages about delivery problems and dropped data. It must
/// not call the Client.
pub type LogFn = Arc<dyn Fn(&str) + Send + Sync>;

/// Error is why the client refused a call.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Closed means the client has been closed.
    Closed,
    /// Config means the configuration can't be used.
    Config(String),
    /// Invalid means the event breaks one of the server's rules.
    Invalid(String),
    /// Io means the queue or the device id could not be read or written.
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("beacon: client closed"),
            Self::Config(message) | Self::Invalid(message) => f.write_str(message),
            Self::Io(e) => write!(f, "beacon: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// Config configures a Client.
#[derive(Clone)]
pub struct Config {
    /// endpoint is the server's base URL.
    pub endpoint: String,
    /// key is the app's ingest key.
    pub key: String,
    /// dir holds the queue and the device id. Use one Client per directory.
    pub dir: PathBuf,
    /// transport sends the requests.
    pub transport: Arc<dyn Transport>,
    /// app_version is the app's version.
    pub app_version: String,
    /// platform defaults to the operating system: macos, windows, linux, ios
    /// or android.
    pub platform: String,
    /// os_version is the operating system's version.
    pub os_version: String,
    /// max_queue_bytes caps the queue on disk, 0 meaning 10 MiB. Past it the
    /// oldest events are dropped.
    pub max_queue_bytes: u64,
    /// process_start is the time app_opened measures from, by default when
    /// the Client is created.
    pub process_start: Option<SystemTime>,
    /// log receives messages about delivery problems and dropped data.
    pub log: Option<LogFn>,
    /// allow_props lists the props keys that may leave the app. Others are
    /// dropped.
    pub allow_props: Vec<String>,
}

impl Config {
    /// Config::new returns a Config with the required fields and defaults for
    /// the rest.
    pub fn new(
        endpoint: impl Into<String>,
        key: impl Into<String>,
        dir: impl Into<PathBuf>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            key: key.into(),
            dir: dir.into(),
            transport,
            app_version: String::new(),
            platform: String::new(),
            os_version: String::new(),
            max_queue_bytes: 0,
            process_start: None,
            log: None,
            allow_props: Vec::new(),
        }
    }
}

/// Values only tests change.
#[derive(Clone)]
pub(crate) struct Tuning {
    pub flush_every: Duration,
    pub heartbeat_every: Duration,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    pub close_wait: Duration,
    pub batch_events: usize,
    pub now: Arc<dyn Fn() -> SystemTime + Send + Sync>,
    pub new_id: Arc<dyn Fn() -> io::Result<String> + Send + Sync>,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            flush_every: DEFAULT_FLUSH_EVERY,
            heartbeat_every: DEFAULT_HEARTBEAT,
            min_backoff: DEFAULT_MIN_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
            close_wait: DEFAULT_CLOSE_WAIT,
            batch_events: DEFAULT_BATCH_EVENTS,
            now: Arc::new(SystemTime::now),
            new_id: Arc::new(random_id),
        }
    }
}

#[derive(Clone)]
struct Logger(Option<LogFn>);

impl Logger {
    fn log(&self, message: &str) {
        if let Some(log) = &self.0 {
            // Without this, a panic on the background thread would silently
            // end delivery.
            let _ = panic::catch_unwind(AssertUnwindSafe(|| log(&format!("beacon: {message}"))));
        }
    }
}

struct Session {
    id: String,
    start: SystemTime,
}

#[derive(Default)]
struct State {
    user_id: String,
    session: Option<Session>,
    closed: bool,
}

#[derive(Default)]
struct Signal {
    notified: bool,
    stop: bool,
}

pub(crate) struct Inner {
    url: String,
    key: String,
    transport: Arc<dyn Transport>,
    pub(crate) queue: Queue,
    log: Logger,
    allowed: HashSet<String>,
    tuning: Tuning,
    started: SystemTime,
    device_id: String,
    app_version: String,
    platform: String,
    os_version: String,
    opened: AtomicBool,
    cancelled: AtomicBool,
    state: Mutex<State>,
    signal: Mutex<Signal>,
    wake: Condvar,
    done: Mutex<bool>,
    finished: Condvar,
}

/// Client queues and sends events, and is safe to use from any thread.
/// Dropping it stops sending at once; close lets queued events go out first.
pub struct Client {
    pub(crate) inner: Arc<Inner>,
    close_once: Once,
}

impl Client {
    /// Client::new opens the queue in config.dir and starts sending in the
    /// background.
    pub fn new(config: Config) -> Result<Self, Error> {
        Self::with_tuning(config, Tuning::default())
    }

    pub(crate) fn with_tuning(config: Config, tuning: Tuning) -> Result<Self, Error> {
        if config.key.is_empty() || config.dir.as_os_str().is_empty() {
            return Err(Error::Config(
                "beacon: endpoint, key and dir are required".into(),
            ));
        }
        if !valid_endpoint(&config.endpoint) {
            return Err(Error::Config(format!(
                "beacon: invalid endpoint {:?}",
                config.endpoint
            )));
        }

        let device_id = load_device_id(&config.dir, &*tuning.new_id)?;
        let platform = if config.platform.is_empty() {
            std::env::consts::OS.to_string()
        } else {
            config.platform
        };
        Event {
            name: "probe".into(),
            device_id: device_id.clone(),
            app_version: config.app_version.clone(),
            platform: platform.clone(),
            os_version: config.os_version.clone(),
            ..Event::default()
        }
        .validate()?;

        let log = Logger(config.log);
        let on_drop = {
            let log = log.clone();
            Box::new(move |n: usize| log.log(&format!("queue full: dropped {n} oldest events")))
        };
        let queue = Queue::open(
            &config.dir.join("queue"),
            match config.max_queue_bytes {
                0 => DEFAULT_MAX_QUEUE_BYTES,
                n => n,
            },
            tuning.batch_events,
            MAX_BATCH_BYTES,
            on_drop,
        )?;

        let inner = Arc::new(Inner {
            url: format!("{}/v1/events", config.endpoint.trim_end_matches('/')),
            key: config.key,
            transport: config.transport,
            queue,
            log,
            allowed: config.allow_props.into_iter().collect(),
            started: config.process_start.unwrap_or_else(|| (tuning.now)()),
            tuning,
            device_id,
            app_version: config.app_version,
            platform,
            os_version: config.os_version,
            opened: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            state: Mutex::default(),
            signal: Mutex::default(),
            wake: Condvar::new(),
            done: Mutex::new(false),
            finished: Condvar::new(),
        });

        let worker = Worker {
            inner: Arc::clone(&inner),
            failures: 0,
        };
        thread::Builder::new()
            .name("beacon".into())
            .spawn(move || worker.run())?;
        inner.notify(); // events left by the previous run
        Ok(Self {
            inner,
            close_once: Once::new(),
        })
    }

    /// device_id returns the id generated for this install.
    pub fn device_id(&self) -> &str {
        &self.inner.device_id
    }

    /// set_user_id sets the user_id of later events, "" for none. Only tester
    /// builds should call it.
    pub fn set_user_id(&self, id: &str) {
        lock(&self.inner.state).user_id = id.to_string();
    }

    /// track queues an event. Report errors with track_error: the dashboard
    /// counts only names starting "error.".
    pub fn track(&self, name: &str, props: Props) -> Result<(), Error> {
        self.inner.track(name, None, props)
    }

    /// track_error queues error.<area>, counted as an error by the dashboard.
    /// A non-empty message, cut to 2 KiB, replaces props["message"] if allowed.
    pub fn track_error(&self, area: &str, message: &str, mut props: Props) -> Result<(), Error> {
        let message = clean_message(message);
        // A message that isn't allowed stays out of props, or every error
        // would log a drop.
        if !message.is_empty() && self.inner.allowed.contains("message") {
            props.insert("message".into(), Value::String(message));
        }
        self.inner.track(&format!("error.{area}"), None, props)
    }

    /// track_value queues an event with its one numeric measure, such as a
    /// duration in milliseconds.
    pub fn track_value(&self, name: &str, value: f64, props: Props) -> Result<(), Error> {
        self.inner.track(name, Some(value), props)
    }

    /// app_opened records app.open once per process, with the time since
    /// process start. Call it when the first screen is usable.
    pub fn app_opened(&self) -> Result<(), Error> {
        if self.inner.opened.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let now = (self.inner.tuning.now)();
        let elapsed = now.duration_since(self.inner.started).unwrap_or_default();
        let props = RawValue::from_string(r#"{"cold":true}"#.to_string())
            .map_err(|e| Error::Invalid(e.to_string()))?;
        self.inner
            .enqueue("app.open", Some(elapsed.as_millis() as f64), Some(props))
    }

    /// start_session begins a session and sends session.start, then a
    /// heartbeat each minute. It does nothing while a session is open.
    pub fn start_session(&self) -> Result<(), Error> {
        let mut state = lock(&self.inner.state);
        if state.session.is_some() {
            return Ok(());
        }
        state.session = Some(Session {
            id: (self.inner.tuning.new_id)()?,
            start: (self.inner.tuning.now)(),
        });
        self.inner
            .enqueue_locked(&state, "session.start", None, None)
    }

    /// end_session sends session.end and closes the session.
    pub fn end_session(&self) -> Result<(), Error> {
        self.inner.end_session()
    }

    /// flush starts sending queued events now, without waiting for them.
    pub fn flush(&self) {
        self.inner.queue.seal();
        self.inner.notify();
    }

    /// close ends any open session and blocks for a few seconds at most while
    /// it tries to send. Unsent events stay on disk for the next run.
    pub fn close(&self) -> Result<(), Error> {
        let mut result = Ok(());
        self.close_once.call_once(|| {
            result = self.inner.end_session();
            self.inner.begin_stop();
            self.inner.wait_done();
        });
        result
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.inner.cancelled.store(true, Ordering::SeqCst);
        self.inner.begin_stop();
    }
}

impl Inner {
    fn track(&self, name: &str, value: Option<f64>, props: Props) -> Result<(), Error> {
        let (encoded, dropped) = filter_props(props, &self.allowed)?;
        if !dropped.is_empty() {
            self.log.log(&format!(
                "{name}: dropped props not allowed: {}",
                dropped.join(", ")
            ));
        }
        self.enqueue(name, value, encoded)
    }

    fn enqueue(
        &self,
        name: &str,
        value: Option<f64>,
        props: Option<Box<RawValue>>,
    ) -> Result<(), Error> {
        self.enqueue_locked(&lock(&self.state), name, value, props)
    }

    fn enqueue_locked(
        &self,
        state: &State,
        name: &str,
        value: Option<f64>,
        props: Option<Box<RawValue>>,
    ) -> Result<(), Error> {
        if state.closed {
            return Err(Error::Closed);
        }
        let event = Event {
            name: name.to_string(),
            ts: Some(format_time((self.tuning.now)())),
            device_id: self.device_id.clone(),
            user_id: state.user_id.clone(),
            session_id: state
                .session
                .as_ref()
                .map(|s| s.id.clone())
                .unwrap_or_default(),
            app_version: self.app_version.clone(),
            platform: self.platform.clone(),
            os_version: self.os_version.clone(),
            value,
            props,
        };
        event.validate()?;
        let line = serde_json::to_vec(&event).map_err(|e| Error::Invalid(e.to_string()))?;
        let (ready, written) = self.queue.add(&line);
        if ready {
            self.notify();
        }
        Ok(written?)
    }

    fn end_session(&self) -> Result<(), Error> {
        let mut state = lock(&self.state);
        let Some(seconds) = self.session_seconds(&state) else {
            return Ok(());
        };
        let result = self.enqueue_locked(&state, "session.end", Some(seconds), None);
        state.session = None;
        result
    }

    fn heartbeat(&self) {
        let state = lock(&self.state);
        let Some(seconds) = self.session_seconds(&state) else {
            return;
        };
        match self.enqueue_locked(&state, "session.heartbeat", Some(seconds), None) {
            Ok(()) | Err(Error::Closed) => {}
            Err(e) => self.log.log(&format!("heartbeat: {e}")),
        }
    }

    fn session_seconds(&self, state: &State) -> Option<f64> {
        let session = state.session.as_ref()?;
        let elapsed = (self.tuning.now)()
            .duration_since(session.start)
            .unwrap_or_default();
        Some(elapsed.as_secs_f64().round())
    }

    fn notify(&self) {
        lock(&self.signal).notified = true;
        self.wake.notify_one();
    }

    fn begin_stop(&self) {
        lock(&self.state).closed = true;
        lock(&self.signal).stop = true;
        self.wake.notify_one();
    }

    /// Waits for the worker to finish, up to close_wait. A transport can't be
    /// interrupted, so past that the worker is left to end on its own.
    pub(crate) fn wait_done(&self) {
        let (_done, timeout) = self
            .finished
            .wait_timeout_while(lock(&self.done), self.tuning.close_wait, |done| !*done)
            .unwrap_or_else(PoisonError::into_inner);
        if timeout.timed_out() {
            self.cancelled.store(true, Ordering::SeqCst);
        }
    }

    /// Sleeps until notified, stopped or deadline. Returns (notified, stop).
    fn wait(&self, deadline: Instant) -> (bool, bool) {
        let mut signal = lock(&self.signal);
        while !signal.notified && !signal.stop {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                break;
            }
            signal = self
                .wake
                .wait_timeout(signal, timeout)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        (std::mem::take(&mut signal.notified), signal.stop)
    }
}

/// Marks the worker finished on the way out, also when it unwinds.
struct Finished(Arc<Inner>);

impl Drop for Finished {
    fn drop(&mut self) {
        *lock(&self.0.done) = true;
        self.0.finished.notify_all();
    }
}

struct Worker {
    inner: Arc<Inner>,
    failures: u32,
}

impl Worker {
    fn run(mut self) {
        let inner = Arc::clone(&self.inner);
        let _finished = Finished(Arc::clone(&inner));
        let mut next_flush = Instant::now() + inner.tuning.flush_every;
        let mut next_beat = Instant::now() + inner.tuning.heartbeat_every;
        let mut retry_at: Option<Instant> = None;

        loop {
            let deadline = retry_at.map_or(next_flush.min(next_beat), |retry| {
                retry.min(next_flush).min(next_beat)
            });
            let (notified, stop) = inner.wait(deadline);
            if stop {
                inner.queue.seal();
                if retry_at.is_none() {
                    self.drain();
                }
                return;
            }

            let now = Instant::now();
            if now >= next_beat {
                inner.heartbeat();
                next_beat = now + inner.tuning.heartbeat_every;
            }
            let mut send = notified;
            if now >= next_flush {
                inner.queue.seal_idle();
                next_flush = now + inner.tuning.flush_every;
                send = true;
            }
            if retry_at.is_some_and(|retry| now >= retry) {
                retry_at = None;
                send = true;
            }
            if send && retry_at.is_none() {
                let wait = self.drain();
                if !wait.is_zero() {
                    retry_at = Some(Instant::now() + wait);
                }
            }
        }
    }

    /// Sends sealed segments oldest first until none are left or the server
    /// can't take more. Returns how long to wait before trying again.
    fn drain(&mut self) -> Duration {
        let inner = Arc::clone(&self.inner);
        while !inner.cancelled.load(Ordering::SeqCst) {
            let Some(seg) = inner.queue.oldest() else {
                return Duration::ZERO;
            };
            let lines = match read_lines(&seg.path) {
                Ok(lines) => lines,
                Err(e) => {
                    inner.log.log(&format!("read queue: {e}"));
                    return self.next_backoff();
                }
            };
            if lines.is_empty() {
                inner.queue.remove(seg.seq);
                continue;
            }
            let outcome = match self.send(&lines) {
                Ok(outcome) => outcome,
                Err(e) => {
                    inner.log.log(&format!("send: {e}"));
                    return self.next_backoff();
                }
            };
            match outcome.action {
                Action::Accept => {
                    self.failures = 0;
                    inner.queue.remove(seg.seq);
                }
                Action::DropBatch => {
                    self.failures = 0;
                    inner.log.log(&format!(
                        "dropped a batch of {} events the server rejected",
                        lines.len()
                    ));
                    inner.queue.remove(seg.seq);
                }
                Action::DropEvent => {
                    self.failures = 0;
                    if let Err(e) = self.drop_event(&seg, lines, outcome.index) {
                        inner.log.log(&format!("rewrite queue: {e}"));
                        return self.next_backoff();
                    }
                }
                Action::RetryAfter => return outcome.wait,
                Action::Backoff => return self.next_backoff(),
            }
        }
        Duration::ZERO
    }

    fn send(&self, lines: &[Vec<u8>]) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
        let inner = &self.inner;
        let request = Request {
            url: inner.url.clone(),
            headers: vec![
                ("Authorization", format!("Bearer {}", inner.key)),
                ("Content-Type", "application/json".into()),
                ("Content-Encoding", "gzip".into()),
            ],
            body: encode_batch(lines)?,
            timeout: REQUEST_TIMEOUT,
        };
        // Without this, a panic would silently end delivery.
        let response = panic::catch_unwind(AssertUnwindSafe(|| inner.transport.post(request)))
            .map_err(|_| "transport panicked")??;
        if response.status != 202 {
            let body = String::from_utf8_lossy(&response.body);
            let body: String = body.chars().take(500).collect();
            inner
                .log
                .log(&format!("server returned {}: {body}", response.status));
        }
        Ok(classify(
            response.status,
            response.retry_after.as_deref(),
            &response.body,
        ))
    }

    /// Removes the event the server named, so the rest of the batch can be
    /// sent again. The server stores nothing from a rejected batch.
    fn drop_event(&self, seg: &SegmentRef, mut lines: Vec<Vec<u8>>, index: i64) -> io::Result<()> {
        let inner = &self.inner;
        let at = usize::try_from(index).ok().filter(|&i| i < lines.len());
        let Some(at) = at.filter(|_| lines.len() > 1) else {
            inner.log.log(&format!(
                "dropped a batch of {} events the server rejected",
                lines.len()
            ));
            inner.queue.remove(seg.seq);
            return Ok(());
        };
        inner.log.log(&format!(
            "dropped event {at} of a batch the server rejected"
        ));
        lines.remove(at);
        inner.queue.rewrite(seg.seq, &lines)
    }

    fn next_backoff(&mut self) -> Duration {
        let tuning = &self.inner.tuning;
        let limit = backoff_limit(self.failures, tuning.min_backoff, tuning.max_backoff);
        self.failures = self.failures.saturating_add(1);
        jitter(limit)
    }
}

fn valid_endpoint(endpoint: &str) -> bool {
    let Some((scheme, rest)) = endpoint.split_once("://") else {
        return false;
    };
    let host = rest.split('/').next().unwrap_or_default();
    (scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https"))
        && !host.is_empty()
        // A query or fragment would swallow the /v1/events appended to it.
        && !rest.contains(|c: char| c.is_whitespace() || c == '?' || c == '#')
}

/// Returns the id stored in dir, creating it on first use. It is random and
/// never derived from hardware.
fn load_device_id(dir: &Path, new_id: &dyn Fn() -> io::Result<String>) -> io::Result<String> {
    create_private_dir(dir)?;
    let path = dir.join("device_id");
    match fs::read_to_string(&path) {
        Ok(text) if !text.trim().is_empty() => return Ok(text.trim().to_string()),
        Ok(_) => {}
        // InvalidData is a file that isn't UTF-8, which would otherwise keep
        // the client from starting on every launch.
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::InvalidData
            ) => {}
        Err(e) => return Err(e),
    }
    let id = new_id()?;
    write_file_atomic(&path, id.as_bytes())?;
    Ok(id)
}

fn random_id() -> io::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| io::Error::other(e.to_string()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
