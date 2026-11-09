use std::io::Write;
use std::time::Duration;

use flate2::Compression;
use flate2::write::GzEncoder;
use serde::Deserialize;

const MAX_RETRY_AFTER_SECONDS: u64 = 3600;

/// TransportError is why a request got no response.
pub type TransportError = Box<dyn std::error::Error + Send + Sync>;

/// Request is one POST for a Transport to send.
pub struct Request {
    /// url is the full URL.
    pub url: String,
    /// headers lists the headers to set, by name.
    pub headers: Vec<(&'static str, String)>,
    /// body is already compressed.
    pub body: Vec<u8>,
    /// timeout is how long the whole request may take.
    pub timeout: Duration,
}

/// Response is what a Transport got back. Any status is a Response, not an
/// error.
pub struct Response {
    /// status is the HTTP status.
    pub status: u16,
    /// retry_after is the Retry-After header, if there is one.
    pub retry_after: Option<String>,
    /// body is the response body. The first 64 KiB is enough.
    pub body: Vec<u8>,
}

/// Transport sends requests with the app's own HTTP client, on a background
/// thread that it may block. It must honour the request's timeout.
pub trait Transport: Send + Sync {
    /// post sends one request and waits for the response.
    fn post(&self, request: Request) -> Result<Response, TransportError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(Deserialize), serde(rename_all = "snake_case"))]
pub(crate) enum Action {
    Accept,
    DropEvent,
    DropBatch,
    RetryAfter,
    Backoff,
}

/// What to do with a batch after a response.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Outcome {
    pub action: Action,
    pub index: i64,
    pub wait: Duration,
}

impl From<Action> for Outcome {
    fn from(action: Action) -> Self {
        Self {
            action,
            index: 0,
            wait: Duration::ZERO,
        }
    }
}

#[derive(Deserialize)]
struct ServerError {
    #[serde(default)]
    error: String,
    #[serde(default)]
    index: Option<i64>,
}

/// Maps a response to an outcome. Events are dropped only on a 400 in the
/// server's own error format, so that an error page from a proxy can't destroy
/// the queue. client/conformance/responses.json is the table every client must
/// follow.
pub(crate) fn classify(status: u16, retry_after: Option<&str>, body: &[u8]) -> Outcome {
    match status {
        202 => Action::Accept.into(),
        // A derived Deserialize also reads a JSON array, as the fields in order.
        400 if !body.trim_ascii_start().starts_with(b"{") => Action::Backoff.into(),
        400 => match serde_json::from_slice::<ServerError>(body) {
            Ok(ServerError { error, .. }) if error.is_empty() => Action::Backoff.into(),
            Ok(ServerError {
                index: Some(index), ..
            }) => Outcome {
                index,
                ..Action::DropEvent.into()
            },
            Ok(_) => Action::DropBatch.into(),
            Err(_) => Action::Backoff.into(),
        },
        429 => match retry_after.and_then(retry_wait) {
            Some(wait) => Outcome {
                wait,
                ..Action::RetryAfter.into()
            },
            None => Action::Backoff.into(),
        },
        _ => Action::Backoff.into(),
    }
}

fn retry_wait(value: &str) -> Option<Duration> {
    // u64::from_str accepts a leading plus sign, which the table refuses.
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // All digits, so a parse error can only be overflow.
    let secs = value.parse().unwrap_or(MAX_RETRY_AFTER_SECONDS);
    (secs > 0).then(|| Duration::from_secs(secs.min(MAX_RETRY_AFTER_SECONDS)))
}

pub(crate) fn encode_batch(lines: &[Vec<u8>]) -> std::io::Result<Vec<u8>> {
    let mut zw = GzEncoder::new(Vec::new(), Compression::default());
    zw.write_all(br#"{"events":["#)?;
    zw.write_all(&lines.join(&b','))?;
    zw.write_all(b"]}")?;
    zw.finish()
}

/// The longest wait after this many failures in a row: doubling from min up to
/// max.
pub(crate) fn backoff_limit(failures: u32, min: Duration, max: Duration) -> Duration {
    2u32.checked_pow(failures)
        .and_then(|factor| min.checked_mul(factor))
        .map_or(max, |d| d.min(max))
}

/// Picks a wait in the upper half of limit, so that a fleet that lost the
/// server together doesn't return together.
pub(crate) fn jitter(limit: Duration) -> Duration {
    let half = limit / 2;
    let range = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX - 1) + 1;
    half + Duration::from_nanos(getrandom::u64().unwrap_or(0) % range)
}
