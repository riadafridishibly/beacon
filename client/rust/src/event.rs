use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::value::RawValue;

use crate::client::Error;

// These limits match internal/ingest. client/conformance/events.json is run
// against both.
const MAX_NAME_LEN: usize = 64;
const MAX_ID_LEN: usize = 128;
const MAX_USER_LEN: usize = 256;
const MAX_ATTR_LEN: usize = 64;
pub(crate) const MAX_PROPS: usize = 16 << 10;

// Keeps an error message within MAX_PROPS even if JSON escaping grows every
// byte to six.
pub(crate) const MAX_MESSAGE: usize = 2 << 10;

/// Props is the JSON object an event carries beside its one numeric value.
pub type Props = serde_json::Map<String, serde_json::Value>;

#[derive(Default, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize), serde(default))]
pub(crate) struct Event {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub device_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub user_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub app_version: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub platform: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub os_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub props: Option<Box<RawValue>>,
}

impl Event {
    /// Applies the server's rules, so that an event it would reject never
    /// reaches the queue.
    pub fn validate(&self) -> Result<(), Error> {
        let invalid = |message: String| Err(Error::Invalid(format!("beacon: {message}")));
        if !valid_name(&self.name) {
            return invalid(format!(
                "name {:?} must be at most {MAX_NAME_LEN} lowercase letters, digits and underscores, in dot-separated parts",
                self.name
            ));
        }
        if self.device_id.is_empty() {
            return invalid("device_id is required".into());
        }
        if self.device_id.len() > MAX_ID_LEN || self.session_id.len() > MAX_ID_LEN {
            return invalid(format!(
                "device_id and session_id must be at most {MAX_ID_LEN} bytes"
            ));
        }
        if self.user_id.len() > MAX_USER_LEN {
            return invalid(format!("user_id must be at most {MAX_USER_LEN} bytes"));
        }
        if [&self.app_version, &self.platform, &self.os_version]
            .iter()
            .any(|s| s.len() > MAX_ATTR_LEN)
        {
            return invalid(format!(
                "app_version, platform and os_version must be at most {MAX_ATTR_LEN} bytes"
            ));
        }
        if [
            &self.device_id,
            &self.user_id,
            &self.session_id,
            &self.app_version,
            &self.platform,
            &self.os_version,
        ]
        .iter()
        .any(|s| s.contains('\0'))
        {
            return invalid(
                "device_id, user_id, session_id, app_version, platform and os_version must not contain NUL".into(),
            );
        }
        // serde_json writes a non-finite number as null.
        if self.value.is_some_and(|v| !v.is_finite()) {
            return invalid("value must be a finite number".into());
        }
        if let Some(props) = &self.props {
            let json = props.get();
            if json.len() > MAX_PROPS {
                return invalid(format!("props must be at most {MAX_PROPS} bytes"));
            }
            if !json.starts_with('{') {
                return invalid("props must be a JSON object".into());
            }
            if !storable_json(json) {
                return invalid("props must not contain \\u0000 or unpaired surrogates".into());
            }
        }
        Ok(())
    }
}

fn valid_name(name: &str) -> bool {
    name.len() <= MAX_NAME_LEN
        && name.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
}

/// Reports whether valid JSON can be stored as jsonb, which refuses \u0000 and
/// unpaired surrogates.
fn storable_json(json: &str) -> bool {
    let b = json.as_bytes();
    let mut i = 0;
    while let Some(&c) = b.get(i) {
        i += 1;
        if c != b'\\' {
            continue;
        }
        let Some(&escape) = b.get(i) else { break };
        i += 1;
        if escape != b'u' {
            continue;
        }
        let Some(unit) = hex4(b, i) else { return false };
        i += 4;
        match unit {
            0 | 0xDC00..=0xDFFF => return false,
            0xD800..=0xDBFF => {
                if b.get(i) != Some(&b'\\') || b.get(i + 1) != Some(&b'u') {
                    return false;
                }
                if !matches!(hex4(b, i + 2), Some(0xDC00..=0xDFFF)) {
                    return false;
                }
                i += 6;
            }
            _ => {}
        }
    }
    true
}

fn hex4(b: &[u8], at: usize) -> Option<u32> {
    b.get(at..at + 4)?
        .iter()
        .try_fold(0, |n, &c| Some(n * 16 + char::from(c).to_digit(16)?))
}

/// Makes error text safe to store, so that a long message can't cost the event.
pub(crate) fn clean_message(message: &str) -> String {
    let mut text = message.replace('\0', "");
    text.truncate(text.floor_char_boundary(MAX_MESSAGE));
    text
}

/// Encodes the allowed keys of props and returns the names of the others.
pub(crate) fn filter_props(
    mut props: Props,
    allowed: &HashSet<String>,
) -> Result<(Option<Box<RawValue>>, Vec<String>), Error> {
    let mut dropped = Vec::new();
    props.retain(|key, _| {
        let keep = allowed.contains(key);
        if !keep {
            dropped.push(key.clone());
        }
        keep
    });
    if props.is_empty() {
        return Ok((None, dropped));
    }
    let raw = serde_json::to_string(&props)
        .and_then(RawValue::from_string)
        .map_err(|e| Error::Invalid(format!("beacon: props: {e}")))?;
    Ok((Some(raw), dropped))
}

/// Formats t as RFC 3339 in UTC, to the millisecond.
pub(crate) fn format_time(t: SystemTime) -> String {
    let since = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = since.as_secs();
    let (year, month, day) = civil_from_days(secs / 86_400);
    let (hour, minute, second) = (secs % 86_400 / 3600, secs % 3600 / 60, secs % 60);
    let millis = since.subsec_millis();
    let fraction = if millis == 0 {
        String::new()
    } else {
        format!(".{millis:03}")
    };
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{fraction}Z")
}

/// Converts days since 1970-01-01 to a Gregorian date. Eras are 400 years long
/// and start on March 1, so that the leap day comes last.
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z % 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    (year, month, day)
}
