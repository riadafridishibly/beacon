use std::ffi::OsString;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use serde::de::IgnoredAny;

use crate::lock;

const SEGMENT_EXT: &str = ".ndjson";

/// One file of newline-separated events. A sealed segment is one request to
/// the server.
struct Segment {
    seq: u64,
    path: PathBuf,
    size: u64,
    events: usize,
}

/// A segment handed to the sender. The size cap may delete it at any time.
#[derive(Clone)]
pub(crate) struct SegmentRef {
    pub seq: u64,
    pub path: PathBuf,
}

struct State {
    segs: Vec<Segment>,   // oldest first
    active: Option<File>, // the open last segment, None when all are sealed
    total: u64,
    next: u64,
}

/// Stores events on disk until the server accepts them. Writes are not synced:
/// events survive a crash of the app, but may not survive one of the operating
/// system.
pub(crate) struct Queue {
    dir: PathBuf,
    max_bytes: u64,
    max_events: usize,
    batch_bytes: u64,
    on_drop: Box<dyn Fn(usize) + Send + Sync>,
    state: Mutex<State>,
}

impl Queue {
    pub fn open(
        dir: &Path,
        max_bytes: u64,
        max_events: usize,
        batch_bytes: u64,
        on_drop: Box<dyn Fn(usize) + Send + Sync>,
    ) -> io::Result<Self> {
        create_private_dir(dir)?;
        let mut segs = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".tmp") {
                let _ = fs::remove_file(&path);
                continue;
            }
            let Ok(seq) = name.trim_end_matches(SEGMENT_EXT).parse::<u64>() else {
                continue;
            };
            let size = entry.metadata()?.len();
            segs.push(Segment {
                seq,
                path,
                size,
                events: 0,
            });
        }
        segs.sort_by_key(|s| s.seq);
        let total = segs.iter().map(|s| s.size).sum();
        let next = segs.last().map_or(0, |s| s.seq + 1);
        Ok(Self {
            dir: dir.to_path_buf(),
            max_bytes,
            max_events,
            batch_bytes: batch_bytes.min(max_bytes),
            on_drop,
            state: Mutex::new(State {
                segs,
                active: None,
                total,
                next,
            }),
        })
    }

    /// Appends one encoded event and reports whether a segment is ready to send.
    pub fn add(&self, line: &[u8]) -> (bool, io::Result<()>) {
        let mut st = lock(&self.state);
        let n = line.len() as u64 + 1;
        let mut ready = false;
        if st.active.is_some()
            && st
                .segs
                .last()
                .is_some_and(|s| s.size + n > self.batch_bytes)
        {
            st.active = None;
            ready = true;
        }
        if st.active.is_none()
            && let Err(e) = self.create(&mut st)
        {
            return (ready, Err(e));
        }

        let mut data = Vec::with_capacity(line.len() + 1);
        data.extend_from_slice(line);
        data.push(b'\n');
        let written = st.active.as_mut().map_or(Ok(()), |f| f.write_all(&data));
        if let Err(e) = written {
            st.active = None; // the next event must not continue a torn line
            return (true, Err(e));
        }

        st.total += n;
        let mut full = false;
        if let Some(seg) = st.segs.last_mut() {
            seg.size += n;
            seg.events += 1;
            full = seg.events >= self.max_events;
        }
        if full {
            st.active = None;
            ready = true;
        }
        while st.total > self.max_bytes && st.segs.len() > 1 {
            self.drop_oldest(&mut st);
        }
        (ready, Ok(()))
    }

    fn create(&self, st: &mut State) -> io::Result<()> {
        let path = self.dir.join(format!("{:020}{SEGMENT_EXT}", st.next));
        let file = private_file_options()
            .create_new(true)
            .append(true)
            .open(&path)?;
        st.segs.push(Segment {
            seq: st.next,
            path,
            size: 0,
            events: 0,
        });
        st.next += 1;
        st.active = Some(file);
        Ok(())
    }

    pub fn seal(&self) {
        lock(&self.state).active = None;
    }

    /// Seals the open segment unless older segments are still waiting. Sealing
    /// on every tick while offline would leave one small file per interval.
    pub fn seal_idle(&self) {
        let mut st = lock(&self.state);
        if st.segs.len() == 1 {
            st.active = None;
        }
    }

    /// Returns the first sealed segment, if there is one.
    pub fn oldest(&self) -> Option<SegmentRef> {
        let st = lock(&self.state);
        if st.active.is_some() && st.segs.len() == 1 {
            return None;
        }
        st.segs.first().map(|s| SegmentRef {
            seq: s.seq,
            path: s.path.clone(),
        })
    }

    /// Removes a segment that needs no more sending. The size cap may have
    /// already removed it.
    pub fn remove(&self, seq: u64) {
        Self::remove_locked(&mut lock(&self.state), seq);
    }

    fn remove_locked(st: &mut MutexGuard<'_, State>, seq: u64) -> bool {
        let Some(i) = st.segs.iter().position(|s| s.seq == seq) else {
            return false;
        };
        let seg = st.segs.remove(i);
        let _ = fs::remove_file(&seg.path);
        st.total = st.total.saturating_sub(seg.size);
        true
    }

    fn drop_oldest(&self, st: &mut MutexGuard<'_, State>) {
        let Some((seq, path)) = st.segs.first().map(|s| (s.seq, s.path.clone())) else {
            return;
        };
        let events = read_lines(&path).map_or(0, |lines| lines.len());
        if Self::remove_locked(st, seq) {
            (self.on_drop)(events);
        }
    }

    /// Replaces the contents of a segment, atomically so that a crash leaves
    /// either version.
    pub fn rewrite(&self, seq: u64, lines: &[Vec<u8>]) -> io::Result<()> {
        let mut st = lock(&self.state);
        let Some(i) = st.segs.iter().position(|s| s.seq == seq) else {
            return Ok(());
        };
        let mut data = lines.join(&b'\n');
        data.push(b'\n');
        let size = data.len() as u64;
        let State { segs, total, .. } = &mut *st;
        let Some(seg) = segs.get_mut(i) else {
            return Ok(());
        };
        write_file_atomic(&seg.path, &data)?;
        *total = (*total + size).saturating_sub(seg.size);
        seg.size = size;
        seg.events = lines.len();
        Ok(())
    }

    #[cfg(test)]
    pub fn total(&self) -> u64 {
        lock(&self.state).total
    }

    #[cfg(test)]
    pub fn sizes(&self) -> Vec<u64> {
        lock(&self.state).segs.iter().map(|s| s.size).collect()
    }
}

/// Returns the events in the file. A line cut short by a crash is skipped, and
/// a file the size cap removed has none.
pub(crate) fn read_lines(path: &Path) -> io::Result<Vec<Vec<u8>>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    Ok(bytes
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty() && serde_json::from_slice::<IgnoredAny>(line).is_ok())
        .map(<[u8]>::to_vec)
        .collect())
}

pub(crate) fn write_file_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let mut tmp = OsString::from(path);
    tmp.push(".tmp");
    let mut file = private_file_options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    file.write_all(data)?;
    drop(file);
    fs::rename(&tmp, path)
}

fn private_file_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options
}

pub(crate) fn create_private_dir(dir: &Path) -> io::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir)
}
