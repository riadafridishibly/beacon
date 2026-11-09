package beacon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const segmentExt = ".ndjson"

// segment is one file of newline-separated events. A sealed segment is one
// request to the server.
type segment struct {
	path   string
	size   int64
	events int
}

// queue stores events on disk until the server accepts them. Writes are not
// synced: events survive a crash of the app, but may not survive one of the
// operating system.
type queue struct {
	dir        string
	maxBytes   int64
	maxEvents  int
	batchBytes int64
	onDrop     func(events int64)

	mu     sync.Mutex
	segs   []*segment // oldest first
	active *os.File   // the open last segment, nil when all are sealed
	total  int64
	next   uint64
}

func openQueue(dir string, maxBytes int64, maxEvents int, batchBytes int64, onDrop func(int64)) (*queue, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir) // sorted by name, and names sort by age
	if err != nil {
		return nil, err
	}
	q := &queue{dir: dir, maxBytes: maxBytes, maxEvents: maxEvents, batchBytes: min(batchBytes, maxBytes), onDrop: onDrop}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if strings.HasSuffix(entry.Name(), ".tmp") {
			_ = os.Remove(path)
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), segmentExt), 10, 64)
		if err != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		q.segs = append(q.segs, &segment{path: path, size: info.Size()})
		q.total += info.Size()
		q.next = seq + 1
	}
	return q, nil
}

// add appends one encoded event and reports whether a segment is ready to send.
func (q *queue) add(line []byte) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	n := int64(len(line)) + 1
	ready := false
	if q.active != nil && q.segs[len(q.segs)-1].size+n > q.batchBytes {
		q.sealLocked()
		ready = true
	}
	if q.active == nil {
		if err := q.createLocked(); err != nil {
			return ready, err
		}
	}
	seg := q.segs[len(q.segs)-1]
	if _, err := q.active.Write(append(line[:len(line):len(line)], '\n')); err != nil {
		q.sealLocked() // the next event must not continue a torn line
		return true, err
	}
	seg.size += n
	seg.events++
	q.total += n
	if seg.events >= q.maxEvents {
		q.sealLocked()
		ready = true
	}
	for q.total > q.maxBytes && len(q.segs) > 1 {
		q.dropLocked(q.segs[0])
	}
	return ready, nil
}

func (q *queue) createLocked() error {
	path := filepath.Join(q.dir, fmt.Sprintf("%020d%s", q.next, segmentExt))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	q.next++
	q.active = f
	q.segs = append(q.segs, &segment{path: path})
	return nil
}

func (q *queue) sealLocked() {
	if q.active != nil {
		_ = q.active.Close()
		q.active = nil
	}
}

func (q *queue) seal() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.sealLocked()
}

// sealIdle seals the open segment unless older segments are still waiting.
// Sealing on every tick while offline would leave one small file per
// interval.
func (q *queue) sealIdle() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.segs) == 1 {
		q.sealLocked()
	}
}

// oldest returns the first sealed segment, nil when there is none.
func (q *queue) oldest() *segment {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.segs) == 0 || (q.active != nil && len(q.segs) == 1) {
		return nil
	}
	return q.segs[0]
}

// read returns the events of seg. A line cut short by a crash is skipped.
func (q *queue) read(seg *segment) ([][]byte, error) {
	b, err := os.ReadFile(seg.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var lines [][]byte
	for line := range bytes.SplitSeq(b, []byte("\n")) {
		if len(line) > 0 && json.Valid(line) {
			lines = append(lines, line)
		}
	}
	return lines, nil
}

// remove deletes seg once it needs no more sending. The size cap may have
// already removed it.
func (q *queue) remove(seg *segment) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.removeLocked(seg)
}

func (q *queue) removeLocked(seg *segment) bool {
	i := slices.Index(q.segs, seg)
	if i < 0 {
		return false
	}
	_ = os.Remove(seg.path)
	q.segs = slices.Delete(q.segs, i, i+1)
	q.total -= seg.size
	return true
}

func (q *queue) dropLocked(seg *segment) {
	lines, _ := q.read(seg)
	if q.removeLocked(seg) && q.onDrop != nil {
		q.onDrop(int64(len(lines)))
	}
}

// rewrite replaces the contents of seg, atomically so that a crash leaves
// either version.
func (q *queue) rewrite(seg *segment, lines [][]byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !slices.Contains(q.segs, seg) {
		return nil
	}
	data := append(bytes.Join(lines, []byte("\n")), '\n')
	if err := writeFileAtomic(seg.path, data); err != nil {
		return err
	}
	q.total += int64(len(data)) - seg.size
	seg.size = int64(len(data))
	seg.events = len(lines)
	return nil
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
