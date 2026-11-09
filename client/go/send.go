package beacon

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

const maxRetryAfterSeconds = 3600

type action string

const (
	actionAccept     action = "accept"
	actionDropEvent  action = "drop_event"
	actionDropBatch  action = "drop_batch"
	actionRetryAfter action = "retry_after"
	actionBackoff    action = "backoff"
)

// outcome says what to do with a batch after a response.
type outcome struct {
	action action
	index  int
	wait   time.Duration
}

// classify maps a response to an outcome. Events are dropped only on a 400
// in the server's own error format, so that an error page from a proxy
// can't destroy the queue. client/conformance/responses.json is the table
// every client must follow.
func classify(status int, retryAfter string, body []byte) outcome {
	switch status {
	case http.StatusAccepted:
		return outcome{action: actionAccept}
	case http.StatusBadRequest:
		var e struct {
			Error string `json:"error"`
			Index *int   `json:"index"`
		}
		if json.Unmarshal(body, &e) != nil || e.Error == "" {
			return outcome{action: actionBackoff}
		}
		if e.Index != nil {
			return outcome{action: actionDropEvent, index: *e.Index}
		}
		return outcome{action: actionDropBatch}
	case http.StatusTooManyRequests:
		secs, err := strconv.ParseUint(retryAfter, 10, 64)
		if errors.Is(err, strconv.ErrRange) || (err == nil && secs >= maxRetryAfterSeconds) {
			return outcome{action: actionRetryAfter, wait: maxRetryAfterSeconds * time.Second}
		}
		if err == nil && secs > 0 {
			return outcome{action: actionRetryAfter, wait: time.Duration(secs) * time.Second}
		}
	}
	return outcome{action: actionBackoff}
}

func encodeBatch(lines [][]byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	for _, part := range [][]byte{[]byte(`{"events":[`), bytes.Join(lines, []byte(",")), []byte(`]}`)} {
		if _, err := zw.Write(part); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Client) send(ctx context.Context, lines [][]byte) (outcome, error) {
	body, err := encodeBatch(lines)
	if err != nil {
		return outcome{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return outcome{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := c.http.Do(req)
	if err != nil {
		return outcome{}, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return outcome{}, err
	}
	if resp.StatusCode != http.StatusAccepted {
		c.logf("server returned %d: %.500s", resp.StatusCode, respBody)
	}
	return classify(resp.StatusCode, resp.Header.Get("Retry-After"), respBody), nil
}

// drain sends sealed segments oldest first until none are left or the
// server can't take more. It returns how long to wait before trying again.
func (c *Client) drain() time.Duration {
	for c.ctx.Err() == nil {
		seg := c.q.oldest()
		if seg == nil {
			return 0
		}
		lines, err := c.q.read(seg)
		if err != nil {
			c.logf("read queue: %v", err)
			return c.nextBackoff()
		}
		if len(lines) == 0 {
			c.q.remove(seg)
			continue
		}
		res, err := c.send(c.ctx, lines)
		if err != nil {
			c.logf("send: %v", err)
			return c.nextBackoff()
		}
		switch res.action {
		case actionAccept:
			c.failures = 0
			c.q.remove(seg)
		case actionDropBatch:
			c.failures = 0
			c.logf("dropped a batch of %d events the server rejected", len(lines))
			c.q.remove(seg)
		case actionDropEvent:
			c.failures = 0
			if err := c.dropEvent(seg, lines, res.index); err != nil {
				c.logf("rewrite queue: %v", err)
				return c.nextBackoff()
			}
		case actionRetryAfter:
			return res.wait
		default:
			return c.nextBackoff()
		}
	}
	return 0
}

// dropEvent removes the event the server named, so the rest of the batch
// can be sent again. The server stores nothing from a rejected batch.
func (c *Client) dropEvent(seg *segment, lines [][]byte, index int) error {
	if index < 0 || index >= len(lines) || len(lines) == 1 {
		c.logf("dropped a batch of %d events the server rejected", len(lines))
		c.q.remove(seg)
		return nil
	}
	c.logf("dropped event %d of a batch the server rejected", index)
	rest := append(lines[:index:index], lines[index+1:]...)
	return c.q.rewrite(seg, rest)
}

// nextBackoff doubles from minBackoff up to maxBackoff, with jitter so a
// fleet that lost the server together doesn't return together.
func (c *Client) nextBackoff() time.Duration {
	d := c.maxBackoff
	if shift := c.failures; shift < 32 && c.minBackoff<<shift < c.maxBackoff {
		d = c.minBackoff << shift
	}
	c.failures++
	return d/2 + rand.N(d/2+1)
}
