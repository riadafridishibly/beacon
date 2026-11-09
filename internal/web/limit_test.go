package web

import (
	"testing"
	"time"
)

func TestLimiterTake(t *testing.T) {
	t.Parallel()
	l := NewLimiter(time.Hour, 2)
	for i := range 2 {
		if ok, wait := l.Take("a"); !ok || wait != 0 {
			t.Fatalf("request %d: %v, %v", i, ok, wait)
		}
	}
	ok, wait := l.Take("a")
	if ok || wait <= 59*time.Minute || wait > time.Hour {
		t.Errorf("over the burst: %v, %v; want a refusal with about an hour to wait", ok, wait)
	}
	if !l.Allow("b") {
		t.Error("a different key was refused")
	}

	l.max = len(l.buckets)
	if ok, wait := l.Take("c"); ok || wait != fullSweepEvery {
		t.Errorf("table full: %v, %v", ok, wait)
	}
}

func TestLimiterSweepsWhenFull(t *testing.T) {
	t.Parallel()
	l := NewLimiter(time.Nanosecond, 1)
	l.max = 1
	if !l.Allow("a") {
		t.Fatal("first key refused")
	}

	if ok, wait := l.Take("b"); ok || wait != fullSweepEvery {
		t.Errorf("swept too soon: %v, %v", ok, wait)
	}

	l.lastSweep = time.Now().Add(-2 * fullSweepEvery)
	if !l.Allow("b") {
		t.Error("full table of refilled buckets was not swept")
	}
	if _, tracked := l.buckets["a"]; tracked {
		t.Error("refilled bucket survived the sweep")
	}
}
