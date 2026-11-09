package config

import (
	"net/netip"
	"slices"
	"testing"
)

func TestTrustedProxies(t *testing.T) {
	t.Setenv("BEACON_TRUSTED_PROXIES", "127.0.0.1, 10.0.0.0/8 ,::1")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.Prefix{
		netip.MustParsePrefix("127.0.0.1/32"),
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("::1/128"),
	}
	if !slices.Equal(c.TrustedProxies, want) {
		t.Errorf("got %v, want %v", c.TrustedProxies, want)
	}

	t.Setenv("BEACON_TRUSTED_PROXIES", "nope")
	if _, err := Load(); err == nil {
		t.Error("bad entry: no error")
	}
}

func TestIngestKeyRate(t *testing.T) {
	t.Setenv("BEACON_INGEST_KEY_RATE", "")
	c, err := Load()
	if err != nil || c.IngestKeyRate != 50 {
		t.Errorf("default: %d, %v", c.IngestKeyRate, err)
	}

	t.Setenv("BEACON_INGEST_KEY_RATE", "200")
	c, err = Load()
	if err != nil || c.IngestKeyRate != 200 {
		t.Errorf("set: %d, %v", c.IngestKeyRate, err)
	}

	for _, v := range []string{"0", "-1", "many"} {
		t.Setenv("BEACON_INGEST_KEY_RATE", v)
		if _, err := Load(); err == nil {
			t.Errorf("%q: no error", v)
		}
	}
}
