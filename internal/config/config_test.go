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

func TestBrand(t *testing.T) {
	t.Setenv("BEACON_BRAND_NAME", "Acme")
	t.Setenv("BEACON_BRAND_DIR", "/etc/beacon/brand")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.BrandName != "Acme" || c.BrandDir != "/etc/beacon/brand" {
		t.Errorf("got %q, %q", c.BrandName, c.BrandDir)
	}
}

func TestFooterSwitches(t *testing.T) {
	for _, sw := range []struct {
		env string
		get func(Config) bool
	}{
		{"BEACON_SHOW_VERSION", func(c Config) bool { return c.ShowVersion }},
		{"BEACON_SHOW_TIMING", func(c Config) bool { return c.ShowTiming }},
	} {
		t.Run(sw.env, func(t *testing.T) {
			for _, tc := range []struct {
				v    string
				want bool
			}{{"", true}, {"true", true}, {"1", true}, {"false", false}, {"0", false}} {
				t.Setenv(sw.env, tc.v)
				c, err := Load()
				if err != nil || sw.get(c) != tc.want {
					t.Errorf("%q: %v, %v; want %v", tc.v, sw.get(c), err, tc.want)
				}
			}
			t.Setenv(sw.env, "off")
			if _, err := Load(); err == nil {
				t.Error("off: no error")
			}
		})
	}
}

func TestIngestRates(t *testing.T) {
	for _, tc := range []struct {
		env      string
		fallback int
		get      func(Config) int
	}{
		{"BEACON_INGEST_IP_RATE", 1, func(c Config) int { return c.IngestIPRate }},
		{"BEACON_INGEST_KEY_RATE", 50, func(c Config) int { return c.IngestKeyRate }},
	} {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv(tc.env, "")
			c, err := Load()
			if err != nil || tc.get(c) != tc.fallback {
				t.Errorf("default: %d, %v", tc.get(c), err)
			}

			t.Setenv(tc.env, "200")
			c, err = Load()
			if err != nil || tc.get(c) != 200 {
				t.Errorf("set: %d, %v", tc.get(c), err)
			}

			for _, v := range []string{"0", "-1", "many"} {
				t.Setenv(tc.env, v)
				if _, err := Load(); err == nil {
					t.Errorf("%q: no error", v)
				}
			}
		})
	}
}

func TestRetentionDays(t *testing.T) {
	for _, tc := range []struct {
		v    string
		want int
	}{{"", 90}, {"30", 30}, {"0", 0}, {"36500", 36500}} {
		t.Setenv("BEACON_RETENTION_DAYS", tc.v)
		c, err := Load()
		if err != nil || c.RetentionDays != tc.want {
			t.Errorf("%q: %d, %v; want %d", tc.v, c.RetentionDays, err, tc.want)
		}
	}
	for _, v := range []string{"-1", "forever", "36501", "213504"} {
		t.Setenv("BEACON_RETENTION_DAYS", v)
		if _, err := Load(); err == nil {
			t.Errorf("%q: no error", v)
		}
	}
}
