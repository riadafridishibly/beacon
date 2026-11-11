// Package config reads BEACON_* environment variables.
package config

import (
	"fmt"
	"math"
	"net/netip"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DatabaseURL string
	Listen      string
	// GeoIPPath is a MaxMind format city database. Without it, events get
	// no country or city.
	GeoIPPath string
	// TrustedProxies are the networks whose X-Forwarded-For is believed.
	TrustedProxies []netip.Prefix
	// ViteURL is the Vite dev server the dashboard loads its CSS and JS
	// from, set when BEACON_DEV=1. Empty serves the embedded build.
	ViteURL string
	// IngestIPRate is the requests per second one client address can
	// sustain on ingest.
	IngestIPRate int
	// IngestKeyRate is the requests per second one ingest key can sustain.
	IngestKeyRate int
	// RetentionDays is how long events are kept, by their ts. 0 keeps them
	// forever.
	RetentionDays int
	// BrandName replaces Beacon as the name shown in the dashboard.
	BrandName string
	// BrandDir holds logo.svg and optionally favicon.svg, which replace the
	// built-in mark.
	BrandDir string
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL: env("BEACON_DATABASE_URL", "postgres://localhost:5432/beacon?sslmode=disable"),
		Listen:      env("BEACON_LISTEN", "127.0.0.1:8178"),
		GeoIPPath:   os.Getenv("BEACON_GEOIP_DB"),
		BrandName:   os.Getenv("BEACON_BRAND_NAME"),
		BrandDir:    os.Getenv("BEACON_BRAND_DIR"),
	}
	if os.Getenv("BEACON_DEV") == "1" {
		c.ViteURL = env("BEACON_VITE_URL", "http://localhost:8367")
	}
	var err error
	if c.IngestIPRate, err = intIn("BEACON_INGEST_IP_RATE", 1, 1, math.MaxInt32); err != nil {
		return Config{}, err
	}
	if c.IngestKeyRate, err = intIn("BEACON_INGEST_KEY_RATE", 50, 1, math.MaxInt32); err != nil {
		return Config{}, err
	}
	// The cap keeps the window, in nanoseconds, inside a time.Duration.
	if c.RetentionDays, err = intIn("BEACON_RETENTION_DAYS", 90, 0, 36500); err != nil {
		return Config{}, err
	}
	for _, s := range strings.Split(os.Getenv("BEACON_TRUSTED_PROXIES"), ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		p, err := netip.ParsePrefix(s)
		if err != nil {
			a, aerr := netip.ParseAddr(s)
			if aerr != nil {
				return Config{}, fmt.Errorf("BEACON_TRUSTED_PROXIES: %q is not an address or CIDR", s)
			}
			p = netip.PrefixFrom(a, a.BitLen())
		}
		c.TrustedProxies = append(c.TrustedProxies, p.Masked())
	}
	return c, nil
}

func intIn(key string, fallback, lo, hi int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < lo || n > hi {
		return 0, fmt.Errorf("%s: %q is not an integer from %d to %d", key, v, lo, hi)
	}
	return n, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
