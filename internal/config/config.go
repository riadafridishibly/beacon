// Package config reads BEACON_* environment variables.
package config

import (
	"fmt"
	"net/netip"
	"os"
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
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL: env("BEACON_DATABASE_URL", "postgres://localhost:5432/beacon?sslmode=disable"),
		Listen:      env("BEACON_LISTEN", "127.0.0.1:8080"),
		GeoIPPath:   os.Getenv("BEACON_GEOIP_DB"),
	}
	if os.Getenv("BEACON_DEV") == "1" {
		c.ViteURL = env("BEACON_VITE_URL", "http://localhost:8341")
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

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
