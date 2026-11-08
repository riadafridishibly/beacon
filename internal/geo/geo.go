// Package geo looks up the country and city of an IP address in a MaxMind
// format database, such as GeoLite2 City or DB-IP City Lite.
package geo

import (
	"net/netip"

	"github.com/oschwald/maxminddb-golang/v2"
)

// Locator looks up addresses. A nil *Locator finds nothing, for running
// without a database.
type Locator struct {
	r *maxminddb.Reader
}

// Open opens the database at path.
func Open(path string) (*Locator, error) {
	r, err := maxminddb.Open(path)
	if err != nil {
		return nil, err
	}
	return &Locator{r: r}, nil
}

func (l *Locator) Close() error {
	if l == nil {
		return nil
	}
	return l.r.Close()
}

type record struct {
	Country struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
}

// Lookup returns the ISO 3166-1 country code and the English city name of
// addr, each "" when unknown.
func (l *Locator) Lookup(addr netip.Addr) (country, city string) {
	if l == nil || !addr.IsValid() {
		return "", ""
	}
	var rec record
	if err := l.r.Lookup(addr.Unmap()).Decode(&rec); err != nil {
		return "", ""
	}
	return rec.Country.ISOCode, rec.City.Names["en"]
}
