package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/testdb"
)

func TestPassword(t *testing.T) {
	t.Parallel()
	hash, err := HashPassword("correct horse")
	if err != nil {
		t.Fatal(err)
	}
	for pw, want := range map[string]bool{"correct horse": true, "correct horsE": false, "": false} {
		ok, err := CheckPassword(hash, pw)
		if err != nil || ok != want {
			t.Errorf("CheckPassword(%q) = %v, %v; want %v", pw, ok, err, want)
		}
	}
	if _, err := CheckPassword("$bcrypt$x", "a"); err == nil {
		t.Error("malformed hash: want error")
	}
}

func TestSafeNext(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"":                 "/",
		"/apps/new":        "/apps/new",
		"/?a=1":            "/?a=1",
		"//evil.example":   "/",
		"/\\evil.example":  "/",
		"https://evil.com": "/",
		"/a\r\nSet-Cookie": "/",
		"/\t/evil.example": "/",
		"/a\x7fb":          "/",
	} {
		if got := safeNext(in); got != want {
			t.Errorf("safeNext(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLoginRejectsBadEmailsEarly(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	New(db.New(testdb.New(t))).Routes(mux)

	post := func(body string) int {
		r := httptest.NewRequest(http.MethodPost, LoginPath, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w.Code
	}
	cases := []string{
		"email=a%00b%40example.com&password=x",
		"email=nope&password=x",
		"email=" + strings.Repeat("a", 1000) + "%40example.com&password=x",
		"email=a%40example.com&password=" + strings.Repeat("x", maxLoginBody),
	}
	// More than a client's burst, so a 429 shows they reached the limiters.
	for i := range 40 {
		body := cases[i%len(cases)]
		if got := post(body); got != http.StatusUnprocessableEntity {
			t.Fatalf("%.60s: status %d", body, got)
		}
	}
}

func TestVerifyWaitsForASlot(t *testing.T) {
	t.Parallel()
	a := New(nil)
	for range maxHashing {
		a.hashing <- struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := a.verify(ctx, "", "x"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("all slots busy: err = %v", err)
	}

	<-a.hashing
	hash, err := HashPassword("right")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := a.verify(context.Background(), hash, "right"); !ok || err != nil {
		t.Errorf("free slot: %v, %v", ok, err)
	}
	if ok, err := a.verify(context.Background(), "", "right"); ok || err != nil {
		t.Errorf("unknown email: %v, %v", ok, err)
	}
	if got := len(a.hashing); got != maxHashing-1 {
		t.Errorf("%d slots held after verify, want %d", got, maxHashing-1)
	}
}

func TestCreateAndResetAccount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.New(t)
	q := db.New(pool)

	if _, err := CreateAccount(ctx, q, "not an email"); !errors.Is(err, ErrBadEmail) {
		t.Errorf("bad email: err = %v", err)
	}
	pw, err := CreateAccount(ctx, q, " Ops@Example.com ")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateAccount(ctx, q, "ops@example.com"); !errors.Is(err, ErrEmailTaken) {
		t.Errorf("duplicate: err = %v", err)
	}
	row, err := q.GetLogin(ctx, "ops@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := CheckPassword(row.PasswordHash, pw); !ok {
		t.Error("printed password doesn't match the stored hash")
	}

	token := hashToken("t")
	if err := q.CreateSession(ctx, db.CreateSessionParams{TokenHash: token, AccountID: row.ID, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	pw2, err := ResetPassword(ctx, pool, "OPS@example.com")
	if err != nil {
		t.Fatal(err)
	}
	row, _ = q.GetLogin(ctx, "ops@example.com")
	if ok, _ := CheckPassword(row.PasswordHash, pw2); !ok {
		t.Error("reset password doesn't match the stored hash")
	}
	if _, err := q.GetSession(ctx, token); err == nil {
		t.Error("reset kept the old session")
	}
	if _, err := ResetPassword(ctx, pool, "nobody@example.com"); !errors.Is(err, ErrNoAccount) {
		t.Errorf("unknown account: err = %v", err)
	}
}
