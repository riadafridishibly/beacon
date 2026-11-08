package auth

import (
	"context"
	"errors"
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
	} {
		if got := safeNext(in); got != want {
			t.Errorf("safeNext(%q) = %q, want %q", in, got, want)
		}
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
