// Package auth signs accounts in to the dashboard: password hashing,
// sessions in PostgreSQL, and the login page.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/mail"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riadafridishibly/beacon/internal/db"
	"github.com/riadafridishibly/beacon/internal/web"
)

const (
	// CookieName uses the __Host- prefix, so browsers only accept it with
	// Secure, Path=/ and no Domain. Browsers treat http://localhost as
	// secure, so it works in development too.
	CookieName = "__Host-session"
	sessionTTL = 30 * 24 * time.Hour
	touchEvery = time.Minute

	LoginPath  = "/login"
	LogoutPath = "/logout"
)

var (
	ErrEmailTaken = errors.New("an account with that email already exists")
	ErrBadEmail   = errors.New("not a valid email address")
	ErrNoAccount  = errors.New("no account with that email")
)

// Account is the signed-in account.
type Account struct {
	ID    int64
	Email string
}

type ctxKey struct{}

// From returns the signed-in account, or nil outside RequireLogin.
func From(ctx context.Context) *Account {
	a, _ := ctx.Value(ctxKey{}).(*Account)
	return a
}

// Nav is the layout header for the signed-in account in ctx.
func Nav(ctx context.Context) *web.Nav {
	a := From(ctx)
	if a == nil {
		return nil
	}
	return &web.Nav{Email: a.Email, LogoutPath: LogoutPath}
}

// Auth serves the login and logout routes and guards dashboard pages.
type Auth struct {
	q *db.Queries
	// A single IP can try one email at most once a minute after a short
	// burst, slower than byEmail refills, so one client can't lock an
	// account out. byEmail is the backstop against guessing from many IPs.
	byIP      *web.Limiter
	byIPEmail *web.Limiter
	byEmail   *web.Limiter
}

// New returns an Auth that keeps sessions through q.
func New(q *db.Queries) *Auth {
	return &Auth{
		q:         q,
		byIP:      web.NewLimiter(10*time.Second, 30),
		byIPEmail: web.NewLimiter(time.Minute, 5),
		byEmail:   web.NewLimiter(10*time.Second, 50),
	}
}

// Routes registers the login and logout routes on mux.
func (a *Auth) Routes(mux *http.ServeMux) {
	mux.Handle("GET "+LoginPath, web.Handle(a.loginForm))
	mux.Handle("POST "+LoginPath, web.Handle(a.login))
	mux.Handle("POST "+LogoutPath, web.Handle(a.logout))
}

// RequireLogin puts the signed-in account in the request context, or sends
// the browser to the login page.
func (a *Auth) RequireLogin(next http.Handler) http.Handler {
	return web.Handle(func(w http.ResponseWriter, r *http.Request) error {
		acc, err := a.load(w, r)
		if err != nil {
			return err
		}
		if acc == nil {
			target := LoginPath
			if r.Method == http.MethodGet {
				target += "?next=" + url.QueryEscape(r.URL.RequestURI())
			}
			web.Redirect(w, r, target)
			return nil
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, acc)))
		return nil
	})
}

// load returns the account for the session cookie, or nil if there is no
// valid session. It extends the session at most once a minute.
func (a *Auth) load(w http.ResponseWriter, r *http.Request) (*Account, error) {
	c, err := r.Cookie(CookieName)
	if errors.Is(err, http.ErrNoCookie) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	hash := hashToken(c.Value)
	row, err := a.q.GetSession(r.Context(), hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if time.Since(row.LastSeenAt) > touchEvery {
		err := a.q.TouchSession(r.Context(), db.TouchSessionParams{TokenHash: hash, ExpiresAt: time.Now().Add(sessionTTL)})
		if err != nil {
			return nil, err
		}
		setCookie(w, c.Value)
	}
	return &Account{ID: row.AccountID, Email: row.Email}, nil
}

func (a *Auth) loginForm(w http.ResponseWriter, r *http.Request) error {
	next := safeNext(r.URL.Query().Get("next"))
	acc, err := a.load(w, r)
	if err != nil {
		return err
	}
	if acc != nil {
		http.Redirect(w, r, next, http.StatusSeeOther)
		return nil
	}
	return web.Render(w, r, http.StatusOK, loginPage(loginView{Next: next}))
}

func (a *Auth) login(w http.ResponseWriter, r *http.Request) error {
	v := loginView{Email: normalizeEmail(r.PostFormValue("email")), Next: safeNext(r.PostFormValue("next"))}
	password := r.PostFormValue("password")

	ip := web.ClientKey(r)
	if !a.byIP.Allow(ip) || !a.byIPEmail.Allow(ip+" "+v.Email) || !a.byEmail.Allow(v.Email) {
		v.Error = "Too many sign-in attempts. Wait a few minutes and try again."
		return web.Render(w, r, http.StatusTooManyRequests, loginPage(v))
	}
	row, err := a.q.GetLogin(r.Context(), v.Email)
	ok := false
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		spendHashTime(password)
	case err != nil:
		return err
	default:
		if ok, err = CheckPassword(row.PasswordHash, password); err != nil {
			return err
		}
	}
	if !ok {
		v.Error = "Email or password is incorrect."
		return web.Render(w, r, http.StatusUnprocessableEntity, loginPage(v))
	}
	if err := a.q.DeleteExpiredSessions(r.Context(), row.ID); err != nil {
		return err
	}
	token := rand.Text()
	err = a.q.CreateSession(r.Context(), db.CreateSessionParams{
		TokenHash: hashToken(token),
		AccountID: row.ID,
		ExpiresAt: time.Now().Add(sessionTTL),
	})
	if err != nil {
		return err
	}
	setCookie(w, token)
	http.Redirect(w, r, v.Next, http.StatusSeeOther)
	return nil
}

func (a *Auth) logout(w http.ResponseWriter, r *http.Request) error {
	if c, err := r.Cookie(CookieName); err == nil {
		if err := a.q.DeleteSession(r.Context(), hashToken(c.Value)); err != nil {
			return err
		}
	}
	http.SetCookie(w, &http.Cookie{Name: CookieName, Path: "/", MaxAge: -1, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
	web.Redirect(w, r, LoginPath)
	return nil
}

// CreateAccount adds an account with a generated password and returns the
// password.
func CreateAccount(ctx context.Context, q *db.Queries, email string) (string, error) {
	email = normalizeEmail(email)
	if !validEmail(email) {
		return "", ErrBadEmail
	}
	password := rand.Text()
	hash, err := HashPassword(password)
	if err != nil {
		return "", err
	}
	_, err = q.CreateAccount(ctx, db.CreateAccountParams{Email: email, PasswordHash: hash})
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "23505" {
		return "", ErrEmailTaken
	}
	return password, err
}

// ResetPassword gives the account a new generated password, signs it out
// everywhere, and returns the password.
func ResetPassword(ctx context.Context, pool *pgxpool.Pool, email string) (string, error) {
	email = normalizeEmail(email)
	password := rand.Text()
	hash, err := HashPassword(password)
	if err != nil {
		return "", err
	}
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		n, err := q.SetAccountPassword(ctx, db.SetAccountPasswordParams{Email: email, PasswordHash: hash})
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNoAccount
		}
		return q.DeleteAccountSessions(ctx, email)
	})
	return password, err
}

func setCookie(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   int(sessionTTL / time.Second),
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func hashToken(token string) []byte {
	h := sha256.Sum256([]byte(token))
	return h[:]
}

func normalizeEmail(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

func validEmail(s string) bool {
	if len(s) > 254 {
		return false
	}
	a, err := mail.ParseAddress(s)
	return err == nil && a.Address == s && a.Name == ""
}

// safeNext keeps post-login redirects on this site.
func safeNext(next string) string {
	if strings.HasPrefix(next, "/") && !strings.HasPrefix(next, "//") && !strings.ContainsAny(next, "\\\r\n") {
		return next
	}
	return "/"
}
