-- name: CreateAccount :one
INSERT INTO accounts (email, password_hash) VALUES (@email, @password_hash)
RETURNING id;

-- name: GetLogin :one
SELECT id, password_hash FROM accounts WHERE email = @email;

-- name: SetAccountPassword :execrows
UPDATE accounts SET password_hash = @password_hash WHERE email = @email;

-- name: CreateSession :exec
INSERT INTO sessions (token_hash, account_id, expires_at)
VALUES (@token_hash, @account_id, @expires_at);

-- name: GetSession :one
SELECT s.account_id, s.last_seen_at, a.email
FROM sessions s
JOIN accounts a ON a.id = s.account_id
WHERE s.token_hash = @token_hash AND s.expires_at > now();

-- name: TouchSession :exec
UPDATE sessions SET last_seen_at = now(), expires_at = @expires_at
WHERE token_hash = @token_hash;

-- name: DeleteSession :exec
DELETE FROM sessions WHERE token_hash = @token_hash;

-- name: DeleteExpiredSessions :exec
DELETE FROM sessions WHERE account_id = @account_id AND expires_at <= now();

-- name: DeleteAccountSessions :exec
DELETE FROM sessions
WHERE account_id = (SELECT id FROM accounts WHERE email = @email);
