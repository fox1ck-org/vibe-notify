// Package outbox gives notifications a durability tier above best-effort HTTP.
//
// The distinction that matters: fire-and-forget is correct when losing the
// notification costs nothing because the state is visible elsewhere, and wrong
// when a person is waiting on it. An order nobody was told about is an order
// nobody fulfils; an onboarding parked on a card nobody was told about stalls
// until somebody notices by accident. Those need at-least-once.
//
// The row is written in the SAME transaction as the state change, so the two
// cannot disagree: if the order was created, the notification exists, and if
// the transaction rolled back, so did the notification. A drainer then posts it
// with backoff. Because the envelope id is a UUID minted at insert time and the
// receiving side dedupes on it, at-least-once delivery is idempotent end to end.
//
// The shape is deliberately the one already proven in landing-studio's
// export_outbox: an advisory lock elects a single drainer, and the batch is
// claimed with FOR UPDATE SKIP LOCKED.
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	notify "github.com/fox1ck-org/vibe-notify"
)

// Tx is satisfied by *sql.Tx and by pgx via a thin adapter, so a caller can
// enlist the outbox write in whatever transaction it already has open.
type Tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Enqueue writes the envelope inside the caller's transaction. The table name
// is a parameter because each service owns its own copy in its own schema —
// deliberately not a shared database.
func Enqueue(ctx context.Context, tx Tx, table string, env notify.Envelope) error {
	if err := env.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: marshal: %w", err)
	}
	_, err = tx.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s (envelope, status, next_attempt_at) VALUES ($1, 'PENDING', now())`, table),
		payload)
	if err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	return nil
}

// DB is the subset of *sql.DB the drainer needs.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type Config struct {
	Table string
	// Distinct per service. Two drainers sharing a key would starve each other.
	AdvisoryLockKey int64
	BatchSize       int
	Interval        time.Duration
	MaxAttempts     int
	Logger          *slog.Logger
}

func (c *Config) withDefaults() {
	if c.BatchSize == 0 {
		c.BatchSize = 50
	}
	if c.Interval == 0 {
		c.Interval = 5 * time.Second
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 8
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type Drainer struct {
	db     DB
	client *notify.Client
	cfg    Config
}

func NewDrainer(db DB, client *notify.Client, cfg Config) *Drainer {
	cfg.withDefaults()
	return &Drainer{db: db, client: client, cfg: cfg}
}

// Run drains until the context is cancelled. Safe to start on every replica:
// the advisory lock means only one actually drains, and the others idle.
func (d *Drainer) Run(ctx context.Context) {
	t := time.NewTicker(d.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := d.drainOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				d.cfg.Logger.Warn("outbox drain failed", "error", err)
			}
		}
	}
}

type row struct {
	id       string
	envelope []byte
	attempts int
}

func (d *Drainer) drainOnce(ctx context.Context) error {
	// A session-scoped lock would leak forever through a connection pooler and
	// silently wedge the drain; the xact-scoped one is released with the
	// transaction no matter how it ends.
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var got bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock($1)`, d.cfg.AdvisoryLockKey).Scan(&got); err != nil {
		return err
	}
	if !got {
		return nil // another replica is draining
	}

	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, envelope, attempts
		FROM %s
		WHERE status IN ('PENDING','FAILED') AND next_attempt_at <= now()
		ORDER BY next_attempt_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, d.cfg.Table), d.cfg.BatchSize)
	if err != nil {
		return err
	}
	var batch []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.envelope, &r.attempts); err != nil {
			_ = rows.Close()
			return err
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()

	for _, r := range batch {
		var env notify.Envelope
		if err := json.Unmarshal(r.envelope, &env); err != nil {
			// Unparseable rows can never succeed; retrying one forever would
			// hold up everything behind it.
			d.markDead(ctx, tx, r.id, fmt.Sprintf("unmarshal: %v", err))
			continue
		}
		if err := d.client.Emit(ctx, env); err != nil {
			d.markFailed(ctx, tx, r, err)
			continue
		}
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf(`UPDATE %s SET status='DONE', updated_at=now() WHERE id=$1`, d.cfg.Table), r.id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *Drainer) markFailed(ctx context.Context, tx *sql.Tx, r row, cause error) {
	attempts := r.attempts + 1
	if attempts >= d.cfg.MaxAttempts {
		d.markDead(ctx, tx, r.id, cause.Error())
		return
	}
	// Exponential backoff, capped so a long outage does not push the next
	// attempt beyond the point where the notification still means anything.
	delay := time.Duration(math.Min(math.Pow(2, float64(attempts)), 300)) * time.Second
	_, _ = tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s SET status='FAILED', attempts=$2, last_error=$3,
		       next_attempt_at = now() + $4::interval, updated_at=now()
		WHERE id=$1`, d.cfg.Table),
		r.id, attempts, cause.Error(), fmt.Sprintf("%d seconds", int(delay.Seconds())))
	d.cfg.Logger.Warn("outbox delivery failed", "id", r.id, "attempts", attempts, "error", cause)
}

func (d *Drainer) markDead(ctx context.Context, tx *sql.Tx, id, cause string) {
	_, _ = tx.ExecContext(ctx,
		fmt.Sprintf(`UPDATE %s SET status='DEAD', last_error=$2, updated_at=now() WHERE id=$1`, d.cfg.Table),
		id, cause)
	// DEAD means a human has to look. Alert on the count, or this is a silent
	// hole exactly where the durable tier was supposed to be.
	d.cfg.Logger.Error("outbox row is dead — a notification will never be delivered", "id", id, "cause", cause)
}
