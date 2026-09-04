package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	notify "github.com/fox1ck-org/vibe-notify"
	_ "github.com/lib/pq"
)

// These exercise the durable tier against a real Postgres, because the parts
// that matter — advisory locking, SKIP LOCKED, backoff arithmetic — are exactly
// the parts a fake would not reproduce.
//
//	TEST_DATABASE_URL=postgres://... go test ./outbox/
//
// Skipped without it rather than silently passing: project history has a case
// where CI ran SQL tests with no database and reported green for weeks.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping outbox integration tests")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if _, err := db.Exec(string(schema)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	if _, err := db.Exec("TRUNCATE notification_outbox"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return db
}

func envelope() notify.Envelope {
	return notify.Envelope{
		ID:         "11111111-1111-1111-1111-111111111111",
		Source:     "vibe-accounts",
		EventType:  "accounts.request.opened",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Recipients: []string{"22222222-2222-2222-2222-222222222222"},
		Priority:   notify.PriorityWarning,
		Category:   "request",
		Title:      "New order to fulfil",
		Deeplink:   "https://accounts.igrudsky.dev/requests/42",
	}
}

func count(t *testing.T, db *sql.DB, status string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM notification_outbox WHERE status=$1`, status).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestEnqueueIsTransactional(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	// The whole point of the outbox: a rolled-back business transaction must
	// not leave a notification claiming something happened.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Enqueue(ctx, tx, "notification_outbox", envelope()); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := count(t, db, "PENDING"); got != 0 {
		t.Fatalf("a rolled-back transaction left %d rows", got)
	}

	tx2, _ := db.BeginTx(ctx, nil)
	if err := Enqueue(ctx, tx2, "notification_outbox", envelope()); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := count(t, db, "PENDING"); got != 1 {
		t.Fatalf("committed transaction left %d rows, want 1", got)
	}
}

func TestEnqueueRejectsAnInvalidEnvelope(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	tx, _ := db.BeginTx(ctx, nil)
	defer func() { _ = tx.Rollback() }()

	bad := envelope()
	bad.Title = ""
	if err := Enqueue(ctx, tx, "notification_outbox", bad); err == nil {
		t.Fatal("expected validation to reject before the row is written")
	}
}

func TestDrainDeliversAndMarksDone(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	var delivered int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered++
		w.WriteHeader(202)
	}))
	defer srv.Close()

	tx, _ := db.BeginTx(ctx, nil)
	_ = Enqueue(ctx, tx, "notification_outbox", envelope())
	_ = tx.Commit()

	d := NewDrainer(db, notify.NewClient(srv.URL, "vibe-accounts", nil), Config{
		Table: "notification_outbox", AdvisoryLockKey: 991001,
	})
	if err := d.drainOnce(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if delivered != 1 {
		t.Fatalf("delivered %d times, want 1", delivered)
	}
	if got := count(t, db, "DONE"); got != 1 {
		t.Fatalf("DONE rows = %d, want 1", got)
	}
}

func TestDrainBacksOffOnFailure(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	tx, _ := db.BeginTx(ctx, nil)
	_ = Enqueue(ctx, tx, "notification_outbox", envelope())
	_ = tx.Commit()

	d := NewDrainer(db, notify.NewClient(srv.URL, "vibe-accounts", nil), Config{
		Table: "notification_outbox", AdvisoryLockKey: 991002,
	})
	if err := d.drainOnce(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := count(t, db, "FAILED"); got != 1 {
		t.Fatalf("FAILED rows = %d, want 1", got)
	}

	var attempts int
	var future bool
	if err := db.QueryRow(
		`SELECT attempts, next_attempt_at > now() FROM notification_outbox LIMIT 1`,
	).Scan(&attempts, &future); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if !future {
		t.Fatal("next_attempt_at should be pushed into the future, or the drainer will spin")
	}
}

func TestDrainGivesUpAfterMaxAttempts(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	tx, _ := db.BeginTx(ctx, nil)
	_ = Enqueue(ctx, tx, "notification_outbox", envelope())
	_ = tx.Commit()
	// Pretend it has already been tried to the limit.
	if _, err := db.Exec(`UPDATE notification_outbox SET attempts = 7`); err != nil {
		t.Fatal(err)
	}

	d := NewDrainer(db, notify.NewClient(srv.URL, "vibe-accounts", nil), Config{
		Table: "notification_outbox", AdvisoryLockKey: 991003, MaxAttempts: 8,
	})
	_ = d.drainOnce(ctx)

	if got := count(t, db, "DEAD"); got != 1 {
		t.Fatalf("DEAD rows = %d, want 1 — a row must not retry forever", got)
	}
}

func TestUnparseableRowGoesDeadRatherThanBlocking(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(202)
	}))
	defer srv.Close()

	// A row that can never be delivered must not hold up everything behind it.
	if _, err := db.Exec(`INSERT INTO notification_outbox (envelope) VALUES ('"not an object"'::jsonb)`); err != nil {
		t.Fatal(err)
	}
	tx, _ := db.BeginTx(ctx, nil)
	_ = Enqueue(ctx, tx, "notification_outbox", envelope())
	_ = tx.Commit()

	d := NewDrainer(db, notify.NewClient(srv.URL, "vibe-accounts", nil), Config{
		Table: "notification_outbox", AdvisoryLockKey: 991004,
	})
	if err := d.drainOnce(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := count(t, db, "DEAD"); got != 1 {
		t.Fatalf("DEAD = %d, want 1", got)
	}
	if got := count(t, db, "DONE"); got != 1 {
		t.Fatalf("DONE = %d — the good row must still go through", got)
	}
}

func TestEnvelopeSurvivesRoundTrip(t *testing.T) {
	// The threading fields are the ones most likely to be dropped silently by
	// a marshalling mistake, and their absence is invisible until a badge is
	// wrong weeks later.
	e := envelope()
	e.ThreadKey = "accounts.request:42"
	e.CollapseKey = "accounts.request:42:status"
	e.Tone = notify.ToneSuccess
	e.SoundClass = notify.SoundAlert

	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var back notify.Envelope
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.ThreadKey != e.ThreadKey || back.CollapseKey != e.CollapseKey ||
		back.Tone != e.Tone || back.SoundClass != e.SoundClass {
		t.Fatalf("threading fields lost in round trip: %+v", back)
	}
}
