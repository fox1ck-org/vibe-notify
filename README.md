# vibe-notify

The Go emitter for the platform notification hub. A service calls it to say
*something happened and these people should know*; everything after that —
channels, per-user preferences, quiet hours, modes, sound, deduplication,
threading — belongs to the hub.

```go
notifier := notify.NewClient(os.Getenv("PLATFORM_API_URL"), "vibe-accounts", logger)

env := notifier.NewEnvelope("accounts.request.opened", recipients)
env.Priority = notify.PriorityWarning
env.Category = "request"
env.Title = "New order to fulfil"
env.Deeplink = "https://accounts.igrudsky.dev/requests/" + id
env.ThreadKey = "accounts.request:" + id
notifier.EmitAsync(ctx, env)
```

## Why this is its own module

Consumers of `vibe-common` currently span **v1.2.0 to v1.23.0**, and two of the
services that need to emit don't depend on it at all. Putting an emitter there
would force a fleet-wide version bump — dragging 21 minor versions of unrelated
auth/keycloak/repository churn into vibe-fb — for the sake of a 200-line HTTP
client. This module's build graph is stdlib plus `uuid`, so taking it costs a
service nothing.

## `NewClient("")` returns nil, and that is deliberate

Every method is nil-safe. A service with no `PLATFORM_API_URL` configured runs
locally and in CI with notifications as no-ops rather than errors. Don't "fix"
this by returning a stub.

## Two reliability tiers

`EmitAsync` is fire-and-forget: it logs a failure and never blocks the business
operation. Correct when losing the notification costs nothing because the state
is visible elsewhere.

`outbox` is for when a **person is waiting**. The row is written in the same
transaction as the state change, so the two cannot disagree: if the order was
created the notification exists, and if the transaction rolled back so did the
notification. A drainer then posts it with exponential backoff, elected by an
xact-scoped advisory lock and claiming work with `FOR UPDATE SKIP LOCKED`.
Because the envelope id is a UUID minted at insert time and the hub dedupes on
it, at-least-once delivery is idempotent end to end.

An order nobody was told about is an order nobody fulfils. Use the outbox there.

```go
tx, _ := db.BeginTx(ctx, nil)
// ... write the state change ...
outbox.Enqueue(ctx, tx, "notification_outbox", env)
tx.Commit()
```

Copy `outbox/schema.sql` into a migration in your own schema — each service
keeps its own table, because the row has to be written in the database that
holds the state it describes.

## Event types come from the catalog

`event_type` strings are defined once in `vibe-platform/catalog/*.yaml` and
generated into each emitting repo as `internal/notify/catalog_gen.go`. Don't
hand-write them: the vocabulary already drifted into three disagreeing copies
once, and the generated file plus a CI drift check is what stops it happening
again.

## Testing

```
go test ./...
TEST_DATABASE_URL=postgres://... go test ./outbox/    # integration
```

The outbox tests skip without `TEST_DATABASE_URL` rather than passing silently.
