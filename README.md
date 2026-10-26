# vibe-notify

The Go emitter for the platform notification hub. A service calls it to say
*something happened and these people should know*; everything after that —
channels, per-user preferences, quiet hours, modes, sound, deduplication,
threading — belongs to the hub.

## How to connect a product

1. **Take the module.** `go get github.com/fox1ck-org/vibe-notify@v0.4.0` (public; the
   default `GOPROXY` resolves it — no `GOPRIVATE` needed).
2. **Wire the env.** Set `NOTIFY_HUB_URL` (and `NOTIFY_HMAC_SECRET` when the hub
   checks signatures) in the product's manifest in vibe-infra; the secret comes from
   ESO (`/vibe/prod/<app>`), never a literal. Unset means notifications are no-ops.
3. **Build the client once** at startup:
   ```go
   notifier := notify.NewClientFromEnv("vibe-cards", logger)
   ```
   (`notify.NewClient(url, source, logger, opts...)` stays for explicit wiring.)
4. **Emit** with an `event_type` from the catalog (see below):
   ```go
   env := notifier.NewEnvelope("cards.card.frozen", recipients) // may be nil for a shared-only org event
   env.OrgID, env.Visibility = orgID, notify.VisibilityOrg            // only org events reach shared destinations
   env.Category, env.Priority = "card", notify.PriorityWarning
   env.Title = "Card frozen"
   env.Deeplink = "https://cards.igrudsky.dev/cards/" + id
   env.Resource = notify.Res("card", id, "Visa *4242", env.Deeplink)
   env.Facts = []notify.Fact{notify.F("Balance", "$12.40")}         // ≤ 10
   env.DestinationIDs = rule.DestinationIDs                          // optional: org destinations the source itself picked (≤ 10)
   env.Actions = []notify.Action{                                     // ≤ 4
       notify.Link("open", "Open", env.Deeplink).Primary(),
       notify.Callback("unfreeze", "Unfreeze"),                       // hub calls the product back
   }
   notifier.EmitAsync(ctx, env) // or outbox.Enqueue when a person is waiting — see below
   ```
5. **Test it** against the fake hub:
   ```go
   hub := notifytest.New(t)
   svc := cards.New(hub.Client("vibe-cards"))
   // ... act ...
   hub.RequireEmitted(t, "cards.card.frozen", notifytest.Org(orgID), notifytest.HasAction("unfreeze"))
   ```

`Validate` mirrors the hub's Zod schema (`vibe-platform`
`packages/notifications/src/types/event.ts`), limits included: `recipients` may be
empty only with `org_id` set and `visibility = "org"`; resource kind ≤ 64 / id ≤ 160 /
label ≤ 200; fact label ≤ 64 / value ≤ 500; action id ≤ 64 / label ≤ 40, unique ids,
style `primary|danger|default`; URLs absolute. Change both sides in lockstep.

## Why this is its own module

Consumers of `vibe-common` currently span **v1.2.0 to v1.23.0**, and two of the
services that need to emit don't depend on it at all. Putting an emitter there
would force a fleet-wide version bump — dragging 21 minor versions of unrelated
auth/keycloak/repository churn into vibe-fb — for the sake of a 200-line HTTP
client. This module's build graph is stdlib plus `uuid`, so taking it costs a
service nothing.

## `NewClient("")` returns nil, and that is deliberate

Every method is nil-safe. A service with no `NOTIFY_HUB_URL` configured runs
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
