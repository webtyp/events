---
PLAN: "feat!: events v0.1.0 — Subscribe returns a Subscription (Cancel); events is the ecosystem's one pub/sub package and webtyp/bus is retired"
TAG: v0.1.0
EXECUTOR: jules
REVIEWER: none
---

> Read AGENTS.md at the repo root before any change.
>
> The previous content of this file (the `Fanout` plan) was already executed: `Fanout` is in `events.go`. It is replaced by this plan.

## Tools

```
go install webtyp.com/devflow/cmd/gotest@latest
```

## Why

The ecosystem has **two** pub/sub packages: `webtyp.com/events` (the contract, 20 dependent repos, an in-process `mock.Broker`,
`sse` adapters, a conformance suite) and `webtyp.com/bus` (an unused second hub: **no repo imports it**). Two packages for one
concern is exactly the duplication the ecosystem forbids. They are unified into `events`.

## Ecosystem search (done before writing this plan)

| Found | What it offers | Decision |
|---|---|---|
| `webtyp/events` | `Event`, `Handler`, `Publisher`, `Subscriber`, `Broker`, `Fanout`; `events/mock` (synchronous in-process broker); `events/conformance` | **the one package** |
| `webtyp/bus` | `Subscribe` returning a `Subscription` with `Cancel`; `Topics()`; `Close()`; asynchronous dispatch (a goroutine per handler); payload `binary.Message`. **No importer anywhere in `webtyp` or `veltylabs`** | **retired**; only what is useful moves |
| `webtyp/sse` | `Publisher` and `Subscriber` adapters over `events` (`Subscriber` is `//go:build wasm`) | consumer; adapted in its own plan after this tag |
| `webtyp/broker` | despite the bus doc's claim, an HTTP request mediator; unrelated | untouched |
| `webtyp/outbox`, `webtyp/idempotency` | a browser offline mutation queue and an HTTP middleware; unrelated | untouched |

What of `bus` is carried over, and what is not:

- **Carried: `Cancel`.** A component that subscribes while mounted and must stop when unmounted has no way to do it with
  `events` today (a `Subscribe` registers for the life of the broker). That is a real need.
- **Not carried: `Topics()`.** No consumer; introspection can be added when one exists.
- **Not carried: `Close()`.** A broker's lifetime belongs to whoever built it; `Cancel` covers a single registration.
- **Not carried: asynchronous dispatch.** `events` documents synchronous, in-order delivery (`mock.Broker`); a handler that
  must not block starts its own goroutine. Two delivery semantics in one contract would be a trap.
- **Not carried: `binary.Message` payloads.** The contract's payload is `model.Encodable`.

## Design gate

1. **Prior art.** (a) Node's `EventEmitter.on` returns the emitter, with `off`; RxJS `subscribe` returns a `Subscription` with
   `unsubscribe()`; Go's `context.AfterFunc` returns a `stop` function; `os/signal.Notify`/`Stop`. The Go-idiomatic and
   TinyGo-friendly form is a returned handle with one method. (b) The ecosystem's own `bus.Subscription` (`Cancel`) — the name is
   adopted. It differs from RxJS in having no `closed` state to query: not needed.
2. **Novice-name test.** `events.Subscription`, `Subscription.Cancel`. Read aloud: "subscribe, and keep the subscription so you
   can cancel it".
3. **Complexity ledger.**
   ```
   Concepts the developer must learn   +1 / −1   (Subscription added; "which bus do I use?" disappears)
   Files they must touch to do X       +0 / −0
   Lines at the call site              +0 / −0   (a caller that ignores the result compiles unchanged)
   Ways to do the same thing           +0 / −1   (one pub/sub package, not two)
   ```
4. **Where it belongs.** Here: the contract owns its handle; implementers (`mock.Broker` here, `sse.Subscriber` in `sse`) adopt it.
5. **What it deletes.** `Subscriber.Subscribe`'s old signature (breaking for implementers only: `mock.Broker` and `sse.Subscriber`;
   every caller that ignores the result still compiles); and, outside this repo, the `webtyp/bus` package (see Stage 4).

## Stage 1 — the contract (`events.go`)

```go
// Subscription is the handle of one Subscribe call.
type Subscription interface {
	// Cancel stops delivery to the handler of this registration. It is idempotent and safe to call from inside a handler.
	// A Cancel that returns before a Publish starts guarantees the handler is not called by that Publish; a Publish already
	// running may still deliver once.
	Cancel()
}

type Subscriber interface {
	Subscribe(topic string, h Handler) Subscription
}
```

Update the doc comments of `Subscriber` and `Broker` accordingly. Nothing else in the contract changes (`Fanout` stays).

## Stage 2 — `mock.Broker` (`mock/broker.go`)

- Each registration gets an increasing id (`uint32`, kept in the broker under its mutex). The stored handler entry becomes
  `{id, h}`. `Subscribe` returns a `*subscription` whose `Cancel` removes that entry by id under the broker's mutex; a second
  `Cancel` does nothing. Keep slices (no `map`), keep `Publish` copying the handler list under the lock and calling outside it
  (so a handler may call `Cancel` or `Subscribe` without deadlock).
- `var _ events.Broker = (*Broker)(nil)` stays.

## Stage 3 — conformance and tests

**`conformance/conformance.go`**: add these clauses to `Run` (same style as the existing ones; the helper types stay):
- `subscribe_returns_a_subscription`: the returned value is not nil.
- `cancel_stops_delivery`: publish → delivered; `Cancel`; publish again → not delivered.
- `cancel_is_idempotent`: `Cancel` twice does not panic and changes nothing.
- `cancel_one_keeps_the_others`: two subscribers on one topic; cancel the first; the second still receives.
- `cancel_from_inside_a_handler`: a handler that cancels its own subscription during delivery does not deadlock and is not
  called again.

**`tests/`**: search first (`tests/broker_test.go`, `conformance_test.go`, `consumer_test.go`, `fanout_test.go` exist) and **extend**
them; do not add a duplicate. The mock must pass the extended conformance suite. `consumer_test.go` gains one case showing that a
subscriber module holding only `events.Subscriber` can cancel.

## Stage 4 — docs

- `README.md`: the Quick Start keeps its shape and gains a three-line `Cancel` example; the **Contracts** list gains `Subscription`.
- New `docs/ARCHITECTURE.md`: the contract in one page, synchronous in-order delivery, the two audiences (`Fanout`), and a section
  **"Why there is no bus"** with the carried / not carried list of this plan's «Ecosystem search» (without linking this plan).
- Permanent docs never link `docs/PLAN.md`.

## Delivery check

```
gotest                                                          # vet, race, tests and wasm green
GOOS=js GOARCH=wasm go vet ./...
gofmt -l .                                                      # → empty
grep -rn "Subscribe(topic string, h Handler)$" events.go       # → empty (the old signature is gone)
git diff --stat main                                            # only the files of the stages
```

## Stages table

| # | Stage | Files |
|---|---|---|
| 1 | the contract | `events.go` |
| 2 | the in-process broker | `mock/broker.go` |
| 3 | conformance and tests | `conformance/conformance.go`, `tests/broker_test.go`, `tests/consumer_test.go`, `tests/conformance_test.go` |
| 4 | docs | `README.md`, `docs/ARCHITECTURE.md` |
