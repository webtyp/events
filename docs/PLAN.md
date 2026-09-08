---
PLAN: "feat: Fanout — one Publish reaches in-proc subscribers and a transport"
EXECUTOR: jules
REVIEWER: none
---

> This plan is dispatched via the CodeJob workflow. See skill: agents-workflow.
>
> **GATE** for `app-demo`'s real-backend split. Orchestrator:
> [webtyp/docs/AGENDA_DOMAIN_MASTER_PLAN.md](https://github.com/webtyp/webtyp/blob/main/docs/AGENDA_DOMAIN_MASTER_PLAN.md).

# Plan — `events.Fanout`

## 1. The gap

A server binary that runs domain modules has **two** audiences for the same
event, and today no way to serve both from one `Publish`:

1. **Other modules in the same process.** `appointment_booking` subscribes to
   `business.calendar.changed` to recompute conflicts. That delivery is
   `events/mock.Broker`'s job — its own doc says so: *"It never crosses a
   process boundary — that is webtyp.com/sse's job."*
2. **The browser.** The same event must reach the WASM client so the view
   reloads. That is `sse.Publisher`'s job — it already satisfies
   `events.Publisher` (`var _ events.Publisher = Publisher{}`).

A module takes **one** `events.Publisher` in its `Deps`. Wire the broker and the
browser never hears; wire SSE and the in-process subscriber never fires. Today
the only ways out are both wrong:

- **Give modules two publishers.** Every domain module's `Deps` grows a second
  field that means "the same thing, but over there". The module would have to
  know there are two audiences, which is exactly the coupling `events` exists to
  remove.
- **Bridge broker → SSE with a subscription.** `Subscriber.Subscribe(topic, h)`
  is topic-exact; there is no wildcard. Every app would maintain a hand-written
  list of topics to forward, and a new topic would silently stop reaching the
  browser — a silent failure.

## 2. Design gate

New exported symbol. Per skill **api-design**:

### 2.1 Prior art

- **Go's `io.MultiWriter`** — the same shape for the same problem: one `Write`,
  several sinks, errors from the first failure. `Fanout` is its `Publisher`
  twin, minus the error (this contract is fire-and-forget by declaration).
- **Watermill (Go)** — a `PublisherDecorator` chain; publishing to several
  backends is composed at the router. Heavier than needed here because
  Watermill also owns delivery guarantees, which `events` explicitly does not.
- **Spring `ApplicationEventMulticaster`** — one publish, N listeners, in-proc
  plus bridges. Same idea, an object graph instead of a slice.

Why a slice type rather than a constructor: `events.Publisher` is a
single-method interface and the composition is a list. `io.MultiWriter` returns
an opaque value because it must close over the slice; a named slice type is
simpler in Go, prints usefully, and lets a caller append.

### 2.2 The novice-name test

`events.Fanout{broker, ssePublisher}` — *"fan this out to the broker and to
SSE."* Fan-out is the term the `mock.Broker` doc already uses ("synchronous,
in-memory fan-out"), so it introduces no vocabulary.

### 2.3 Complexity ledger

| Row | Δ |
|---|---|
| Concepts the developer must learn | **0** — it *is* an `events.Publisher` |
| Files touched to reach two audiences | **1** (the composition root) |
| Lines at the call site | **+1** |
| Ways to do the same thing | **−2** — the two-publishers-in-Deps workaround and the hand-listed topic bridge both stop being reachable |
| Exported surface | **+1 type, +1 method** |

### 2.4 Where it belongs

`events` owns the pub/sub contract. Composing two `Publisher`s is that concern,
and the package doc already frames itself as the place where "every module
redeclared its own EventPublisher" was fixed. An app-local copy would be that
regression.

### 2.5 What it deletes

Nothing today (no app has shipped either workaround). It **prevents** both, and
it is what lets `app-demo` keep a single `Publisher` per module.

## 3. Stage 1 — the type

**File: `events.go`** — beside `Publisher`, so the two read together.

```go
// Fanout publishes one Event to every Publisher it holds, in order. It is
// itself a Publisher, so a module still takes exactly one.
//
// It exists because a server binary has two audiences for the same event: the
// modules in its own process (mock.Broker) and the browser (sse.Publisher).
// Without it a module would need two Publisher fields — it would have to know
// there are two audiences, which is the coupling this package removes.
//
// Delivery follows the Publisher contract: fire-and-forget, no ordering or
// delivery promise beyond "every Publisher in the slice is called". A nil
// entry is skipped, so an optional transport can be composed without a guard
// at the call site.
type Fanout []Publisher

func (f Fanout) Publish(e Event) {
	for _, p := range f {
		if p != nil {
			p.Publish(e)
		}
	}
}

var _ Publisher = Fanout(nil)
```

**A panicking Publisher is not recovered.** That matches `mock.Broker`, whose
doc states a broken subscriber must fail loudly rather than be silently dropped.
Do **not** add a `recover()` — it would turn a broken transport into an event
nobody notices.

**Nil entries are skipped, but a nil `Fanout` is a valid no-op Publisher** —
ranging over a nil slice iterates zero times. That is deliberate: it lets a
config pass `Fanout` unconditionally.

## 4. Stage 2 — tests

**File: `events_test.go`** (extend it; do not add a second file for one type).

```go
// Fanout delivers to every Publisher, in slice order.
func TestFanoutDeliversToEveryPublisherInOrder(t *testing.T)

// A nil entry is skipped, so an optional transport composes without a guard.
func TestFanoutSkipsNilPublishers(t *testing.T)

// A nil Fanout is a valid no-op Publisher.
func TestNilFanoutPublishesNothing(t *testing.T)

// The consumer-shaped case this exists for: one Publish reaches an in-proc
// subscriber AND a transport double.
func TestFanoutReachesBrokerSubscriberAndTransport(t *testing.T)
```

The last one is the publication rule from skill **api-design**: a
consumer-shaped test, inside the library, through the real stack — a real
`mock.Broker` with a real `Subscribe`, and a recording `Publisher` standing in
for the transport.

Use `webtyp.com/fmt`, never stdlib `errors`/`strings`. `testing` is the only
stdlib import allowed here, as elsewhere in this repo.

## 5. Stage 3 — docs

`README.md` gains a short "Two audiences" section showing the composition:

```go
pub := events.Fanout{broker, sse.Publisher{Server: sseSrv}}
mod, err := appointmentbooking.New(db, appointmentbooking.Deps{
    Publisher:  pub,     // reaches both
    Subscriber: broker,  // in-proc only, by definition
})
```

State plainly that **`Subscriber` is always the broker**: SSE is one-way
(server→browser) and cannot deliver to an in-process handler. That asymmetry is
the thing a reader gets wrong first.

## 6. Constraints

- **No stdlib** beyond `testing` in `_test.go`: `webtyp.com/fmt`.
- **No `map`**, no new import. This is a slice and a loop.
- This package compiles to WASM; keep it allocation-free on the hot path.
- **No `recover()`** (§3).
- **No `TODO`, nothing deprecated.** Before closing:
  `grep -rn "TODO\|FIXME\|Deprecated" --include='*.go' .`
- `gotest`, never `go test`.

## 7. Acceptance criteria

| # | Check | Expected |
|---|-------|----------|
| 1 | `gotest ./...` | green, the four tests in §4 present |
| 2 | `grep -n "var _ Publisher = Fanout" events.go` | present |
| 3 | `grep -n "recover()" events.go` | **empty** |
| 4 | `grep -rn "\"errors\"\|\"strconv\"\|\"strings\"" --include='*.go' .` | **empty** |
| 5 | `GOOS=js GOARCH=wasm go build ./...` | compiles |
| 6 | `grep -rn "TODO\|FIXME\|Deprecated" --include='*.go' .` | no hit introduced here |

## 8. Stages

| # | Stage | Files | Done when |
|---|-------|-------|-----------|
| 1 | The type | `events.go` | `Fanout` + `var _ Publisher` |
| 2 | Tests | `events_test.go` | §4 complete, including the consumer-shaped case |
| 3 | Docs | `README.md` | the "Two audiences" section, and the Subscriber asymmetry |
