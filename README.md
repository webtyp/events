# events
<img src="docs/img/badges.svg">

Typed pub/sub contract: `Publisher`/`Subscriber` decoupled from the broker implementation. A
domain module fires and listens to events without knowing whether delivery happens in-process
(module-to-module, same binary) or over the wire (push to a browser via SSE).

## Why

Every module used to redeclare its own `EventPublisher`, and none of them agreed: some took
`payload any`, some threaded a context through the call, none had a `Subscriber` at all. A
module that wanted to react to another module's event had nothing typed to depend on but that
module's own ad-hoc interface.

## Quick Start

```go
import "webtyp.com/events"

const TopicItemCreated = "catalog.item.created"

type ItemCreated struct{ ID, Name string }

func (e *ItemCreated) IsNil() bool { return e == nil }
func (e *ItemCreated) EncodeFields(w model.FieldWriter) {
    w.String("id", e.ID)
    w.String("name", e.Name)
}

// A module that publishes depends on events.Publisher only.
func (m *Module) create(item CatalogItem) {
    // … persist …
    m.pub.Publish(events.Event{Topic: TopicItemCreated, Payload: &ItemCreated{ID: item.Id, Name: item.Name}})
}

// A module that reacts depends on events.Subscriber only.
func (m *Notifier) Init(sub events.Subscriber) {
    sub.Subscribe(TopicItemCreated, func(e events.Event) {
        if item, ok := e.Payload.(*ItemCreated); ok {
            m.notify(item.Name)
        }
    })
}
```

Neither module imports the other, and neither imports a concrete broker. The composition root
builds ONE broker and hands each module the narrower contract (`Publisher` or `Subscriber`) it
declared it needs.

## Contracts

- **`Event`**: `Topic string` + `Payload model.Encodable` — never `any`.
- **`Handler`**: `func(Event)` — what a subscriber registers.
- **`Publisher`**: `Publish(Event)` — fire-and-forget.
- **`Subscriber`**: `Subscribe(topic string, h Handler)` — fan-out: every Subscribe on the same
  topic receives the event.
- **`Broker`**: `Publisher` + `Subscriber` — the full contract a composition root injects.
- **`mock`**: the reference `Broker` — synchronous, in-process. Used both as the real
  module-to-module implementation inside a single binary and as the test double.
- **`conformance`**: `conformance.Run(t, conformance.Factory{New: ...})` — the executable
  behavior contract every `Broker` must pass, the same role `router/conformance` plays for
  `router.Router`.

## Design

An event's `Payload` travels as the concrete Go value the publisher built. An in-process broker
(`mock.Broker`) delivers it as-is — no serialization. A broker that crosses a real wire
(`webtyp.com/sse`, pushing to a browser) encodes `Payload` via its own `EncodeFields`
when it needs to; that is the broker's concern, never the contract's or the module's.

No `any` in the public API surface beyond the one Go interface value (`model.Encodable`) every
typed payload already satisfies.
