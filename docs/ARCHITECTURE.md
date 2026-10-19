# Architecture

`webtyp.com/events` is the typed pub/sub contract: a Publisher a domain module fires events through, and a Subscriber it listens on — decoupled from whatever broker actually moves the message.

## The Contract

- **Event**: `Topic string` + `Payload model.Encodable`. Payloads travel as the concrete Go value.
- **Handler**: `func(Event)`.
- **Publisher**: `Publish(Event)`. Fire-and-forget.
- **Subscriber**: `Subscribe(topic string, h Handler) Subscription`. Returns a handle to cancel the registration.
- **Subscription**: `Cancel()`. Stops delivery to the handler.
- **Broker**: `Publisher` + `Subscriber`. The full contract wired at the root.
- **mock.Broker**: The in-process reference implementation delivering synchronously in order.

## Synchronous In-Order Delivery

The in-process `mock.Broker` delivers synchronously, in subscription order, to every Subscriber registered for an Event's Topic at the moment `Publish` runs. The `events` contract guarantees nothing beyond this, but `mock.Broker` avoids crossing process boundaries or async handoffs. Handlers that block must start their own goroutines.

## Two Audiences (`Fanout`)

A server binary usually has two audiences for the same event:
1. The modules in its own process (`mock.Broker`).
2. The browser via SSE (`webtyp.com/sse`).

`events.Fanout` is a `[]Publisher` that implements `Publisher` to distribute events to multiple destinations, keeping the single-publisher simplicity for modules.

## Why there is no bus

The ecosystem previously had two pub/sub packages: `webtyp.com/events` and `webtyp.com/bus`. They are unified into `events`.

- **Carried to events: `Cancel`.** A component that subscribes while mounted must stop when unmounted.
- **Not carried: `Topics()`.** No consumer; introspection can be added when one exists.
- **Not carried: `Close()`.** A broker's lifetime belongs to whoever built it; `Cancel` covers a single registration.
- **Not carried: asynchronous dispatch.** `events` documents synchronous, in-order delivery. A handler that must not block starts its own goroutine. Two delivery semantics in one contract would be a trap.
- **Not carried: `binary.Message` payloads.** The contract's payload is `model.Encodable`.
