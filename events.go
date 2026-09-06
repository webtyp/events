// Package events is the typed pub/sub contract: a Publisher a domain module fires
// events through, and a Subscriber it (or another module) listens on — decoupled
// from whatever broker actually moves the message.
//
// Before this package, every module redeclared its own EventPublisher, and none of
// them agreed: some took `payload any`, some threaded a context.Context through the
// call, none had a Subscriber at all. A module that published had to invent the
// interface its consumer would satisfy; a module that wanted to react to another
// module's event had nothing to depend on but that ad-hoc, per-module type.
//
// This package is the contract both sides depend on instead. A module imports
// events + model — never a concrete broker (an in-process broker for module-to-module
// delivery, webtyp.com/sse for push to the browser) — and the composition
// root injects the Broker.
package events

import "webtyp.com/model"

// Event is one message on a topic: a name plus a typed payload — never `any`.
// Payload travels as the concrete Go value the publisher built. An in-process
// broker delivers it as-is, with no serialization. A broker that crosses a real
// wire (SSE to a browser) encodes Payload via its own EncodeFields when it needs
// to — that is the broker's concern, not this contract's.
type Event struct {
	// Topic identifies the kind of event, e.g. "catalog.item.created". The
	// publishing module exports it as a typed constant; a subscriber imports
	// that constant rather than repeating the string — the same convention as
	// an RPC operation name.
	Topic string
	// Payload is the event's data. A subscriber that needs the concrete shape
	// type-asserts it: the topic is the shared vocabulary between publisher and
	// subscriber, so both already agree on which concrete type travels on it.
	Payload model.Encodable
}

// Handler receives one delivered Event.
type Handler func(Event)

// Publisher fires an event. Fire-and-forget: this contract makes no delivery-order
// or delivery-guarantee promise beyond "every currently-registered Subscriber for
// Event.Topic is invoked" — a broker that offers more (durability, ordering across
// processes) exposes that as its own, separately documented behavior.
type Publisher interface {
	Publish(e Event)
}

// Subscriber registers a Handler for a topic. Multiple Subscribe calls on the same
// topic all receive the event (fan-out).
type Subscriber interface {
	Subscribe(topic string, h Handler)
}

// Broker is the full contract a composition root wires in: publish and subscribe
// together. Implementations: webtyp.com/events/mock (in-process, for
// module-to-module delivery and tests) and webtyp.com/sse (push to the
// browser).
type Broker interface {
	Publisher
	Subscriber
}
