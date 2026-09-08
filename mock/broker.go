// Package mock is the canonical in-process events.Broker: synchronous, in-memory
// fan-out. It is both the reference implementation module-to-module delivery uses
// in a single binary, and the test double consumers wire in place of a real broker.
package mock

import (
	"sync"

	"webtyp.com/events"
)

type subEntry struct {
	topic    string
	handlers []events.Handler
}

// Broker delivers synchronously, in subscription order, to every Subscriber
// registered for an Event's Topic at the moment Publish runs. It never crosses a
// process boundary — that is webtyp.com/sse's job.
type Broker struct {
	mu   sync.Mutex
	subs []subEntry
}

// Subscribe registers h for topic. Safe for concurrent use with Publish.
func (b *Broker) Subscribe(topic string, h events.Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range b.subs {
		if b.subs[i].topic == topic {
			b.subs[i].handlers = append(b.subs[i].handlers, h)
			return
		}
	}
	b.subs = append(b.subs, subEntry{
		topic:    topic,
		handlers: []events.Handler{h},
	})
}

// Publish invokes every Handler subscribed to e.Topic, synchronously. A Handler
// that panics is not recovered: a broken subscriber must fail loudly, not be
// silently dropped from future delivery.
func (b *Broker) Publish(e events.Event) {
	b.mu.Lock()
	var handlers []events.Handler
	for i := range b.subs {
		if b.subs[i].topic == e.Topic {
			handlers = append(handlers, b.subs[i].handlers...)
		}
	}
	b.mu.Unlock()

	for _, h := range handlers {
		h(e)
	}
}

var _ events.Broker = (*Broker)(nil)
