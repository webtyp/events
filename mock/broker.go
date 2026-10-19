// Package mock is the canonical in-process events.Broker: synchronous, in-memory
// fan-out. It is both the reference implementation module-to-module delivery uses
// in a single binary, and the test double consumers wire in place of a real broker.
package mock

import (
	"sync"

	"webtyp.com/events"
)

type handlerEntry struct {
	id uint32
	h  events.Handler
}

type subEntry struct {
	topic    string
	handlers []handlerEntry
}

// Broker delivers synchronously, in subscription order, to every Subscriber
// registered for an Event's Topic at the moment Publish runs. It never crosses a
// process boundary — that is webtyp.com/sse's job.
type Broker struct {
	mu     sync.Mutex
	nextID uint32
	subs   []subEntry
}

type subscription struct {
	broker *Broker
	topic  string
	id     uint32
}

func (s *subscription) Cancel() {
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()

	for i := range s.broker.subs {
		if s.broker.subs[i].topic == s.topic {
			handlers := s.broker.subs[i].handlers
			for j := range handlers {
				if handlers[j].id == s.id {
					// Delete by shifting
					s.broker.subs[i].handlers = append(handlers[:j], handlers[j+1:]...)
					return
				}
			}
			return
		}
	}
}

// Subscribe registers h for topic. Safe for concurrent use with Publish.
func (b *Broker) Subscribe(topic string, h events.Handler) events.Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextID++
	id := b.nextID

	entry := handlerEntry{id: id, h: h}

	for i := range b.subs {
		if b.subs[i].topic == topic {
			b.subs[i].handlers = append(b.subs[i].handlers, entry)
			return &subscription{broker: b, topic: topic, id: id}
		}
	}
	b.subs = append(b.subs, subEntry{
		topic:    topic,
		handlers: []handlerEntry{entry},
	})
	return &subscription{broker: b, topic: topic, id: id}
}

// Publish invokes every Handler subscribed to e.Topic, synchronously. A Handler
// that panics is not recovered: a broken subscriber must fail loudly, not be
// silently dropped from future delivery.
func (b *Broker) Publish(e events.Event) {
	b.mu.Lock()
	var handlers []events.Handler
	for i := range b.subs {
		if b.subs[i].topic == e.Topic {
			for _, entry := range b.subs[i].handlers {
				handlers = append(handlers, entry.h)
			}
		}
	}
	b.mu.Unlock()

	for _, h := range handlers {
		h(e)
	}
}

var _ events.Broker = (*Broker)(nil)
