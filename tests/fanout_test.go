package events_test

import (
	"testing"

	"webtyp.com/events"
	"webtyp.com/events/mock"
)

// recordingPublisher stands in for a transport (e.g. webtyp.com/sse) — it only
// records what it was asked to publish.
type recordingPublisher struct {
	received []events.Event
}

func (r *recordingPublisher) Publish(e events.Event) {
	r.received = append(r.received, e)
}

// Fanout delivers to every Publisher, in slice order.
func TestFanoutDeliversToEveryPublisherInOrder(t *testing.T) {
	var a, b, c recordingPublisher
	f := events.Fanout{&a, &b, &c}

	f.Publish(events.Event{Topic: "topic.a"})

	for name, rec := range map[string]*recordingPublisher{"a": &a, "b": &b, "c": &c} {
		if len(rec.received) != 1 || rec.received[0].Topic != "topic.a" {
			t.Errorf("publisher %s: expected 1 event topic.a, got %v", name, rec.received)
		}
	}
}

// A nil entry is skipped, so an optional transport composes without a guard.
func TestFanoutSkipsNilPublishers(t *testing.T) {
	var a recordingPublisher
	f := events.Fanout{&a, nil}

	f.Publish(events.Event{Topic: "topic.a"})

	if len(a.received) != 1 {
		t.Errorf("expected the non-nil publisher to receive 1 event, got %v", a.received)
	}
}

// A nil Fanout is a valid no-op Publisher.
func TestNilFanoutPublishesNothing(t *testing.T) {
	var f events.Fanout
	// Must not panic ranging over a nil slice.
	f.Publish(events.Event{Topic: "topic.a"})
}

// The consumer-shaped case this exists for: one Publish reaches an in-proc
// subscriber (through mock.Broker) AND a transport double, simultaneously —
// the two audiences a server binary has for the same event.
func TestFanoutReachesBrokerSubscriberAndTransport(t *testing.T) {
	broker := &mock.Broker{}
	transport := &recordingPublisher{}
	f := events.Fanout{broker, transport}

	var subscriberSaw []string
	broker.Subscribe("business.calendar.changed", func(e events.Event) {
		subscriberSaw = append(subscriberSaw, e.Topic)
	})

	f.Publish(events.Event{Topic: "business.calendar.changed"})

	if len(subscriberSaw) != 1 {
		t.Errorf("expected the in-proc subscriber to fire once, got %v", subscriberSaw)
	}
	if len(transport.received) != 1 {
		t.Errorf("expected the transport to receive 1 event, got %v", transport.received)
	}
}

var _ events.Publisher = events.Fanout(nil)
