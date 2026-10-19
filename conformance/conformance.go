// Package conformance is the executable contract of events.Broker.
//
// The interface states the SIGNATURES; this package states the BEHAVIOUR — the same
// role webtyp.com/router/conformance plays for router.Router. Two brokers can
// satisfy events.Broker and disagree on what matters (does a subscriber to topic A see
// an event published on topic B? does a second subscriber on the same topic see
// anything at all?). That has to become something that goes red, not folklore.
//
// An implementation proves conformance from its own test package:
//
//	func TestMockConformance(t *testing.T) {
//	    conformance.Run(t, conformance.Factory{New: func(t *testing.T) events.Broker { return &mock.Broker{} }})
//	}
package conformance

import (
	"testing"
	"time"

	"webtyp.com/events"
	"webtyp.com/model"
)

// TopicA and TopicB are the two topics the suite drives its cases with.
const (
	TopicA = "conformance.a"
	TopicB = "conformance.b"
)

// waitFor is how long a clause waits for an expected delivery before failing. Generous
// enough for an async broker (network, goroutine hand-off), short enough that a suite
// run stays fast when delivery is, correctly, synchronous.
const waitFor = 2 * time.Second

// Fixture is the minimal model.Encodable payload the suite publishes, so a clause can
// assert the FIELDS survived delivery — not just that some Event arrived.
type Fixture struct{ Value string }

func (f *Fixture) IsNil() bool                      { return f == nil }
func (f *Fixture) EncodeFields(w model.FieldWriter) { w.String("value", f.Value) }

var _ model.Encodable = (*Fixture)(nil)

// Factory builds a fresh Broker for one case, so no case can be polluted by another's
// subscriptions.
type Factory struct {
	New func(t *testing.T) events.Broker
}

// Run executes every clause of the contract against the implementation.
func Run(t *testing.T, f Factory) {
	if f.New == nil {
		t.Fatal("conformance: Factory.New is required")
	}

	t.Run("subscriber_receives_matching_topic", func(t *testing.T) { subscriberReceivesMatchingTopic(t, f) })
	t.Run("subscriber_does_not_receive_other_topic", func(t *testing.T) { subscriberIgnoresOtherTopic(t, f) })
	t.Run("multiple_subscribers_all_receive", func(t *testing.T) { fanOutToMultipleSubscribers(t, f) })
	t.Run("payload_fields_survive_delivery", func(t *testing.T) { payloadFieldsSurviveDelivery(t, f) })
	t.Run("publish_with_no_subscriber_does_not_block", func(t *testing.T) { publishWithNoSubscriberDoesNotBlock(t, f) })
	t.Run("subscribe_returns_a_subscription", func(t *testing.T) { subscribeReturnsASubscription(t, f) })
	t.Run("cancel_stops_delivery", func(t *testing.T) { cancelStopsDelivery(t, f) })
	t.Run("cancel_is_idempotent", func(t *testing.T) { cancelIsIdempotent(t, f) })
	t.Run("cancel_one_keeps_the_others", func(t *testing.T) { cancelOneKeepsTheOthers(t, f) })
	t.Run("cancel_from_inside_a_handler", func(t *testing.T) { cancelFromInsideAHandler(t, f) })
}

func build(t *testing.T, f Factory) events.Broker {
	t.Helper()
	b := f.New(t)
	if b == nil {
		t.Fatal("conformance: Factory.New returned a nil Broker")
	}
	return b
}

// subscriberReceivesMatchingTopic: the baseline — publish on a topic, the subscriber
// on that SAME topic gets it.
func subscriberReceivesMatchingTopic(t *testing.T, f Factory) {
	b := build(t, f)

	got := make(chan events.Event, 1)
	b.Subscribe(TopicA, func(e events.Event) { got <- e })

	b.Publish(events.Event{Topic: TopicA, Payload: &Fixture{Value: "x"}})

	select {
	case e := <-got:
		if e.Topic != TopicA {
			t.Errorf("delivered event carries the wrong topic: got %q, want %q", e.Topic, TopicA)
		}
	case <-time.After(waitFor):
		t.Fatal("subscriber never received the event published on its own topic")
	}
}

// subscriberIgnoresOtherTopic: a subscriber to TopicA must not see an event published
// on TopicB. A sentinel published on TopicA afterwards synchronizes the check — it
// arriving alone (not preceded by the TopicB event) proves TopicB was never delivered
// here, without an arbitrary sleep.
func subscriberIgnoresOtherTopic(t *testing.T, f Factory) {
	b := build(t, f)

	got := make(chan events.Event, 2)
	b.Subscribe(TopicA, func(e events.Event) { got <- e })

	b.Publish(events.Event{Topic: TopicB, Payload: &Fixture{Value: "wrong-topic"}})
	b.Publish(events.Event{Topic: TopicA, Payload: &Fixture{Value: "sentinel"}})

	select {
	case e := <-got:
		if e.Topic != TopicA {
			t.Fatalf("a subscriber to %q received an event published on %q", TopicA, e.Topic)
		}
	case <-time.After(waitFor):
		t.Fatal("the sentinel event never arrived")
	}

	select {
	case e := <-got:
		t.Fatalf("a subscriber to %q received a SECOND, unexpected event: %+v", TopicA, e)
	default:
	}
}

// fanOutToMultipleSubscribers: two Subscribe calls on the same topic both fire — a
// broker that only remembers the last subscriber breaks every second listener.
func fanOutToMultipleSubscribers(t *testing.T, f Factory) {
	b := build(t, f)

	first := make(chan events.Event, 1)
	second := make(chan events.Event, 1)
	b.Subscribe(TopicA, func(e events.Event) { first <- e })
	b.Subscribe(TopicA, func(e events.Event) { second <- e })

	b.Publish(events.Event{Topic: TopicA, Payload: &Fixture{Value: "fan-out"}})

	for name, ch := range map[string]chan events.Event{"first": first, "second": second} {
		select {
		case <-ch:
		case <-time.After(waitFor):
			t.Errorf("the %s subscriber on the same topic never received the event", name)
		}
	}
}

// payloadFieldsSurviveDelivery: the delivered Event.Payload must carry the SAME field
// values the publisher set — proving delivery is not merely "some Event arrived" but
// carries real data, whether the broker passes the value through directly (in-process)
// or round-trips it through a wire codec (cross-process).
func payloadFieldsSurviveDelivery(t *testing.T, f Factory) {
	b := build(t, f)

	got := make(chan events.Event, 1)
	b.Subscribe(TopicA, func(e events.Event) { got <- e })

	b.Publish(events.Event{Topic: TopicA, Payload: &Fixture{Value: "payload-survives"}})

	select {
	case e := <-got:
		fx, ok := e.Payload.(*Fixture)
		if !ok {
			t.Fatalf("delivered Payload is not the published type: got %T", e.Payload)
		}
		if fx.Value != "payload-survives" {
			t.Errorf("delivered Payload lost its field value: got %q, want %q", fx.Value, "payload-survives")
		}
	case <-time.After(waitFor):
		t.Fatal("subscriber never received the event")
	}
}

// publishWithNoSubscriberDoesNotBlock: Publish on a topic nobody subscribed to must
// return promptly — a broker that blocks waiting for a listener that will never come
// is a deadlock waiting to happen in production.
func publishWithNoSubscriberDoesNotBlock(t *testing.T, f Factory) {
	b := build(t, f)

	done := make(chan struct{})
	go func() {
		b.Publish(events.Event{Topic: "conformance.nobody-listens", Payload: &Fixture{Value: "x"}})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(waitFor):
		t.Fatal("Publish with no subscriber did not return: the implementation blocks on delivery")
	}
}

// subscribeReturnsASubscription: the returned value is not nil.
func subscribeReturnsASubscription(t *testing.T, f Factory) {
	b := build(t, f)
	sub := b.Subscribe(TopicA, func(events.Event) {})
	if sub == nil {
		t.Fatal("Subscribe returned a nil Subscription")
	}
}

// cancelStopsDelivery: publish → delivered; Cancel; publish again → not delivered.
func cancelStopsDelivery(t *testing.T, f Factory) {
	b := build(t, f)

	got := make(chan events.Event, 2)
	sub := b.Subscribe(TopicA, func(e events.Event) { got <- e })

	b.Publish(events.Event{Topic: TopicA, Payload: &Fixture{Value: "first"}})
	select {
	case <-got:
	case <-time.After(waitFor):
		t.Fatal("first publish was not delivered")
	}

	sub.Cancel()

	b.Publish(events.Event{Topic: TopicA, Payload: &Fixture{Value: "second"}})
	b.Publish(events.Event{Topic: TopicB, Payload: &Fixture{Value: "sentinel"}})

	// Wait a bit to ensure the second event was not delivered
	select {
	case e := <-got:
		t.Fatalf("canceled subscription received an event: %+v", e)
	case <-time.After(10 * time.Millisecond):
		// Test passed
	}
}

// cancelIsIdempotent: Cancel twice does not panic and changes nothing.
func cancelIsIdempotent(t *testing.T, f Factory) {
	b := build(t, f)

	got := make(chan events.Event, 1)
	sub := b.Subscribe(TopicA, func(e events.Event) { got <- e })

	sub.Cancel()
	sub.Cancel() // should not panic

	b.Publish(events.Event{Topic: TopicA, Payload: &Fixture{Value: "test"}})

	select {
	case e := <-got:
		t.Fatalf("canceled subscription received an event: %+v", e)
	case <-time.After(10 * time.Millisecond):
		// Test passed
	}
}

// cancelOneKeepsTheOthers: two subscribers on one topic; cancel the first; the second still receives.
func cancelOneKeepsTheOthers(t *testing.T, f Factory) {
	b := build(t, f)

	got1 := make(chan events.Event, 1)
	got2 := make(chan events.Event, 1)

	sub1 := b.Subscribe(TopicA, func(e events.Event) { got1 <- e })
	b.Subscribe(TopicA, func(e events.Event) { got2 <- e })

	sub1.Cancel()

	b.Publish(events.Event{Topic: TopicA, Payload: &Fixture{Value: "test"}})

	select {
	case e := <-got1:
		t.Fatalf("canceled subscription received an event: %+v", e)
	case <-time.After(10 * time.Millisecond):
	}

	select {
	case <-got2:
	case <-time.After(waitFor):
		t.Fatal("second subscriber did not receive the event")
	}
}

// cancelFromInsideAHandler: a handler that cancels its own subscription during delivery does not deadlock and is not called again.
func cancelFromInsideAHandler(t *testing.T, f Factory) {
	b := build(t, f)

	got := make(chan events.Event, 2)
	var sub events.Subscription
	sub = b.Subscribe(TopicA, func(e events.Event) {
		got <- e
		sub.Cancel()
	})

	b.Publish(events.Event{Topic: TopicA, Payload: &Fixture{Value: "first"}})

	select {
	case <-got:
	case <-time.After(waitFor):
		t.Fatal("first publish was not delivered")
	}

	b.Publish(events.Event{Topic: TopicA, Payload: &Fixture{Value: "second"}})

	select {
	case e := <-got:
		t.Fatalf("canceled subscription received a second event: %+v", e)
	case <-time.After(10 * time.Millisecond):
	}
}
