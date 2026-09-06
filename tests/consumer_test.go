package events_test

import (
	"testing"
	"time"

	"webtyp.com/events"
	"webtyp.com/events/mock"
	"webtyp.com/model"
)

// itemCreated mimics a domain module's own event payload — e.g. item_catalog
// publishing "catalog.item.created". It only needs to be model.Encodable; nothing
// here is generated or ormc-specific.
type itemCreated struct {
	ID   string
	Name string
}

func (e *itemCreated) IsNil() bool { return e == nil }
func (e *itemCreated) EncodeFields(w model.FieldWriter) {
	w.String("id", e.ID)
	w.String("name", e.Name)
}

const topicItemCreated = "catalog.item.created"

// TestConsumer_ModuleToModule is the consumer-shaped proof the construction harness
// requires: a "publisher" module holds only events.Publisher, a "subscriber" module
// holds only events.Subscriber — neither imports the other, neither imports mock —
// and the composition root is the only place that constructs a concrete Broker and
// hands each module the narrow interface it declared it needs.
func TestConsumer_ModuleToModule(t *testing.T) {
	// The composition root's job: one concrete broker, injected as two different,
	// narrower contracts.
	broker := &mock.Broker{}
	var pub events.Publisher = broker
	var sub events.Subscriber = broker

	// The "subscriber module": depends on events.Subscriber only.
	received := make(chan *itemCreated, 1)
	sub.Subscribe(topicItemCreated, func(e events.Event) {
		if item, ok := e.Payload.(*itemCreated); ok {
			received <- item
		}
	})

	// The "publisher module": depends on events.Publisher only.
	pub.Publish(events.Event{
		Topic:   topicItemCreated,
		Payload: &itemCreated{ID: "svc1", Name: "Consulta general"},
	})

	select {
	case item := <-received:
		if item.ID != "svc1" || item.Name != "Consulta general" {
			t.Errorf("subscriber received the wrong payload: %+v", item)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the subscriber module never received the publisher module's event")
	}
}
