package events_test

import (
	"sync"
	"testing"

	"webtyp.com/events"
	"webtyp.com/events/mock"
)

func TestBroker_MultipleTopics_ScopedDelivery(t *testing.T) {
	b := &mock.Broker{}

	var recA, recB []string
	b.Subscribe("topic.a", func(e events.Event) {
		recA = append(recA, e.Topic)
	})
	b.Subscribe("topic.b", func(e events.Event) {
		recB = append(recB, e.Topic)
	})

	b.Publish(events.Event{Topic: "topic.a"})

	if len(recA) != 1 || recA[0] != "topic.a" {
		t.Errorf("expected topic.a subscriber to receive event, got: %v", recA)
	}
	if len(recB) != 0 {
		t.Errorf("expected topic.b subscriber to receive nothing, got: %v", recB)
	}
}

func TestBroker_OrderPreserved(t *testing.T) {
	b := &mock.Broker{}

	var order []int
	b.Subscribe("topic.seq", func(e events.Event) {
		order = append(order, 1)
	})
	b.Subscribe("topic.seq", func(e events.Event) {
		order = append(order, 2)
	})
	b.Subscribe("topic.seq", func(e events.Event) {
		order = append(order, 3)
	})

	b.Publish(events.Event{Topic: "topic.seq"})

	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Errorf("expected subscription order [1 2 3], got %v", order)
	}
}

func TestBroker_ConcurrentPublish_NoRace(t *testing.T) {
	b := &mock.Broker{}

	var wg sync.WaitGroup

	// Concurrent subscribers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			b.Subscribe("topic.concurrent", func(e events.Event) {})
		}(i)
	}

	// Concurrent publishers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Publish(events.Event{Topic: "topic.concurrent"})
		}()
	}

	wg.Wait()
}
