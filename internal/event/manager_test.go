package event

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"goagent-lite/internal/agent"
)

func TestSequenceHistoryAndFanout(t *testing.T) {
	m := New(8)
	history, ch, cancel := m.Open("t1")
	defer cancel()
	if len(history) != 0 {
		t.Fatalf("fresh history = %v", history)
	}
	for i := 0; i < 3; i++ {
		m.Publish("t1", agent.AgentEvent{Type: agent.EventToken, Payload: json.RawMessage(`{"i":1}`)})
	}
	hist := m.History("t1")
	if len(hist) != 3 {
		t.Fatalf("history len = %d", len(hist))
	}
	for i, e := range hist {
		if e.Sequence != int64(i+1) {
			t.Fatalf("sequence[%d] = %d", i, e.Sequence)
		}
		if e.TaskID != "t1" || e.Type != agent.EventToken || e.Timestamp.IsZero() {
			t.Fatalf("bad event: %+v", e)
		}
	}
	for i := 0; i < 3; i++ {
		select {
		case e := <-ch:
			if e.Sequence != int64(i+1) {
				t.Fatalf("live seq[%d] = %d", i, e.Sequence)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for live event")
		}
	}
}

func TestUnknownEventTypeNormalized(t *testing.T) {
	m := New(4)
	e := m.Publish("t1", agent.AgentEvent{Type: "made-up"})
	if e.Type != agent.EventCustom {
		t.Fatalf("type = %q, want custom", e.Type)
	}
}

func TestSlowConsumerDropped(t *testing.T) {
	m := New(1)
	var calls int
	m.OnSlowConsumer = func() { calls++ }
	_, ch, cancel := m.Open("t1")
	defer cancel()
	m.Publish("t1", agent.AgentEvent{Type: agent.EventStarted})
	m.Publish("t1", agent.AgentEvent{Type: agent.EventToken}) // buffer full -> dropped
	if _, open := <-ch; !open {
		t.Fatal("buffered event should still be delivered before close")
	}
	if _, open := <-ch; open {
		t.Fatal("channel should be closed after slow-consumer drop")
	}
	if calls != 1 {
		t.Fatalf("OnSlowConsumer calls = %d", calls)
	}
}

func TestCloseAndOpenAfterClose(t *testing.T) {
	m := New(4)
	hist, ch, cancel := m.Open("t1")
	_ = hist
	m.Publish("t1", agent.AgentEvent{Type: agent.EventStarted})
	m.Close("t1")
	if _, open := <-ch; !open {
		t.Fatal("buffered event should be delivered")
	}
	if _, open := <-ch; open {
		t.Fatal("subscription should be closed")
	}
	cancel()

	h, ch2, cancel2 := m.Open("t1")
	defer cancel2()
	if len(h) != 1 {
		t.Fatalf("history after close = %d", len(h))
	}
	if _, open := <-ch2; open {
		t.Fatal("new subscription on a closed stream must be closed")
	}
}

func TestCloseWithoutEvents(t *testing.T) {
	m := New(4)
	m.Close("never-published")
	_, ch, cancel := m.Open("never-published")
	defer cancel()
	if _, open := <-ch; open {
		t.Fatal("closed stream without events must terminate subscribers")
	}
}

func TestPurge(t *testing.T) {
	m := New(4)
	m.Publish("t1", agent.AgentEvent{Type: agent.EventStarted})
	_, ch, cancel := m.Open("t1")
	defer cancel()
	m.Purge("t1")
	if _, open := <-ch; open {
		t.Fatal("purge must close subscribers")
	}
	if got := m.History("t1"); got != nil {
		t.Fatalf("history after purge = %v", got)
	}
}

func TestConcurrentPublishAssignsUniqueSequence(t *testing.T) {
	m := New(4)
	const n = 500
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Publish("t1", agent.AgentEvent{Type: agent.EventToken})
		}()
	}
	wg.Wait()
	hist := m.History("t1")
	if len(hist) != n {
		t.Fatalf("history len = %d", len(hist))
	}
	seen := make(map[int64]bool, n)
	for _, e := range hist {
		if seen[e.Sequence] {
			t.Fatalf("duplicate sequence %d", e.Sequence)
		}
		seen[e.Sequence] = true
	}
}
