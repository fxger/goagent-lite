package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"
)

func mustRegister(t *testing.T, s *Scheduler, id string, max, queue int) {
	t.Helper()
	if err := s.RegisterAgent(AgentConfig{ID: id, Version: "v1", MaxConcurrency: max, QueueCapacity: queue}); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
}

func job(id, agentKey string) *Job { return &Job{ID: id, AgentKey: agentKey, Value: id} }

func next(t *testing.T, s *Scheduler) (*Job, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	j, rel, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	return j, rel
}

func TestRoundRobinFairness(t *testing.T) {
	s := New(100)
	mustRegister(t, s, "a", 2, 0)
	mustRegister(t, s, "b", 2, 0)
	for _, j := range []*Job{job("a1", "a@v1"), job("a2", "a@v1"), job("b1", "b@v1"), job("b2", "b@v1")} {
		if err := s.Enqueue(j); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	for i := 0; i < 4; i++ {
		j, rel := next(t, s)
		got = append(got, j.ID)
		rel()
	}
	want := []string{"a1", "b1", "a2", "b2"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("round robin order = %v, want %v", got, want)
		}
	}
}

func TestQuotaExhaustedSkipsQueueWithoutHeadOfLineBlocking(t *testing.T) {
	s := New(100)
	mustRegister(t, s, "a", 1, 0)
	mustRegister(t, s, "b", 1, 0)
	_ = s.Enqueue(job("a1", "a@v1"))
	_ = s.Enqueue(job("a2", "a@v1"))
	_ = s.Enqueue(job("b1", "b@v1"))

	first, rel := next(t, s)
	if first.ID != "a1" {
		t.Fatalf("first = %s, want a1", first.ID)
	}
	// a's quota is now exhausted; the next dispatch must be b1, not a2.
	second, rel2 := next(t, s)
	if second.ID != "b1" {
		t.Fatalf("second = %s, want b1 (no head-of-line blocking)", second.ID)
	}
	rel()
	rel2()
	third, rel3 := next(t, s)
	if third.ID != "a2" {
		t.Fatalf("third = %s, want a2", third.ID)
	}
	rel3()
}

func TestQueueBounds(t *testing.T) {
	s := New(2)
	mustRegister(t, s, "a", 10, 1)
	if err := s.Enqueue(job("j1", "a@v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Enqueue(job("j2", "a@v1")); err != ErrAgentQueueFull {
		t.Fatalf("want ErrAgentQueueFull, got %v", err)
	}

	s2 := New(2)
	mustRegister(t, s2, "a", 10, 0)
	_ = s2.Enqueue(job("j1", "a@v1"))
	_ = s2.Enqueue(job("j2", "a@v1"))
	if err := s2.Enqueue(job("j3", "a@v1")); err != ErrQueueFull {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
}

func TestLazyCancellationDiscardsWithoutConsumingQuota(t *testing.T) {
	s := New(10)
	mustRegister(t, s, "a", 1, 0)
	_ = s.Enqueue(job("dead", "a@v1"))
	_ = s.Enqueue(job("live", "a@v1"))
	s.MarkCancelled("dead")

	j, rel := next(t, s)
	if j.ID != "live" {
		t.Fatalf("got %s, want live", j.ID)
	}
	_, stats := s.Snapshot()
	if stats[0].Running != 1 || stats[0].Queued != 0 {
		t.Fatalf("stats after lazy cancel = %+v", stats[0])
	}
	rel()
}

func TestCloseUnblocksNext(t *testing.T) {
	s := New(10)
	mustRegister(t, s, "a", 1, 0)
	done := make(chan error, 1)
	go func() {
		_, _, err := s.Next(context.Background())
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	s.Close()
	select {
	case err := <-done:
		if err != ErrClosed {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next did not unblock")
	}
	s.Close() // idempotent
}

func TestEveryQueuedJobIsDispatchedExactlyOnce(t *testing.T) {
	s := New(1000)
	mustRegister(t, s, "a", 100, 0)
	const n = 100
	for i := 0; i < n; i++ {
		if err := s.Enqueue(&Job{ID: id(i), AgentKey: "a@v1"}); err != nil {
			t.Fatal(err)
		}
	}
	// All workers block on Next before the jobs are visible to them: this is
	// the regression test for the single notification token starvation bug.
	errs := make(chan error, n)
	seen := make(chan string, n)
	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			j, rel, err := s.Next(ctx)
			if err != nil {
				errs <- err
				return
			}
			seen <- j.ID
			rel()
		}()
	}
	wg.Wait()
	close(errs)
	close(seen)
	for err := range errs {
		t.Fatalf("Next error: %v", err)
	}
	unique := map[string]bool{}
	for id := range seen {
		unique[id] = true
	}
	if len(unique) != n {
		t.Fatalf("dispatched %d unique jobs, want %d", len(unique), n)
	}
}

func id(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "j0"
	}
	s := ""
	for i > 0 {
		s = string(digits[i%10]) + s
		i /= 10
	}
	return "j" + s
}
