package repository

import (
	"errors"
	"testing"
	"time"

	"goagent-lite/internal/task"
)

func newTask(id string) *task.Task {
	return &task.Task{
		ID:           id,
		AgentID:      "a",
		AgentVersion: "v1",
		Status:       task.StatusQueued,
		CreatedAt:    time.Now().UTC(),
	}
}

func TestCreateGetCloneSemantics(t *testing.T) {
	r := NewMemory()
	in := newTask("t1")
	in.Metadata = map[string]string{"k": "v"}
	if err := r.Create(in); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(in); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate create: %v", err)
	}
	got, err := r.Get("t1")
	if err != nil {
		t.Fatal(err)
	}
	got.Status = task.StatusRunning
	got.Metadata["k"] = "mutated"
	again, _ := r.Get("t1")
	if again.Status != task.StatusQueued || again.Metadata["k"] != "v" {
		t.Fatal("store leaked internal state through a clone")
	}
}

func TestTransitionCASAndTerminalSingleSet(t *testing.T) {
	r := NewMemory()
	if err := r.Create(newTask("t1")); err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC()
	if err := r.Transition("t1", task.StatusRunning, func(x *task.Task) { x.StartedAt = &started }); err != nil {
		t.Fatal(err)
	}
	if err := r.Transition("t1", task.StatusSucceeded, func(x *task.Task) {
		x.Result = []byte(`{"ok":true}`)
	}); err != nil {
		t.Fatal(err)
	}
	err := r.Transition("t1", task.StatusFailed, nil)
	if !task.IsTransitionError(err) {
		t.Fatalf("second terminal transition must fail, got %v", err)
	}
	got, _ := r.Get("t1")
	if got.Status != task.StatusSucceeded || got.FinishedAt == nil || string(got.Result) != `{"ok":true}` {
		t.Fatalf("unexpected task: %+v", got)
	}
}

func TestTransitionNotFound(t *testing.T) {
	r := NewMemory()
	if err := r.Transition("missing", task.StatusRunning, nil); !errors.Is(err, task.ErrTaskNotFound) {
		t.Fatalf("want ErrTaskNotFound, got %v", err)
	}
	if _, err := r.Get("missing"); !errors.Is(err, task.ErrTaskNotFound) {
		t.Fatalf("want ErrTaskNotFound, got %v", err)
	}
}

func TestListDelete(t *testing.T) {
	r := NewMemory()
	_ = r.Create(newTask("a"))
	_ = r.Create(newTask("b"))
	if got := len(r.List()); got != 2 {
		t.Fatalf("list len = %d", got)
	}
	if err := r.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if got := len(r.List()); got != 1 {
		t.Fatalf("list len after delete = %d", got)
	}
	if err := r.Delete("a"); err != nil {
		t.Fatalf("delete is idempotent, got %v", err)
	}
}
