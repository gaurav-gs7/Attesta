package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kubefake "k8s.io/client-go/kubernetes/fake"
)

// replica is one controller process competing for the Lease.
type replica struct {
	cancel  context.CancelFunc
	done    chan error
	leading atomic.Bool
}

// tracker records how many replicas are inside work at once.
type tracker struct {
	mu        sync.Mutex
	active    map[string]bool
	maxActive int
	worked    map[string]bool
}

func (tr *tracker) enter(id string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.active[id] = true
	tr.worked[id] = true
	if n := len(tr.active); n > tr.maxActive {
		tr.maxActive = n
	}
}

func (tr *tracker) exit(id string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	delete(tr.active, id)
}

func (tr *tracker) snapshot() (maxActive int, worked map[string]bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	w := map[string]bool{}
	for k, v := range tr.worked {
		w[k] = v
	}
	return tr.maxActive, w
}

func testOptions() options {
	return options{
		leaderElectionNS: "attesta-system",
		leaderElectionID: "attesta-controller",
		leaseDuration:    1 * time.Second,
		renewDeadline:    800 * time.Millisecond,
		retryPeriod:      100 * time.Millisecond,
	}
}

// startReplica runs one candidate. Its work runs until cancelled, then takes
// drain to return, standing in for a reconcile that is mid-rollback.
func startReplica(kube *kubefake.Clientset, id string, tr *tracker, drain time.Duration) *replica {
	ctx, cancel := context.WithCancel(context.Background())
	r := &replica{cancel: cancel, done: make(chan error, 1)}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		r.done <- runWithLeaderElection(ctx, kube, id, testOptions(), logger, &r.leading, func(workCtx context.Context) error {
			tr.enter(id)
			<-workCtx.Done()
			time.Sleep(drain)
			tr.exit(id)
			return nil
		})
	}()
	return r
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Audit C-10: with two replicas only one does work at a time, and on a
// graceful shutdown the standby takes over only after the old leader's work
// has returned, not as soon as its context is cancelled.
func TestLeaderElectionNeverOverlapsWork(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	tr := &tracker{active: map[string]bool{}, worked: map[string]bool{}}
	const drain = 500 * time.Millisecond // longer than retryPeriod, shorter than renewDeadline

	a := startReplica(kube, "replica-a", tr, drain)
	waitFor(t, "replica-a to lead", a.leading.Load)
	b := startReplica(kube, "replica-b", tr, drain)
	time.Sleep(3 * testOptions().leaseDuration) // b keeps retrying the held lease
	if b.leading.Load() {
		t.Fatal("standby acquired the lease while the leader was healthy")
	}

	a.cancel() // SIGTERM to the leader
	if err := <-a.done; err != nil {
		t.Fatalf("leader shutdown returned %v", err)
	}
	waitFor(t, "replica-b to take over", b.leading.Load)

	maxActive, worked := tr.snapshot()
	if maxActive != 1 {
		t.Fatalf("%d replicas were working at the same time, want 1", maxActive)
	}
	if !worked["replica-a"] || !worked["replica-b"] {
		t.Fatalf("expected both replicas to work in turn, got %v", worked)
	}

	b.cancel()
	if err := <-b.done; err != nil {
		t.Fatalf("second leader shutdown returned %v", err)
	}
}
