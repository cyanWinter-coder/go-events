package events

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingSink collects every event without a predeclared capacity, unlike
// testSink whose expected count makes it unsuitable for open-ended tests.
type recordingSink struct {
	mu     sync.Mutex
	events []Event
	closed bool
}

func (s *recordingSink) Write(event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSinkClosed
	}
	s.events = append(s.events, event)
	return nil
}

func (s *recordingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *recordingSink) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// blockingSink blocks every Write until released or closed, simulating a
// stalled audit receiver. It records everything it eventually receives.
type blockingSink struct {
	release     chan struct{}
	releaseOnce sync.Once
	closed      chan struct{}
	once        sync.Once

	mu     sync.Mutex
	events []Event
}

func newBlockingSink() *blockingSink {
	return &blockingSink{
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (s *blockingSink) Write(event Event) error {
	select {
	case <-s.release:
	case <-s.closed:
		return ErrSinkClosed
	}
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
	return nil
}

func (s *blockingSink) releaseWrites() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func (s *blockingSink) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func (s *blockingSink) snapshot() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// closeErrorSink fails Close exactly once and blocks Write until closed, so
// tests can prove close failures are isolated and shutdowns still terminate.
type closeErrorSink struct {
	closed chan struct{}
	once   sync.Once
}

func newCloseErrorSink() *closeErrorSink {
	return &closeErrorSink{closed: make(chan struct{})}
}

func (s *closeErrorSink) Write(event Event) error {
	<-s.closed
	return ErrSinkClosed
}

func (s *closeErrorSink) Close() error {
	s.once.Do(func() { close(s.closed) })
	return errors.New("sink close boom")
}

// waitFor blocks until cond is true or the test deadline approaches.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

// TestBroadcasterSlowSinkIsolation verifies requirement 1 & 2 (drop policy):
// a stalled sink fills only its own bounded backlog; healthy sinks and the
// publisher keep moving, and every rejected event is counted with a reason.
func TestBroadcasterSlowSinkIsolation(t *testing.T) {
	const capacity = 4

	slow := newBlockingSink()
	healthy := &recordingSink{}

	b := NewBroadcasterWithConfig(BroadcasterConfig{
		PerSinkQueueCapacity: capacity,
		FullQueuePolicy:      PolicyDropNew,
	}, slow, healthy)
	t.Cleanup(func() {
		slow.releaseWrites()
		_ = b.Close()
	})

	const nEvents = 30
	// Publish with enough pacing that a healthy consumer can always keep its
	// backlog drained. The point of the test is isolation: the stalled sink
	// must not change that, while its own backlog fills and overflows.
	for i := range nEvents {
		if err := b.Write(fmt.Sprintf("event-%d", i)); err != nil {
			t.Fatalf("Write(%d) failed: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)

		// The healthy sink must have received everything so far, even though
		// the slow sink's first write never returned.
		waitFor(t, func() bool {
			return healthy.len() == i+1
		}, fmt.Sprintf("healthy sink to receive event %d despite stalled sink", i))
	}

	slowStats, ok := b.SinkStats(slow)
	if !ok {
		t.Fatal("no stats for slow sink")
	}
	// The slow sink never completed a write, so it holds at most its backlog
	// capacity (one in flight plus the queued events); everything else must
	// be recorded as queue-full drops.
	if slowStats.Delivered != 0 {
		t.Fatalf("slow sink delivered %d events, want 0", slowStats.Delivered)
	}
	if slowStats.Dropped < nEvents-capacity-1 {
		t.Fatalf("slow sink dropped %d events, want at least %d", slowStats.Dropped, nEvents-capacity-1)
	}
	if slowStats.Dropped+slowStats.Delivered > nEvents {
		t.Fatalf("accounting overcounts: dropped %d + delivered %d > %d",
			slowStats.Dropped, slowStats.Delivered, nEvents)
	}
	if slowStats.Drops[DropQueueFull] != slowStats.Dropped {
		t.Fatalf("all drops must be DropQueueFull, got drops: %v", slowStats.Drops)
	}

	healthyStats, _ := b.SinkStats(healthy)
	if healthyStats.Dropped != 0 || healthyStats.Delivered != nEvents {
		t.Fatalf("healthy sink stats = %+v, want no drops and %d delivered", healthyStats, nEvents)
	}

	// Releasing the slow sink lets it consume whatever survived in its
	// backlog without ever blocking the broadcaster; delivery stays ordered.
	survived := int(nEvents - slowStats.Dropped)
	slow.releaseWrites()
	waitFor(t, func() bool {
		return len(slow.snapshot()) == survived
	}, "slow sink to drain its surviving backlog")

	got := slow.snapshot()
	if len(got) != survived {
		t.Fatalf("slow sink delivered %d after release, want %d", len(got), survived)
	}
	for i, event := range got {
		// The surviving events are the lowest-numbered ones, in order.
		want := fmt.Sprintf("event-%d", i)
		if event != want {
			t.Fatalf("slow sink event %d = %v, want %v (ordering broken)", i, event, want)
		}
	}
}

// TestBroadcasterRemoveSinkPolicy verifies requirement 2 (remove policy): a
// full sink is evicted, its queued events are flushed once, remaining events
// are accounted for as DropSinkRemoved, and other sinks are untouched.
func TestBroadcasterRemoveSinkPolicy(t *testing.T) {
	const capacity = 4

	slow := newBlockingSink()
	healthy := &recordingSink{}

	b := NewBroadcasterWithConfig(BroadcasterConfig{
		PerSinkQueueCapacity: capacity,
		FullQueuePolicy:      PolicyRemoveSink,
		// A bounded drain deadline lets the eviction of a sink whose Write is
		// stuck complete instead of waiting forever.
		DrainTimeout: 100 * time.Millisecond,
	}, slow, healthy)

	const nEvents = 20
	for i := range nEvents {
		if err := b.Write(fmt.Sprintf("event-%d", i)); err != nil {
			t.Fatalf("Write(%d) failed: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)

		// The healthy sink stays fully current while the slow sink backs up.
		waitFor(t, func() bool {
			return healthy.len() == i+1
		}, fmt.Sprintf("healthy sink to receive event %d", i))
	}

	waitFor(t, func() bool {
		stats, ok := b.SinkStats(slow)
		return ok && !stats.Active
	}, "slow sink to be evicted")

	healthyGot := healthy.len()
	if healthyGot != nEvents {
		t.Fatalf("healthy sink got %d events, want %d", healthyGot, nEvents)
	}

	slowStats, _ := b.SinkStats(slow)
	if slowStats.Active {
		t.Fatal("evicted sink must report inactive")
	}
	// At eviction the slow sink had not completed a write, so nothing is
	// delivered yet. The event that triggered the overflow is recorded as a
	// removal drop; the rest of the backlog is held for a final flush.
	if slowStats.Delivered != 0 {
		t.Fatalf("evicted sink delivered %d, want 0", slowStats.Delivered)
	}
	if slowStats.Drops[DropQueueFull] != 0 {
		t.Fatalf("PolicyRemoveSink must not count DropQueueFull, got %d", slowStats.Drops[DropQueueFull])
	}
	if slowStats.Drops[DropSinkRemoved] == 0 {
		t.Fatalf("eviction must record DropSinkRemoved drops, got: %v", slowStats.Drops)
	}

	// Events delivered after eviction never reach the removed sink; release
	// it and confirm only the events held at eviction were flushed, in order.
	for i := nEvents; i < nEvents+10; i++ {
		if err := b.Write(fmt.Sprintf("event-%d", i)); err != nil {
			t.Fatalf("Write after eviction failed: %v", err)
		}
		time.Sleep(time.Millisecond)
	}

	slow.releaseWrites()
	// The evicted sink delivers exactly the events it held (queued plus the
	// one in flight) at eviction; wait for that backlog to finish draining.
	waitFor(t, func() bool {
		final, _ := b.SinkStats(slow)
		return len(slow.snapshot()) == int(final.Delivered) && final.Delivered > 0
	}, "evicted sink to flush its surviving backlog")

	got := slow.snapshot()

	// Full accounting across every event published to this broadcaster: the
	// events held at eviction were delivered once, every other event (the
	// overflow and everything published after eviction) is a removal drop.
	// Nothing is unaccounted for and nothing is duplicated.
	const totalPublished = nEvents + 10
	finalStats, _ := b.SinkStats(slow)
	if int(finalStats.Delivered)+int(finalStats.Dropped) != totalPublished {
		t.Fatalf("eviction accounting lost events: delivered %d + dropped %d != %d",
			finalStats.Delivered, finalStats.Dropped, totalPublished)
	}
	// All non-deliveries must be attributed to the eviction, never to queue-full.
	if finalStats.Drops[DropQueueFull] != 0 {
		t.Fatalf("PolicyRemoveSink must not count DropQueueFull, got %d", finalStats.Drops[DropQueueFull])
	}
	if finalStats.Drops[DropSinkRemoved] != finalStats.Dropped {
		t.Fatalf("all drops must be DropSinkRemoved, got %v", finalStats.Drops)
	}
	// Delivery is ordered and stops at the events present at eviction time, so
	// none of the post-eviction events (event-nEvents onward) reached it.
	for i, event := range got {
		if want := fmt.Sprintf("event-%d", i); event != want {
			t.Fatalf("evicted sink event %d = %v, want %v", i, event, want)
		}
	}
	if len(got) >= nEvents {
		t.Fatalf("evicted sink received %d events, must not see post-eviction events", len(got))
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestBroadcasterRemoveFlushesExactlyOnce verifies requirement 3: Remove
// returns only after queued events are delivered once, and later writes never
// reach the removed sink; re-adding restarts delivery without duplicates.
func TestBroadcasterRemoveFlushesExactlyOnce(t *testing.T) {
	sink := &recordingSink{}
	b := NewBroadcasterWithConfig(BroadcasterConfig{
		PerSinkQueueCapacity: 16,
		FullQueuePolicy:      PolicyDropNew,
	}, sink)

	const nEvents = 10
	for i := range nEvents {
		if err := b.Write(i); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := b.Remove(sink); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	sink.mu.Lock()
	got := append([]Event(nil), sink.events...)
	sink.mu.Unlock()
	if len(got) != nEvents {
		t.Fatalf("sink got %d events before Remove returned, want %d", len(got), nEvents)
	}
	for i, event := range got {
		if event != i {
			t.Fatalf("event %d = %v, want %d (ordering broken)", i, event, i)
		}
	}

	// Nothing written after Remove may land in the sink.
	for i := nEvents; i < nEvents+5; i++ {
		if err := b.Write(i); err != nil {
			t.Fatalf("Write after Remove: %v", err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	sink.mu.Lock()
	if len(sink.events) != nEvents {
		t.Fatalf("removed sink received %d events, want %d", len(sink.events), nEvents)
	}
	sink.mu.Unlock()

	// Re-add: old events stay at nEvents, new ones append exactly once.
	if err := b.Add(sink); err != nil {
		t.Fatalf("re-Add: %v", err)
	}
	for i := nEvents; i < nEvents+5; i++ {
		if err := b.Write(i); err != nil {
			t.Fatalf("Write after re-Add: %v", err)
		}
	}

	waitFor(t, func() bool {
		sink.mu.Lock()
		n := len(sink.events)
		sink.mu.Unlock()
		return n == nEvents+5
	}, "re-added sink to receive new events")

	sink.mu.Lock()
	for i, event := range sink.events {
		if event != i {
			t.Fatalf("re-added sink event %d = %v, want %d", i, event, i)
		}
	}
	sink.mu.Unlock()

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestBroadcasterCloseDrainTimeout verifies requirement 4: Close stops
// accepting writes, drains what it can within the deadline, expires the rest
// visibly, and surfaces the timeout as an error.
func TestBroadcasterCloseDrainTimeout(t *testing.T) {
	const capacity = 8

	slow := newBlockingSink()
	b := NewBroadcasterWithConfig(BroadcasterConfig{
		PerSinkQueueCapacity: capacity,
		FullQueuePolicy:      PolicyDropNew,
		DrainTimeout:         100 * time.Millisecond,
	}, slow)

	for i := range capacity {
		if err := b.Write(i); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	start := time.Now()
	err := b.Close()
	elapsed := time.Since(start)

	if !errors.Is(err, ErrBroadcasterDrainTimeout) {
		t.Fatalf("Close error = %v, want ErrBroadcasterDrainTimeout", err)
	}
	if elapsed >= time.Second {
		t.Fatalf("Close took %v, must be bounded by drain deadline", elapsed)
	}

	// New writes are rejected once Close has run.
	if err := b.Write("late"); !errors.Is(err, ErrSinkClosed) {
		t.Fatalf("Write after Close = %v, want ErrSinkClosed", err)
	}

	stats, ok := b.SinkStats(slow)
	if !ok {
		t.Fatal("stats missing after close")
	}
	// Nothing was ever delivered: the 7 queued events plus the one parked in
	// the blocked Write must all be accounted for as drain-timeout drops.
	if got := stats.Drops[DropDrainTimeout]; got != capacity {
		t.Fatalf("DropDrainTimeout = %d, want %d (stats: %+v)",
			got, capacity, stats)
	}
	if stats.Dropped != capacity {
		t.Fatalf("total Dropped = %d, want %d", stats.Dropped, capacity)
	}
	if stats.Delivered != 0 {
		t.Fatalf("Delivered = %d, want 0 for a sink that never unblocked", stats.Delivered)
	}
	if stats.Active {
		t.Fatal("closed sink must report inactive")
	}

	// Repeated Close is idempotent and returns the same error.
	err2 := b.Close()
	if !errors.Is(err2, ErrBroadcasterDrainTimeout) {
		t.Fatalf("second Close error = %v, want same timeout error", err2)
	}

	slow.releaseWrites() // unblock the goroutine parked in the sink write
}

// TestBroadcasterCloseDrainsHealthyQueues verifies that within the deadline,
// everything drainable is flushed and Close reports success.
func TestBroadcasterCloseDrainsHealthyQueues(t *testing.T) {
	const nEvents = 100
	sink := &recordingSink{}
	b := NewBroadcasterWithConfig(BroadcasterConfig{
		PerSinkQueueCapacity: nEvents,
		FullQueuePolicy:      PolicyDropNew,
		DrainTimeout:         5 * time.Second,
	}, sink)

	for i := range nEvents {
		if err := b.Write(i); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	stats, _ := b.SinkStats(sink)
	if stats.Delivered != nEvents || stats.Dropped != 0 {
		t.Fatalf("stats = %+v, want %d delivered, no drops", stats, nEvents)
	}
}

// TestBroadcasterCloseErrorIsolation verifies requirement 5: one sink failing
// Close does not stop the others from closing, the failure is visible in the
// joined error, double Close is idempotent, and no goroutines leak.
func TestBroadcasterCloseErrorIsolation(t *testing.T) {
	bad := newCloseErrorSink()
	good := &recordingSink{}

	before := runtime.NumGoroutine()
	b := NewBroadcasterWithConfig(BroadcasterConfig{
		PerSinkQueueCapacity: 4,
		DrainTimeout:         time.Second,
	}, bad, good)

	if err := b.Write("event"); err != nil {
		t.Fatalf("Write: %v", err)
	}

	err := b.Close()
	if err == nil {
		t.Fatal("Close must report the failing sink's error")
	}
	// The joined error must contain the bad sink's failure text.
	if !strings.Contains(err.Error(), "sink close boom") {
		t.Fatalf("Close error %v does not contain sink failure", err)
	}

	good.mu.Lock()
	goodClosed := good.closed
	good.mu.Unlock()
	if !goodClosed {
		t.Fatal("healthy sink must still be closed despite failing sink")
	}

	// Idempotent repeat: no panic, same error shape.
	if err := b.Close(); err == nil {
		t.Fatal("second Close should keep reporting the stored error")
	}

	waitFor(t, func() bool {
		// Allow the parked write goroutine (now unblocked by bad.Close) to
		// wind down; no broadcaster goroutine should remain.
		runtime.GC()
		return runtime.NumGoroutine() <= before+1
	}, "all broadcaster goroutines to exit")
}

// TestBroadcasterAddAfterClose verifies configuration calls fail cleanly once
// the broadcaster is shut down instead of hanging.
func TestBroadcasterAddAfterClose(t *testing.T) {
	b := NewBroadcaster()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sink := &recordingSink{}
	if err := b.Add(sink); !errors.Is(err, ErrSinkClosed) {
		t.Fatalf("Add after Close = %v, want ErrSinkClosed", err)
	}
	if err := b.Remove(sink); !errors.Is(err, ErrSinkClosed) {
		t.Fatalf("Remove after Close = %v, want ErrSinkClosed", err)
	}
}

// TestBroadcasterStatsUnknownSink confirms the stats lookup contract for
// sinks that were never registered.
func TestBroadcasterStatsUnknownSink(t *testing.T) {
	b := NewBroadcaster()
	t.Cleanup(func() { _ = b.Close() })

	sink := &recordingSink{}
	if _, ok := b.SinkStats(sink); ok {
		t.Fatal("unknown sink must not report stats")
	}
	if stats := b.Stats(); len(stats) != 0 {
		t.Fatalf("empty broadcaster Stats = %v, want empty map", stats)
	}
}

// TestBroadcasterRemovedSinkStillClosed verifies that a sink detached by an
// explicit Remove is still closed when the broadcaster itself closes: Remove
// must not take ownership of closing the sink, but Close must not forget it.
func TestBroadcasterRemovedSinkStillClosed(t *testing.T) {
	sink := &recordingSink{}
	b := NewBroadcasterWithConfig(BroadcasterConfig{PerSinkQueueCapacity: 4}, sink)

	if err := b.Write("event"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.Remove(sink); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if sink.closed {
		t.Fatal("Remove must not close the underlying sink")
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !sink.closed {
		t.Fatal("broadcaster Close must close the previously removed sink")
	}
}

// TestBroadcasterCloseIsIdempotentNoLeak checks that repeated Close calls are
// safe and that a normal bounded broadcaster leaves no goroutines behind.
func TestBroadcasterCloseIsIdempotentNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	sinks := make([]*recordingSink, 4)
	var registered []Sink
	for i := range sinks {
		sinks[i] = &recordingSink{}
		registered = append(registered, sinks[i])
	}
	b := NewBroadcasterWithConfig(BroadcasterConfig{
		PerSinkQueueCapacity: 8,
		DrainTimeout:         time.Second,
	}, registered...)

	for i := 0; i < 20; i++ {
		if err := b.Write(i); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	for i := 0; i < 3; i++ {
		if err := b.Close(); err != nil {
			t.Fatalf("Close call %d: %v", i, err)
		}
	}

	waitFor(t, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= before+1
	}, "all broadcaster goroutines to exit after Close")

	for _, sink := range sinks {
		if !sink.closed {
			t.Fatal("every sink must be closed after broadcaster Close")
		}
	}
}

// TestBroadcasterUnboundedBacklog verifies the zero-config path preserves the
// historical unbounded behavior: a slow sink never causes event rejection.
func TestBroadcasterUnboundedBacklog(t *testing.T) {
	slow := newBlockingSink()
	healthy := &recordingSink{}
	b := NewBroadcaster(slow, healthy) // zero config = unbounded
	t.Cleanup(func() {
		slow.releaseWrites()
		_ = b.Close()
	})

	const nEvents = 50
	for i := range nEvents {
		if err := b.Write(i); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}

	waitFor(t, func() bool {
		return healthy.len() == nEvents
	}, "healthy sink to receive every event")

	slowStats, _ := b.SinkStats(slow)
	if slowStats.Dropped != 0 {
		t.Fatalf("unbounded backlog must not drop events, got %+v", slowStats)
	}

	slow.releaseWrites()
	waitFor(t, func() bool {
		return len(slow.snapshot()) == nEvents
	}, "slow sink to receive every queued event after release")
}
