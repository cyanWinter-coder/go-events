package events

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// FullQueuePolicy decides what the [Broadcaster] does when a sink's bounded
// backlog is full. It only ever affects the sink whose backlog overflowed;
// other sinks and [Broadcaster.Write] callers keep progressing.
type FullQueuePolicy int

const (
	// PolicyDropNew rejects the new event for the full sink only. The event
	// is counted against that sink's drop statistics with [DropQueueFull]
	// and dispatch to every other sink is unaffected.
	PolicyDropNew FullQueuePolicy = iota

	// PolicyRemoveSink removes the offending sink from the broadcaster. Its
	// queued events are flushed once and the sink's backlog is shut down;
	// the events still pending when it is detached are counted with
	// [DropSinkRemoved]. The sink itself is not closed.
	PolicyRemoveSink
)

// BroadcasterConfig configures the per-sink isolation of a [Broadcaster].
// The zero value is valid and keeps the historical unbounded behavior.
type BroadcasterConfig struct {
	// PerSinkQueueCapacity bounds the backlog of every sink. A zero value
	// means unbounded: a slow sink can accumulate without limit but events
	// are never rejected. A positive value enables [FullQueuePolicy].
	PerSinkQueueCapacity int

	// FullQueuePolicy applies only when PerSinkQueueCapacity is positive.
	// Defaults to [PolicyDropNew].
	FullQueuePolicy FullQueuePolicy

	// DrainTimeout bounds how long [Broadcaster.Close] waits for each sink
	// backlog to flush. A zero value waits for every queued event to be
	// delivered. Events unflushed when the deadline elapses are expired and
	// counted with [DropDrainTimeout], and Close returns an error joining
	// [ErrBroadcasterDrainTimeout] with any sink close errors.
	DrainTimeout time.Duration
}

// pendingReadd is a fresh sink queue parked while the sink's previous
// registration drains, together with the Add call waiting for it to go live.
type pendingReadd struct {
	q        *sinkQueue
	response chan error
}

// Broadcaster sends events to multiple Sinks. Each registered sink owns an
// independent, optionally bounded backlog drained by its own goroutine: a
// single slow sink can fill (and overflow) only its own backlog and can never
// block event publication or delivery to the healthy sinks. Overflow and
// shutdown behavior is configured through [BroadcasterConfig].
//
// All events accepted by Write are serialized with Add/Remove/Close in the
// broadcaster's single run loop, so an event is never delivered to a sink that
// was removed before the Write that carried it, and each queued event reaches
// a given sink at most once.
//
// The Sink interface is unchanged; reliability can still be provided by
// wrapping incoming sinks with [Queue] and [RetryingSink].
type Broadcaster struct {
	config BroadcasterConfig

	// Registry state is touched by the run loop and by the drain-completion
	// callbacks under mu. Lifecycle of one sink registration:
	//
	//	active -> (Remove/evict) -> draining -> (drain done) -> retired
	//
	// An Add while draining parks a fresh queue in pendingReadds so the run
	// loop never blocks; the drain callback swaps it into active atomically.
	mu       sync.RWMutex
	active   map[Sink]*sinkQueue
	draining map[Sink]*sinkQueue
	retired  map[Sink]*sinkQueue
	// evicted holds queues detached by PolicyRemoveSink after their backlog
	// finished draining. They never receive events again, but every later
	// event addressed to their sink is still counted against them as a
	// DropSinkRemoved drop so the loss stays visible.
	evicted       map[Sink]*sinkQueue
	pendingReadds map[Sink]pendingReadd

	events   chan Event
	adds     chan configureRequest
	removes  chan configureRequest
	shutdown chan struct{}
	closed   chan struct{}

	closeMu  sync.Mutex
	closeErr error
	once     sync.Once
	bgWG     sync.WaitGroup
}

// NewBroadcaster appends one or more sinks to the list of sinks and starts a
// broadcaster with unbounded per-sink backlogs, matching the historical
// behavior. For bounded, isolated backlogs use [NewBroadcasterWithConfig].
func NewBroadcaster(sinks ...Sink) *Broadcaster {
	return NewBroadcasterWithConfig(BroadcasterConfig{}, sinks...)
}

// NewBroadcasterWithConfig starts a broadcaster using the given config and
// appends the provided sinks.
func NewBroadcasterWithConfig(config BroadcasterConfig, sinks ...Sink) *Broadcaster {
	b := &Broadcaster{
		config:        config,
		active:        make(map[Sink]*sinkQueue),
		draining:      make(map[Sink]*sinkQueue),
		retired:       make(map[Sink]*sinkQueue),
		evicted:       make(map[Sink]*sinkQueue),
		pendingReadds: make(map[Sink]pendingReadd),
		events:        make(chan Event),
		adds:          make(chan configureRequest),
		removes:       make(chan configureRequest),
		shutdown:      make(chan struct{}),
		closed:        make(chan struct{}),
	}

	for _, sink := range sinks {
		b.active[sink] = newSinkQueue(sink, config.PerSinkQueueCapacity)
	}

	go b.run()

	return b
}

// Write accepts an event to be dispatched to all active sinks. It never blocks
// on a slow sink: enqueue into each sink's independent backlog is
// non-blocking, and an overflow is handled per sink according to the
// configured [FullQueuePolicy] while the counters stay queryable via
// [Broadcaster.Stats].
func (b *Broadcaster) Write(event Event) error {
	select {
	case b.events <- event:
	case <-b.closed:
		return ErrSinkClosed
	}
	return nil
}

// Add the sink to the broadcaster.
//
// The provided sink must be comparable with equality. Typically, this just
// works with a regular pointer type. Adding the same sink again is a no-op.
// Re-adding a sink that was previously removed blocks until the previous
// registration has fully flushed, so events queued before the removal are
// delivered exactly once before the new registration takes over.
func (b *Broadcaster) Add(sink Sink) error {
	return b.configure(b.adds, sink)
}

// Remove the provided sink. Events already accepted by Write are delivered to
// it exactly once before Remove returns; events written after Remove returns
// are never delivered to it. The underlying sink is not closed.
func (b *Broadcaster) Remove(sink Sink) error {
	return b.configure(b.removes, sink)
}

type configureRequest struct {
	sink     Sink
	response chan error
}

func (b *Broadcaster) configure(ch chan configureRequest, sink Sink) error {
	response := make(chan error, 1)

	for {
		select {
		case ch <- configureRequest{sink: sink, response: response}:
			ch = nil
		case err := <-response:
			return err
		case <-b.closed:
			return ErrSinkClosed
		}
	}
}

// Close stops accepting new events, flushes drainable backlogs within the
// configured drain deadline and closes every underlying sink. A failure to
// close one sink does not prevent the others from being closed; the joined
// error (if any) reports every failure and every [ErrBroadcasterDrainTimeout].
// Close is idempotent and repeated calls return the same result.
func (b *Broadcaster) Close() error {
	b.once.Do(func() {
		close(b.shutdown)
		<-b.closed
		b.bgWG.Wait()
	})

	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	return b.closeErr
}

// run is the single goroutine owning the sink registry. Serializing events and
// add/remove requests here is what gives the exactly-once ordering guarantee:
// the only place a sink can be detached is between two fully dispatched
// events. Slow sink writes happen on the sinkQueue goroutines and cannot stall
// this loop.
func (b *Broadcaster) run() {
	defer close(b.closed)

	for {
		select {
		case event := <-b.events:
			b.dispatch(event)
		case request := <-b.adds:
			b.handleAdd(request)
		case request := <-b.removes:
			b.handleRemove(request)
		case <-b.shutdown:
			b.closeErr = b.shutdownAll()
			return
		}
	}
}

func (b *Broadcaster) handleAdd(request configureRequest) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.active[request.sink]; ok {
		request.response <- nil
		return
	}

	// Once shutdown started no new registration is possible. The configure
	// call's select on b.closed turns this into ErrSinkClosed.
	select {
	case <-b.closed:
		return
	default:
	}

	// A fresh queue is created up front. If the sink's previous registration
	// is still draining, the queue waits in pendingReadds and the drain
	// completion callback installs it without this goroutine blocking.
	q := newSinkQueue(request.sink, b.config.PerSinkQueueCapacity)
	if _, ok := b.draining[request.sink]; ok {
		b.pendingReadds[request.sink] = pendingReadd{q: q, response: request.response}
		return // response is sent by the drain callback
	}

	delete(b.retired, request.sink)
	delete(b.evicted, request.sink)
	b.active[request.sink] = q
	request.response <- nil
}

func (b *Broadcaster) handleRemove(request configureRequest) {
	b.mu.Lock()
	q, ok := b.active[request.sink]
	if ok {
		delete(b.active, request.sink)
		b.draining[request.sink] = q
	}
	b.mu.Unlock()

	if !ok {
		request.response <- nil
		return
	}

	// Detach first so the run loop keeps dispatching to the other sinks,
	// then wait for the removed queue to flush before answering: Remove
	// returns only after events accepted before it have been delivered once.
	b.detach(request.sink, q, request.response, false)
}

// detach shuts a queue down in the background and reconciles the registry
// once draining completes. response, when non-nil, is signaled only after the
// queue has fully drained. policyEvicted marks a PolicyRemoveSink eviction:
// the drained queue is retained in evicted so later events stay counted as
// drops. It must not be called with mu held.
func (b *Broadcaster) detach(sink Sink, q *sinkQueue, response chan error, policyEvicted bool) {
	q.policyEvicted = policyEvicted
	b.bgWG.Add(1)
	go func() {
		defer b.bgWG.Done()
		err := q.shutdown(b.config.DrainTimeout, DropSinkRemoved, false)

		b.mu.Lock()
		delete(b.draining, sink)
		var pendingResponse chan error
		if pending, ok := b.pendingReadds[sink]; ok {
			// A re-Add raced the removal: hand over to the fresh queue.
			delete(b.pendingReadds, sink)
			b.active[sink] = pending.q
			b.retired[sink] = q
			pendingResponse = pending.response
		} else if policyEvicted {
			b.evicted[sink] = q
		} else {
			b.retired[sink] = q
		}
		b.mu.Unlock()

		if pendingResponse != nil {
			pendingResponse <- nil
		}
		if response != nil {
			response <- err
		}
	}()
}

func (b *Broadcaster) dispatch(event Event) {
	// Snapshot the active queues under the read lock; the actual enqueues and
	// overflow handling happen without it so a slow sink can never hold up
	// registry access or the other sinks.
	b.mu.RLock()
	queues := make([]*sinkQueue, 0, len(b.active))
	for _, q := range b.active {
		queues = append(queues, q)
	}
	// detachedQueues covers queues already pulled out of active (still
	// draining, or fully evicted). Queues detached by PolicyRemoveSink will
	// never receive another event, so every event published in the meantime is
	// counted against them; an explicitly Removed sink is not charged.
	detachedQueues := make([]*sinkQueue, 0, len(b.draining)+len(b.evicted))
	for _, q := range b.draining {
		if q.policyEvicted {
			detachedQueues = append(detachedQueues, q)
		}
	}
	for _, q := range b.evicted {
		detachedQueues = append(detachedQueues, q)
	}
	b.mu.RUnlock()

	for _, q := range detachedQueues {
		q.recordDrop(1, DropSinkRemoved)
	}

	for _, q := range queues {
		if !q.enqueue(event) {
			continue
		}

		// Bounded backlog full. Apply the configured overflow policy without
		// doing slow work under the registry lock.
		b.mu.Lock()
		cur, ok := b.active[q.sink]
		same := ok && cur == q
		if same && b.config.FullQueuePolicy == PolicyRemoveSink {
			delete(b.active, q.sink)
			b.draining[q.sink] = q
		}
		b.mu.Unlock()

		if same && b.config.FullQueuePolicy == PolicyRemoveSink {
			// The event that overflowed the backlog is itself the first event
			// the evicted sink will never receive; count it immediately.
			q.recordDrop(1, DropSinkRemoved)
			b.detach(q.sink, q, nil, true)
			continue
		}
		if same {
			q.recordDrop(1, DropQueueFull)
		}
	}
}

// shutdownAll drains and closes every sink still registered, plus any queue
// still draining from an earlier removal and queues parked for a re-Add. Each
// sink is shut down independently so one failing or stuck close cannot block
// the rest.
func (b *Broadcaster) shutdownAll() error {
	b.mu.Lock()
	queues := make([]*sinkQueue, 0, len(b.active)+len(b.draining)+len(b.evicted)+len(b.retired)+len(b.pendingReadds))
	seen := make(map[*sinkQueue]struct{})
	collect := func(q *sinkQueue) {
		if _, ok := seen[q]; ok {
			return
		}
		seen[q] = struct{}{}
		queues = append(queues, q)
	}
	// Retired queues (e.g. an explicitly Removed sink that already drained)
	// are collected first so their underlying sink still gets closed; the
	// seen set makes the moves below idempotent when a key already exists.
	for _, q := range b.retired {
		collect(q)
	}
	for sink, q := range b.active {
		collect(q)
		b.retired[sink] = q
	}
	for sink, q := range b.draining {
		collect(q)
		b.retired[sink] = q
	}
	for sink, q := range b.evicted {
		collect(q)
		b.retired[sink] = q
	}
	// Pending re-Add queues never received events, but their goroutines still
	// have to be stopped and the waiting Add call released.
	for sink, pending := range b.pendingReadds {
		collect(pending.q)
		b.retired[sink] = pending.q
		pending.response <- ErrSinkClosed
	}
	clear(b.active)
	clear(b.draining)
	clear(b.evicted)
	clear(b.pendingReadds)
	b.mu.Unlock()

	var (
		wg     sync.WaitGroup
		errMu  sync.Mutex
		joined error
	)
	for _, q := range queues {
		wg.Add(1)
		go func(q *sinkQueue) {
			defer wg.Done()
			// Drain within the deadline. Queues removed/evicted earlier already
			// ran shutdown with closeSink=false; the once makes this a no-op for
			// draining, and closeUnderlyingSink below still closes their sink.
			if err := q.shutdown(b.config.DrainTimeout, DropDrainTimeout, true); err != nil {
				errMu.Lock()
				joined = errors.Join(joined, err)
				errMu.Unlock()
			}
			// Idempotent: closes the sink once even when shutdown was already
			// consumed. Independent per queue, so one failure cannot block the
			// rest (requirement 5).
			if err := q.closeUnderlyingSink(); err != nil {
				errMu.Lock()
				joined = errors.Join(joined, err)
				errMu.Unlock()
			}
		}(q)
	}
	wg.Wait()
	return joined
}

// Stats returns the delivery accounting of every sink that was ever
// registered, including sinks removed by [Broadcaster.Remove] or evicted by
// [PolicyRemoveSink]. The map is keyed by the sink itself.
func (b *Broadcaster) Stats() map[Sink]SinkStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := make(map[Sink]SinkStats, len(b.active)+len(b.draining)+len(b.evicted)+len(b.retired))
	for sink, q := range b.active {
		stats[sink] = q.stats(true)
	}
	for sink, q := range b.draining {
		if _, ok := stats[sink]; !ok {
			stats[sink] = q.stats(false)
		}
	}
	for sink, q := range b.evicted {
		if _, ok := stats[sink]; !ok {
			stats[sink] = q.stats(false)
		}
	}
	for sink, q := range b.retired {
		if _, ok := stats[sink]; !ok {
			stats[sink] = q.stats(false)
		}
	}
	return stats
}

// SinkStats returns the delivery accounting of a single sink. The boolean is
// false if the sink is not registered and has never been registered.
func (b *Broadcaster) SinkStats(sink Sink) (SinkStats, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if q, ok := b.active[sink]; ok {
		return q.stats(true), true
	}
	if q, ok := b.draining[sink]; ok {
		return q.stats(false), true
	}
	if q, ok := b.evicted[sink]; ok {
		return q.stats(false), true
	}
	if q, ok := b.retired[sink]; ok {
		return q.stats(false), true
	}
	return SinkStats{Sink: sink}, false
}

func (b *Broadcaster) String() string {
	// Serialize a copy of this broadcaster without the mutexes, to avoid a
	// data race.
	return fmt.Sprint(map[string]any{
		"config":   b.config,
		"events":   b.events,
		"shutdown": b.shutdown,
		"closed":   b.closed,
	})
}
