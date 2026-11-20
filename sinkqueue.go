package events

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// DropReason categorizes why an event was never delivered to a sink. It is
// recorded per sink in [SinkStats] and allows callers to tell apart events
// rejected because a backlog was full from events expired during shutdown.
type DropReason string

const (
	// DropQueueFull is recorded when a bounded sink backlog was full and
	// the broadcaster was configured with [PolicyDropNew].
	DropQueueFull DropReason = "queue_full"

	// DropSinkRemoved is recorded for the events still queued when a sink
	// is removed from the broadcaster (explicitly or via
	// [PolicyRemoveSink]).
	DropSinkRemoved DropReason = "sink_removed"

	// DropDrainTimeout is recorded for events that were still queued, or
	// stuck in an in-flight write, when the drain deadline of a Close
	// elapsed.
	DropDrainTimeout DropReason = "drain_timeout"

	// DropSinkClosed is recorded when an event could not be delivered
	// because the underlying sink had already been closed on its own.
	DropSinkClosed DropReason = "sink_closed"
)

// SinkStats reports the delivery accounting of one sink inside a
// [Broadcaster]. Counters are cumulative for the lifetime of the sink's
// registration.
type SinkStats struct {
	// Sink is the registered sink these statistics describe.
	Sink Sink

	// Delivered is the number of events successfully written to the sink.
	Delivered uint64

	// Dropped is the total number of events never delivered, broken down
	// by reason in Drops.
	Dropped uint64

	// Drops counts discarded events per [DropReason].
	Drops map[DropReason]uint64

	// Active reports whether the sink is still receiving events. It is
	// false once the sink has been removed or the broadcaster closed.
	Active bool
}

// sinkQueue is the per-sink isolation boundary inside a [Broadcaster]. Each
// registered sink gets its own goroutine draining an independent backlog, so
// a sink that is slow to write can only fill its own backlog and can never
// stall event dispatch to the other sinks or block [Broadcaster.Write].
//
// A capacity of zero selects an unbounded backlog (the historical broadcaster
// behavior); a positive capacity selects a bounded FIFO backed by a channel.
type sinkQueue struct {
	sink     Sink
	capacity int

	mu      sync.Mutex
	events  *list.List // unbounded backlog, used only when capacity == 0
	cond    *sync.Cond
	bounded chan Event // bounded backlog, used only when capacity > 0
	closed  bool

	delivered uint64
	dropped   uint64
	drops     map[DropReason]uint64
	inFlight  bool // an event is parked in the underlying sink's Write
	// policyEvicted marks a queue detached by PolicyRemoveSink rather than an
	// explicit Remove. Events published while such a queue is detached are
	// counted as DropSinkRemoved; an explicit Remove is not charged for them.
	policyEvicted bool
	// inFlightCounted marks the in-flight event as already accounted for by
	// expireBacklog, so run must not count it again when the write returns.
	inFlightCounted bool

	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
	doneOnce     sync.Once
	done         chan struct{} // closed when the drain goroutine has exited

	// sinkClosed records whether the underlying sink has already been closed
	// by this queue, so a later shutdown can close it exactly once even when
	// an earlier Remove/eviction drained the queue without closing the sink.
	sinkClosed  bool
	sinkCloseMu sync.Mutex
}

func newSinkQueue(sink Sink, capacity int) *sinkQueue {
	q := &sinkQueue{
		sink:         sink,
		capacity:     capacity,
		events:       list.New(),
		drops:        make(map[DropReason]uint64),
		shutdownDone: make(chan struct{}),
		done:         make(chan struct{}),
	}
	q.cond = sync.NewCond(&q.mu)
	if capacity > 0 {
		q.bounded = make(chan Event, capacity)
	}
	go q.run()
	return q
}

// enqueue appends an event to the backlog. It returns queueFull=true when a
// bounded backlog has no room, without blocking, so the broadcaster can apply
// its configured full-queue policy independently per sink.
func (q *sinkQueue) enqueue(event Event) (queueFull bool) {
	if q.capacity > 0 {
		select {
		case q.bounded <- event:
			return false
		default:
			return true
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		q.recordDropLocked(1, DropSinkClosed)
		return false
	}
	q.events.PushBack(event)
	q.cond.Signal()
	return false
}

// run is the single goroutine that serializes writes to the underlying sink,
// which preserves per-sink event ordering.
func (q *sinkQueue) run() {
	defer q.doneOnce.Do(func() { close(q.done) })

	for {
		event, ok := q.next()
		if !ok {
			return
		}

		q.mu.Lock()
		q.inFlight = true
		q.inFlightCounted = false
		q.mu.Unlock()

		err := q.sink.Write(event)

		q.mu.Lock()
		q.inFlight = false
		alreadyCounted := q.inFlightCounted
		if err != nil {
			if errors.Is(err, ErrSinkClosed) {
				q.closed = true
				if !alreadyCounted {
					q.recordDropLocked(1+uint64(q.backlogLenLocked()), DropSinkClosed)
				}
				q.mu.Unlock()
				return
			}
			q.mu.Unlock()
			logrus.WithFields(logrus.Fields{
				"error":       err,
				"event":       event,
				"events.sink": q.sink,
			}).Error("broadcaster: dropping event")
			continue
		}

		if !alreadyCounted {
			q.delivered++
		}
		q.mu.Unlock()
	}
}

// next returns the next queued event. The boolean is false when the backlog
// has been closed and fully drained (or expired), signaling run to exit.
func (q *sinkQueue) next() (Event, bool) {
	if q.capacity > 0 {
		event, ok := <-q.bounded
		return event, ok
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for q.events.Len() < 1 {
		if q.closed {
			return nil, false
		}
		q.cond.Wait()
	}

	front := q.events.Front()
	event := front.Value.(Event)
	q.events.Remove(front)
	return event, true
}

// shutdown stops accepting new events and flushes whatever can be flushed
// within deadline. When closeSink is true the underlying sink is closed as
// well; it must be false on Remove/eviction because the same sink object may be
// re-added under a fresh queue, and closing it here would make every later
// write fail with ErrSinkClosed. Only the broadcaster's own Close passes true.
//
// A zero or negative deadline waits for the full backlog. expireReason is
// recorded for any event still queued or stuck in an in-flight write when the
// deadline elapses. It is safe to call concurrently and repeatedly; all
// callers observe the same result.
func (q *sinkQueue) shutdown(deadline time.Duration, expireReason DropReason, closeSink bool) error {
	q.shutdownOnce.Do(func() {
		q.shutdownErr = q.drainAndClose(deadline, expireReason, closeSink)
		close(q.shutdownDone)
	})
	<-q.shutdownDone
	return q.shutdownErr
}

func (q *sinkQueue) drainAndClose(deadline time.Duration, expireReason DropReason, closeSink bool) error {
	// Stop accepting new events. For the unbounded backlog this also wakes
	// the drain goroutine once the list empties.
	q.mu.Lock()
	wasClosed := q.closed
	q.closed = true
	if q.capacity == 0 {
		q.cond.Broadcast()
	}
	q.mu.Unlock()

	if q.capacity > 0 {
		close(q.bounded)
	}

	timedOut := false
	if deadline > 0 {
		timer := time.NewTimer(deadline)
		select {
		case <-q.done:
			timer.Stop()
		case <-timer.C:
			timedOut = true
		}
	} else {
		<-q.done
	}

	var err error
	if timedOut {
		// Expire everything the drain goroutine has not gotten to, including
		// an event parked in a blocked write.
		remaining := q.expireBacklog(expireReason)
		if remaining > 0 {
			err = errors.Join(err, ErrBroadcasterDrainTimeout)
		}
	}

	// The underlying sink is closed only when the broadcaster itself is
	// shutting down. On Remove/eviction it must stay open so a later re-Add
	// can keep writing to it. Closing it is also what cooperatively unblocks a
	// write stuck in it; a sink whose Write blocks forever despite Close
	// cannot be unblocked at all in Go, so the leftover goroutine is
	// documented rather than waited on indefinitely.
	if closeSink && !wasClosed {
		if closeErr := q.closeUnderlyingSink(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}

	if !timedOut {
		// Happy path: make sure the drain goroutine has fully exited before
		// returning so Close never overlaps the sink's last Write.
		<-q.done
		return err
	}

	// Deadline already fired. Give a sink that honors Close a bounded chance
	// to wind its in-flight write down, then return regardless so Close stays
	// within its deadline.
	select {
	case <-q.done:
	case <-time.After(graceAfterClose):
		logrus.WithFields(logrus.Fields{
			"events.sink": q.sink,
		}).Warn("broadcaster: sink write still blocked after Close; abandoning drain goroutine")
	}
	return err
}

// closeUnderlyingSink closes the wrapped sink exactly once, regardless of how
// many shutdown paths reach it (Remove followed by broadcaster Close, repeated
// Close calls). ErrSinkClosed from an already-closed sink is treated as
// success. It is safe for concurrent use.
func (q *sinkQueue) closeUnderlyingSink() error {
	q.sinkCloseMu.Lock()
	defer q.sinkCloseMu.Unlock()
	if q.sinkClosed {
		return nil
	}
	if err := q.sink.Close(); err != nil && !errors.Is(err, ErrSinkClosed) {
		return err
	}
	q.sinkClosed = true
	return nil
}

// graceAfterClose bounds how long shutdown waits for an in-flight write to
// return after the underlying sink has been closed, when the drain deadline
// already elapsed.
const graceAfterClose = 100 * time.Millisecond

// expireBacklog discards every event still queued (and, when the drain
// deadline fired, one event stuck in an in-flight write), counting them
// against the sink's drop statistics, and unblocks the drain goroutine.
func (q *sinkQueue) expireBacklog(reason DropReason) uint64 {
	var n uint64
	if q.capacity > 0 {
		for range q.bounded {
			n++
		}
	} else {
		q.mu.Lock()
		n += uint64(q.events.Len())
		if n > 0 {
			q.events.Init()
			q.cond.Broadcast()
		}
		if q.inFlight && !q.inFlightCounted {
			n++
			q.inFlightCounted = true
		}
		q.recordDropLocked(n, reason)
		q.mu.Unlock()
		return n
	}

	q.mu.Lock()
	if q.inFlight && !q.inFlightCounted {
		n++
		q.inFlightCounted = true
	}
	q.recordDropLocked(n, reason)
	q.mu.Unlock()
	return n
}

func (q *sinkQueue) recordDrop(n uint64, reason DropReason) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.recordDropLocked(n, reason)
}

func (q *sinkQueue) recordDropLocked(n uint64, reason DropReason) {
	q.dropped += n
	q.drops[reason] += n
}

func (q *sinkQueue) backlogLenLocked() int {
	if q.capacity > 0 {
		return len(q.bounded)
	}
	return q.events.Len()
}

// stats returns a point-in-time snapshot of the sink's delivery accounting.
func (q *sinkQueue) stats(active bool) SinkStats {
	q.mu.Lock()
	defer q.mu.Unlock()

	drops := make(map[DropReason]uint64, len(q.drops))
	for reason, n := range q.drops {
		drops[reason] = n
	}

	return SinkStats{
		Sink:      q.sink,
		Delivered: q.delivered,
		Dropped:   q.dropped,
		Drops:     drops,
		Active:    active,
	}
}
