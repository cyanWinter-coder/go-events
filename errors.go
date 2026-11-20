package events

import "fmt"

// ErrSinkClosed is returned if a write is issued to a sink that has been
// closed. If encountered, the error should be considered terminal and
// retries will not be successful.
var ErrSinkClosed = fmt.Errorf("events: sink closed")

// ErrBroadcasterDrainTimeout is returned by Broadcaster.Close when one or
// more sink backlogs could not be drained within the configured deadline.
// Events left in those backlogs are expired and counted as
// DropDrainTimeout; it is joined with any errors returned by closing the
// underlying sinks.
var ErrBroadcasterDrainTimeout = fmt.Errorf("events: broadcaster drain timeout")
