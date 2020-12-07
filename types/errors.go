package types

import "fmt"

type ErrConcurrencyConflict struct {
	Version         int64
	ExpectedVersion int64
	EventType       string
	EventTopic      string
}

func (err *ErrConcurrencyConflict) Error() string {
	return fmt.Sprintf("met concurrency conflict: event %s topic %s version %d, expected version %d",
		err.EventType, err.EventTopic, err.Version, err.ExpectedVersion)
}
