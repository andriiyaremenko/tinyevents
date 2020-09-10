package tinyevents

import (
	"github.com/andriiyaremenko/tinyevents/internal"
	"github.com/andriiyaremenko/tinyevents/types"
)

type Event = types.Event

type EventStore interface {
	CreateEvent(event Event, expectedVersion int64) error
	GetEvents(aggregateType string) ([]Event, int64, error)
}

func NewEventStore(dbDriver, conn, table string, publish chan<- Event) (EventStore, error) {
	db, err := internal.NewDatabase(dbDriver, conn, table)
	if err != nil {
		return nil, err
	}

	return &eventStore{db, publish}, nil
}

type eventStore struct {
	db      *internal.Database
	publish chan<- Event
}

func (es *eventStore) CreateEvent(event Event, expectedVersion int64) error {
	if err := es.db.CreateEvent(event, expectedVersion); err != nil {
		return err
	}

	es.publish <- event
	return nil
}

func (es *eventStore) GetEvents(aggregateType string) ([]Event, int64, error) {
	return es.db.GetEvents(aggregateType)
}
