package tinyevents

import (
	"github.com/andriiyaremenko/tinyevents/internal"
	"github.com/andriiyaremenko/tinyevents/types"
)

type Event = types.Event
type EventType string

type EventStore interface {
	Topic() string
	TopicID() string
	Version() (int64, error)
	GetEvents(eventType EventType) ([]Event, int64, error)
	GetEventsFrom(evenType EventType, version int64) ([]Event, int64, error)
	GetAllEvents() ([]Event, int64, error)
	GetAllEventsFrom(version int64) ([]Event, int64, error)
	CreateEvent(eventType EventType, data []byte) (*Event, error)
	CreateRecordedEvent(eventType EventType, data []byte, expectedVersion int64) (*Event, error)
	Close() error
}

func NewEventStoreConstructor(dbDriver, conn, table string) func(string) (EventStore, error) {
	return func(topic string) (EventStore, error) {
		db, err := internal.NewDatabase(dbDriver, conn, table)
		if err != nil {
			return nil, err
		}

		topicID, _, err := db.CreateTopicIfNotExists(topic)
		if err != nil {
			return nil, err
		}

		return &eventStore{
			topicID: topicID,
			topic:   topic,
			db:      db}, nil
	}
}

func NewEventStore(topic, dbDriver, conn, table string) (EventStore, error) {
	db, err := internal.NewDatabase(dbDriver, conn, table)
	if err != nil {
		return nil, err
	}

	topicID, _, err := db.CreateTopicIfNotExists(topic)
	if err != nil {
		return nil, err
	}

	return &eventStore{
		topicID: topicID,
		topic:   topic,
		db:      db}, nil
}
