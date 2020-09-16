package tinyevents

import (
	"sync"

	"github.com/andriiyaremenko/tinyevents/internal"
	"github.com/andriiyaremenko/tinyevents/types"
)

type Event = types.Event

type EventStore interface {
	Topic() string
	TopicID() string
	Version() int64
	GetEvents() ([]Event, int64, error)
	CreateEvent(eventType string, data []byte) (*Event, error)
	CreateRecordedEvent(eventType string, data []byte, expectedVersion int64) (*Event, error)
	Close() error
}

func NewEventStoreConstructor(dbDriver, conn, table string) func(string) (EventStore, error) {
	return func(topic string) (EventStore, error) {
		db, err := internal.NewDatabase(dbDriver, conn, table)
		if err != nil {
			return nil, err
		}

		topicID, version, err := db.CreateTopicIfNotExists(topic)
		if err != nil {
			return nil, err
		}

		return &eventStore{
			topicID: topicID,
			topic:   topic,
			version: version,
			db:      db}, nil
	}
}

func NewEventStore(topic, dbDriver, conn, table string) (EventStore, error) {
	db, err := internal.NewDatabase(dbDriver, conn, table)
	if err != nil {
		return nil, err
	}

	topicID, version, err := db.CreateTopicIfNotExists(topic)
	if err != nil {
		return nil, err
	}

	return &eventStore{
		topicID: topicID,
		topic:   topic,
		version: version,
		db:      db}, nil
}

type eventStore struct {
	mu sync.Mutex

	topicID string
	topic   string
	version int64
	db      *internal.Database
}

func (es *eventStore) Topic() string {
	return es.topic
}
func (es *eventStore) TopicID() string {
	return es.topicID
}

func (es *eventStore) Version() int64 {
	return es.version
}

func (es *eventStore) CreateEvent(eventType string, data []byte) (*Event, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	event, err := es.db.CreateEvent(eventType, es.topicID, data, es.version)
	if err != nil {
		return nil, err
	}

	es.version++

	return event, nil
}

func (es *eventStore) CreateRecordedEvent(eventType string, data []byte, expectedVersion int64) (*Event, error) {
	event, err := es.db.CreateEvent(eventType, es.topicID, data, expectedVersion)
	if err != nil {
		return nil, err
	}

	es.mu.Lock()
	es.version++
	es.mu.Unlock()

	return event, nil
}

func (es *eventStore) GetEvents() ([]Event, int64, error) {
	return es.db.GetEvents(es.topicID)
}

func (es *eventStore) Close() error {
	return es.db.Close()
}
