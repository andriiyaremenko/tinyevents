package tinyevents

import (
	"sync"

	"github.com/andriiyaremenko/tinyevents/internal"
	"github.com/andriiyaremenko/tinyevents/types"
	"github.com/pkg/errors"
)

type Event = types.Event

type EventStore interface {
	Topic() string
	TopicID() string
	Version() (int64, error)
	GetEvents() ([]Event, int64, error)
	GetEventsFrom(int64) ([]Event, int64, error)
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

type eventStore struct {
	mu sync.Mutex

	topicID string
	topic   string
	db      *internal.Database
}

func (es *eventStore) Topic() string {
	return es.topic
}
func (es *eventStore) TopicID() string {
	return es.topicID
}

func (es *eventStore) Version() (int64, error) {
	v, err := es.db.Version(es.topicID)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read EventStore %s version", es.topic)
	}

	return v, nil
}

func (es *eventStore) CreateEvent(eventType string, data []byte) (*Event, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	version, err := es.Version()
	if err != nil {
		return nil, err
	}

	return es.CreateRecordedEvent(eventType, data, version)
}

func (es *eventStore) CreateRecordedEvent(eventType string, data []byte, expectedVersion int64) (*Event, error) {
	event, err := es.db.CreateEvent(eventType, es.topicID, data, expectedVersion)
	if err != nil {
		return nil, errors.Wrapf(err, "EventStore %s: failed to create event %d", expectedVersion, es.topic)
	}

	return event, nil
}

func (es *eventStore) GetEvents() ([]Event, int64, error) {
	return es.GetEventsFrom(0)
}

func (es *eventStore) GetEventsFrom(version int64) ([]Event, int64, error) {
	events, v, err := es.db.GetEvents(es.topicID, version)
	if err != nil {
		return nil, 0, errors.Wrapf(err,
			"EventStore %s: failed to read events starting from version %d", es.topic, version)
	}

	return events, v, nil
}

func (es *eventStore) Close() error {
	return es.db.Close()
}
