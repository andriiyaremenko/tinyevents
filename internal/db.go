package internal

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/andriiyaremenko/tinyevents/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

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

type Database struct {
	mu    sync.Mutex
	table string
	db    *sql.DB
}

func NewDatabase(driver, connection, table string) (*Database, error) {
	db, err := sql.Open(driver, connection)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open connection to database")
	}

	command := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s_topics (id UUID PRIMARY KEY, topic STRING NOT NULL, version INT64 NOT NULL)",
		table,
	)

	if _, err := db.Exec(command); err != nil {
		return nil, errors.Wrap(err, "failed to initialize tables")
	}

	command = fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (id UUID PRIMARY KEY,
										type STRING NOT NULL,
										topicId UUID NOT NULL REFERENCES %s_topics ON DELETE RESTRICT,
										topic STRING NOT NULL,
										data STRING NOT NULL,
										version INT64 NOT NULL,
										timeStamp INT64 NOT NULL)`,
		table, table,
	)

	if _, err = db.Exec(command); err != nil {
		return nil, errors.Wrap(err, "failed to initialize tables")
	}

	return &Database{db: db, table: table}, nil
}

func (d *Database) CreateEvent(eventType, topicId string, data []byte, expectedVersion int64) (*types.Event, error) {
	timeStamp := time.Now().UTC().Unix()
	d.mu.Lock()
	defer d.mu.Unlock()

	query := fmt.Sprintf(
		"SELECT topic, version FROM %s_topics WHERE id = $1",
		d.table,
	)

	rows, err := d.db.Query(query, topicId)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read topic")
	}

	defer rows.Close()

	var topic string
	found := false

	for rows.Next() {
		var version int64

		if err := rows.Scan(&topic, &version); err != nil {
			return nil, errors.Wrap(err, "failed to read topic")
		}

		if version != expectedVersion {
			return nil, &ErrConcurrencyConflict{version, expectedVersion, eventType, topic}
		}

		found = true
	}

	if !found {
		return nil, errors.Errorf("critical: topic %s not found", topicId)
	}

	version := expectedVersion + 1
	command := fmt.Sprintf(
		"UPDATE %s_topics SET version = $1 WHERE id = $2",
		d.table,
	)

	if _, err = d.db.Exec(command, topicId, version); err != nil {
		return nil, errors.Wrap(err, "failed to update topic version")
	}

	command = fmt.Sprintf(
		`INSERT INTO %s (id, type, topicId, topic, data, version, timeStamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		d.table,
	)

	id := uuid.New().String()

	if _, err = d.db.Exec(command, id, eventType, topicId, topic, string(data), version, timeStamp); err != nil {
		return nil, errors.Wrap(err, "failed to create event")
	}

	return &types.Event{
		Id:        id,
		Type:      eventType,
		Topic:     topic,
		Data:      data,
		TimeStamp: timeStamp}, nil
}

func (d *Database) GetEvents(topic string) ([]types.Event, int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE topic = $1",
		d.table,
	)

	rows, err := d.db.Query(query, topic)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to read events")
	}

	defer rows.Close()

	var events []types.Event
	var version int64 = 0

	for rows.Next() {
		event := new(types.RecordedEvent)
		var data string
		err := rows.Scan(event.Id, event.Type, event.TopicId, event.Topic, &data, event.Version, event.TimeStamp)

		if err != nil {
			return nil, 0, errors.Wrap(err, "failed to read events")
		}

		event.Data = []byte(data)
		events = append(events, event.Event)

		if version < event.Version {
			version = event.Version
		}
	}

	return events, version, nil
}

func (d *Database) CreateTopicIfNotExists(topic string) (string, int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := fmt.Sprintf(
		"SELECT id, version FROM %s_topics WHERE topic = $1",
		d.table,
	)

	rows, err := d.db.Query(query, topic)

	if err != nil {
		return "", 0, errors.Wrap(err, "failed to read topic")
	}

	defer rows.Close()

	for rows.Next() {
		var id string
		var version int64

		if err := rows.Scan(&id, &version); err != nil {
			return "", 0, errors.Wrap(err, "failed to read topic")
		}

		return id, version, nil
	}

	command := fmt.Sprintf(
		"INSERT INTO %s_topics (id, topic, version) VALUES ($1, $2, $3)",
		d.table,
	)

	id := uuid.New().String()

	if _, err := d.db.Exec(command, id, topic, 0); err != nil {
		return "", 0, errors.Wrap(err, "failed to create new topic")
	}

	return id, 0, nil
}

func (d *Database) Close() error {
	return d.db.Close()
}
