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

type Database struct {
	rwM   sync.RWMutex
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
		return nil, errors.Wrapf(err, "failed to initialize %s_topics table", table)
	}

	command = fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (id UUID PRIMARY KEY,
										type STRING NOT NULL,
										topicID UUID NOT NULL REFERENCES %s_topics ON DELETE RESTRICT,
										topic STRING NOT NULL,
										data STRING NOT NULL,
										version INT64 NOT NULL,
										timeStamp INT64 NOT NULL)`,
		table, table,
	)

	if _, err = db.Exec(command); err != nil {
		return nil, errors.Wrapf(err, "failed to initialize %s table", table)
	}

	return &Database{db: db, table: table}, nil
}

func (d *Database) Version(topicID string) (int64, error) {
	d.rwM.RLock()
	defer d.rwM.RUnlock()

	query := fmt.Sprintf(
		"SELECT version FROM %s_topics WHERE id = $1",
		d.table,
	)

	rows, err := d.db.Query(query, topicID)
	if err != nil {
		return 0, errors.Wrap(err, "failed to query topic")
	}

	defer rows.Close()

	for rows.Next() {
		var version int64

		if err := rows.Scan(&version); err != nil {
			return 0, errors.Wrap(err, "failed to read topic")
		}

		return version, nil
	}

	return 0, nil
}

func (d *Database) CreateEvent(eventType, topicID string, data []byte, expectedVersion int64) (*types.Event, error) {
	timeStamp := time.Now().UTC().Unix()
	d.rwM.Lock()
	defer d.rwM.Unlock()

	query := fmt.Sprintf(
		"SELECT topic, version FROM %s_topics WHERE id = $1",
		d.table,
	)

	rows, err := d.db.Query(query, topicID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query topic")
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
			return nil, &types.ErrConcurrencyConflict{
				Version:         version,
				ExpectedVersion: expectedVersion,
				EventType:       eventType,
				EventTopic:      topic}
		}

		found = true
	}

	if !found {
		return nil, errors.Errorf("critical: topic %s not found", topicID)
	}

	version := expectedVersion + 1
	command := fmt.Sprintf(
		"UPDATE %s_topics SET version = $2 WHERE id = $1",
		d.table,
	)

	if _, err = d.db.Exec(command, topicID, version); err != nil {
		return nil, errors.Wrap(err, "failed to update topic version")
	}

	command = fmt.Sprintf(
		`INSERT INTO %s (id, type, topicID, topic, data, version, timeStamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		d.table,
	)

	id := uuid.New().String()

	if _, err = d.db.Exec(command, id, eventType, topicID, topic, string(data), version, timeStamp); err != nil {
		return nil, errors.Wrap(err, "failed to create event")
	}

	return &types.Event{
		ID:        id,
		Type:      eventType,
		Topic:     topic,
		Data:      data,
		TimeStamp: timeStamp}, nil
}

func (d *Database) GetEvents(topicID string, eventType string, from int64) ([]types.Event, int64, error) {
	d.rwM.RLock()
	defer d.rwM.RUnlock()

	v, err := d.Version(topicID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to query events")
	}

	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE topicID = $1 and type = $2 and version >= $3 ORDER BY version",
		d.table,
	)

	rows, err := d.db.Query(query, topicID, eventType, from)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to query events")
	}

	defer rows.Close()

	events, err := d.getEvents(rows)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to query events")
	}

	return events, v, nil
}

func (d *Database) GetAllEvents(topicID string, from int64) ([]types.Event, int64, error) {
	d.rwM.RLock()
	defer d.rwM.RUnlock()

	v, err := d.Version(topicID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to query events")
	}

	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE topicID = $1 and version >= $2 ORDER BY version",
		d.table,
	)

	rows, err := d.db.Query(query, topicID, from)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to query events")
	}

	defer rows.Close()

	events, err := d.getEvents(rows)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to query events")
	}

	return events, v, nil
}

func (d *Database) CreateTopicIfNotExists(topic string) (string, int64, error) {
	d.rwM.Lock()
	defer d.rwM.Unlock()

	query := fmt.Sprintf(
		"SELECT id, version FROM %s_topics WHERE topic = $1",
		d.table,
	)

	rows, err := d.db.Query(query, topic)

	if err != nil {
		return "", 0, errors.Wrap(err, "failed to query topic")
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

func (d *Database) getEvents(rows *sql.Rows) ([]types.Event, error) {
	var events []types.Event
	for rows.Next() {
		event := new(types.RecordedEvent)
		var data string
		err := rows.Scan(&event.ID, &event.Type, &event.TopicID, &event.Topic, &data, &event.Version, &event.TimeStamp)

		if err != nil {
			return nil, errors.Wrap(err, "failed to read events")
		}

		event.Data = []byte(data)
		events = append(events, event.Event)
	}

	return events, nil
}
