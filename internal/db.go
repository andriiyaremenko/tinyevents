package internal

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/andriiyaremenko/tinyevents/types"
	"github.com/pkg/errors"
)

type ErrConcurrencyConflict struct {
	Version         int64
	ExpectedVersion int64
	EventType       string
	AggregateType   string
}

func (err *ErrConcurrencyConflict) Error() string {
	return fmt.Sprintf("met concurrency conflict: event %s aggregate %s version %d, expected version %d",
		err.EventType, err.AggregateType, err.Version, err.ExpectedVersion)
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
		`CREATE TABLE IF NOT EXISTS %s (id STRING PRIMARY KEY,
										type STRING NOT NULL,
										aggregateId STRING NOT NULL,
										aggregateType STRING NOT NULL,
										data STRING NOT NULL,
										channel STRING NOT NULL,
										version INT64 NOT NULL,
										timeStamp INT64 NOT NULL)`,
		table,
	)

	if _, err = db.Exec(command); err != nil {
		return nil, errors.Wrap(err, "failed to initialize tables")
	}

	command = fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s_versions (aggregateType STRING PRIMARY KEY, version INT64 NOT NULL)`,
		table,
	)

	if _, err = db.Exec(command); err != nil {
		return nil, errors.Wrap(err, "failed to initialize tables")
	}

	return &Database{db: db, table: table}, nil
}

func (d *Database) CreateEvent(event types.Event, expectedVersion int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := fmt.Sprintf(
		`SELECT version FROM %s_versions WHERE aggregateType = %s`,
		d.table, event.AggregateType,
	)

	rows, err := d.db.Query(query)
	if err != nil {
		return errors.Wrap(err, "failed to read events versions")
	}

	defer rows.Close()

	for rows.Next() {
		var version int64

		if err := rows.Scan(&version); err != nil {
			return errors.Wrap(err, "failed to read events versions")
		}

		if version != expectedVersion {
			return &ErrConcurrencyConflict{version, expectedVersion, event.Type, event.AggregateType}
		}
	}

	command := fmt.Sprintf(
		`UPDATE %s_versions SET version = $1 WHERE aggregateType = $2`,
		d.table,
	)

	_, err = d.db.Exec(command, event.AggregateType, expectedVersion+1)

	command = fmt.Sprintf(
		`INSERT INTO %s (id, type, aggregateId, aggregateType, data, channel, version, timeStamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
		d.table,
	)

	_, err = d.db.Exec(command,
		event.Id, event.Type, event.AggregateId, event.AggregateType, string(event.Data),
		expectedVersion+1, time.Now().UTC().Unix())

	if err != nil {
		return errors.Wrap(err, "failed to create event")
	}

	return nil
}

func (d *Database) GetEvents(aggregateType string) ([]types.Event, int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE aggregateType = %s`,
		d.table, aggregateType,
	)

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to read events")
	}

	defer rows.Close()

	var events []types.Event
	var version int64 = 0

	for rows.Next() {
		event := new(types.RecordedEvent)
		var data string
		err := rows.Scan(event.Id, event.Type, event.AggregateId, event.AggregateType, &data, event.Version, event.TimeStamp)

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
