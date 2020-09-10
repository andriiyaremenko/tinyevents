package types

import (
	_ "encoding/json"
)

type Event struct {
	Id            string `json:"id,omitempty"`
	Type          string `json:"type,omitempty"`
	AggregateType string `json:"aggregateId,omitempty"`
	AggregateId   string `json:"aggregateType,omitempty"`
	Data          []byte `json:"data"`
	Channel       string `json:"channel,omitempty"`
}

type RecordedEvent struct {
	Event
	Version   int64 `json:"version,omitempty"`
	TimeStamp int64 `json:"timeStamp"`
}
