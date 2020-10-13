package types

import (
	_ "encoding/json"
)

type Event struct {
	ID        string `json:"id,omitempty"`
	Type      string `json:"type,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Data      []byte `json:"data"`
	TimeStamp int64  `json:"timeStamp"`
}

type RecordedEvent struct {
	Event
	TopicID string `json:"topicId,omitempty"`
	Version int64  `json:"version,omitempty"`
}
