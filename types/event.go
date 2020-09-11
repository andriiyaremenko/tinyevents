package types

import (
	_ "encoding/json"
)

type Event struct {
	Id        string `json:"id,omitempty"`
	Type      string `json:"type,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Channel   string `json:"channel,omitempty"`
	Data      []byte `json:"data"`
	TimeStamp int64  `json:"timeStamp"`
}

type RecordedEvent struct {
	Event
	TopicId string `json:"topicId,omitempty"`
	Version int64  `json:"version,omitempty"`
}
