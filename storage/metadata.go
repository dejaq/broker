package storage

import (
	"time"
)

// ConsumerMetadata wraps the info we have on a consumer
// Is is stored in the metadata table with the topicUUID and ID as the key
// The rest of its properties are stored in the body
type ConsumerMetadata struct {
	TopicHash  uint32    `json:"-"` //it is in the key
	ConsumerID string    `json:"-"` //it is in the key
	LastSeen   time.Time `json:"s"`
}

func (cm ConsumerMetadata) ConsumerIDBytes() []byte {
	//TODO avoid memory allocs
	return []byte(cm.ConsumerID)
}

// ConsumerPartitions is a container for all Partitions owned by a ConsumerID
type ConsumerPartitions struct {
	TopicHash  uint32   //it is in the key
	ConsumerID string   //is in the key
	Partitions []uint16 //is the body
}

func (cm ConsumerPartitions) ConsumerIDBytes() []byte {
	//TODO avoid memory allocs
	return []byte(cm.ConsumerID)
}

// TopicMetadata holds the basic info for a topic
type TopicMetadata struct {
	TopicID         string `json:"-"` //it is already in the Key
	TopicHash       uint32 `json:"u"`
	PartitionsCount int    `json:"p"`
}
