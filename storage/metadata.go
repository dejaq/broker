package storage

import (
	"time"
)

// ConsumerMetadata wraps the info we have on a consumer
// Is is stored in the metadata table with the topicUUID and ID as the key
// The rest of its properties are stored in the body
type ConsumerMetadata struct {
	TopicUUID  string    `json:"-"` //it is in the key
	ConsumerID string    `json:"-"` //it is in the key
	LastSeen   time.Time `json:"s"`
}

// ConsumerPartitions is a container for all Partitions owned by a ConsumerID
type ConsumerPartitions struct {
	ConsumerID string
	Partitions []uint16
}

// TopicMetadata holds the basic info for a topic
type TopicMetadata struct {
	TopicID         string `json:"-"` //it is already in the Key
	TopicUUID       []byte `json:"u"`
	PartitionsCount int    `json:"p"`
}
