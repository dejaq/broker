package storage

import (
	"encoding/json"
	"fmt"
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

// asKV is an internal helper to convert into a stored KV with prefix
func (cm ConsumerMetadata) asKV() (KVPair, error) {
	body, err := json.Marshal(cm)
	if err != nil {
		return KVPair{}, err
	}
	keyStr := fmt.Sprintf("topics:cm:%s:%s", cm.TopicUUID, cm.ConsumerID)
	return KVPair{
		//todo avoid memory allocs by using unsafe cast
		Key: []byte(keyStr),
		Val: body,
	}, nil
}

// ConsumerPartitions is a container for all Partitions owned by a ConsumerID
type ConsumerPartitions struct {
	ConsumerID string
	Partitions []uint16
}

// TopicMetadata holds the basic info for a topic
type TopicMetadata struct {
	TopicID         string `json:""` //it is already in the Key
	TopicUUID       []byte `json:"u"`
	PartitionsCount int    `json:"p"`
}
