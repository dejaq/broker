package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v2"
	"github.com/sirupsen/logrus"
)

// LocalStorageMessages handles all the topics logic relative to the local storage.
// All the Input and Output keys of all methods will NOT contain the prefix (they are relative keys)
type LocalStorageMessages struct {
	db       *badger.DB
	prefix   []byte
	logger   logrus.FieldLogger
	parent   *LocalStorage
	metadata *LocalStorageMetadata
}

// WriteBatch creates a write transaction for a specific topic and partition.
// the Keys of KVPairs should NOT have the topic/partition prefixes, they are prepended in this method
func (m *LocalStorageMessages) WriteBatch(topic string, partition uint16, batch []KVPair) error {
	//these are concat without a delimiter! because they have fixed sizes
	topicUUID, err := m.metadata.GetTopicUUID(topic)
	if err != nil {
		return fmt.Errorf("cannot retrive topics UUID err:%w", err)
	}
	partitionAsBytes := UInt16ToBytes(partition)
	prefix := concatSlices(m.prefix, topicUUID, partitionAsBytes)
	return m.parent.WriteBatch(prefix, batch)
}
