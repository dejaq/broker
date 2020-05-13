package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v2"
	"github.com/sirupsen/logrus"
)

// LocalStorageMessages handles all the topics logic relative to the local messages.
// All the Input and Output keys of all methods will NOT contain the prefix (they are relative keys)
type LocalStorageMessages struct {
	db       *badger.DB
	prefix   []byte
	logger   logrus.FieldLogger
	parent   *LocalStorage
	metadata *LocalStorageMetadata
}

// UpsertMessages creates a write transaction for a specific topic and partition.
// the Keys of KVPairs should NOT have the topic/partition prefixes, they are prepended in this method
func (m *LocalStorageMessages) UpsertMessages(topic string, partition uint16, batch []Message) error {
	prefix, err := m.prefixForTopicAndPart(topic, partition)
	if err != nil {
		return err
	}

	kvs := make([]KVPair, len(batch))
	for i := range batch {
		kvs[i] = KVPair{
			Key: batch[i].Key(),
			Val: batch[i].Body,
		}
	}
	return m.parent.WriteBatch(prefix, kvs)
}

// LowestPriorityMsgs reads in a transaction the messages with the lowest priority
// the Keys of KVPairs should NOT have the topic/partition prefixes, they are prepended in this method
func (m *LocalStorageMessages) LowestPriorityMsgs(topic string, partition uint16, limit int) ([]Message, error) {
	prefix, err := m.prefixForTopicAndPart(topic, partition)
	if err != nil {
		return nil, err
	}

	msgs, err := m.parent.ReadFirstsKVPairs(prefix, limit)
	result := make([]Message, len(msgs))
	var terr error
	for i := range msgs {
		result[i], terr = NewMessageFromKV(msgs[i])
		if terr != nil {
			m.logger.WithError(terr).Error("found malformed message")
		}
	}
	return result, err
}

func (m *LocalStorageMessages) prefixForTopicAndPart(topic string, partition uint16) ([]byte, error) {
	//these are concat without a delimiter! because they have fixed sizes
	topicUUID, err := m.metadata.GetTopicUUID(topic)
	if err != nil {
		return nil, fmt.Errorf("cannot retrive topics UUID err: %w", err)
	}
	partitionAsBytes := UInt16ToBytes(partition)
	prefix := concatSlices(m.prefix, topicUUID, partitionAsBytes)
	return prefix, nil
}
