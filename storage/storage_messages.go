package storage

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/sirupsen/logrus"
)

/* LocalStorageMessages handles all the topics logic relative to the local metadata.
 All the Input and Output keys of all methods will NOT contain the prefix (they are relative keys)

The keys are as following, without delimiters:
<topicUUID><partition><priority><msgID> => body
<32bytes>  <2bytes>  <2bytes>  <6bytes>


When calling *LocalStorage the struct prefix has to be used. In badgerDB the full key would be
t:<topicUUID><partition><priority><msgID> because this struct receives a prefix of "t:"
*/
type LocalStorageMessages struct {
	db       *badger.DB
	prefix   []byte
	logger   logrus.FieldLogger
	parent   *LocalStorage
	metadata *LocalStorageMetadata
}

// Upsert creates a write transaction for a specific topic and partition.
// If the metadata does not exists it will create them, otherwise their body will be replaced
// the Keys of KVPairs should NOT have the topic/partition prefixes, they are prepended in this method
func (m *LocalStorageMessages) Upsert(topicUUID []byte, partition uint16, batch []Message) error {
	prefix, err := m.prefixForTopicAndPart(topicUUID, partition)
	if err != nil {
		return err
	}

	kvs := make([]KVPair, len(batch))
	for i := range batch {
		if !IsMsgBodyValid(batch[i].Body) {
			return ErrMessageInvalid
		}
		kvs[i] = KVPair{
			Key: batch[i].Key(),
			Val: batch[i].Body,
		}
	}

	return m.parent.WriteBatch(prefix, kvs)
}

// Ack removes the metadata to be seen by any consumer
// The Body of the Message can be empty (Ack operation does not need it)
func (m *LocalStorageMessages) Ack(topicUUID []byte, partition uint16, batch []Message) error {
	prefix, err := m.prefixForTopicAndPart(topicUUID, partition)
	if err != nil {
		return err
	}

	kvs := make([][]byte, len(batch))
	for i := range batch {
		kvs[i] = batch[i].Key()
	}

	return m.parent.DeleteBatch(prefix, kvs)
}

// GetLowestPriority reads in a transaction the metadata with the lowest priority
// the Keys of KVPairs should NOT have the topic/partition prefixes, they are prepended in this method
func (m *LocalStorageMessages) GetLowestPriority(topicUUID []byte, partition uint16, limit int) ([]Message, error) {
	prefix, err := m.prefixForTopicAndPart(topicUUID, partition)
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

func (m *LocalStorageMessages) prefixForTopicAndPart(topicUUID []byte, partition uint16) ([]byte, error) {
	partitionAsBytes := uInt16ToBytes(partition)
	//these are concat without a delimiter! because they have fixed sizes
	prefix := concatSlices(m.prefix, topicUUID, partitionAsBytes)
	return prefix, nil
}
