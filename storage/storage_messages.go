package storage

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v2"

	"github.com/sirupsen/logrus"
)

const (
	// length in bytes of the partition
	partitionSize = 2
	// length in bytes for the priority
	prioritySize = 2
	// msg id size in bytes
	msgIDSize = 6
)

/* LocalStorageMessages handles all the topics logic relative to the local messages.
 All the Input and Output keys of all methods will NOT contain the prefix (they are relative keys)

The keys are as following, without delimiters:
<topicHash><partition><priority><msgID> => body
<4bytes>  <2bytes>  <2bytes>  <6bytes>


When calling *LocalStorage the struct prefix has to be used. In badgerDB the full key would be
t:<topicHash><partition><priority><msgID> because this struct receives a prefix of "t:"
*/
type LocalStorageMessages struct {
	prefix []byte
	logger logrus.FieldLogger
	parent *LocalStorage
}

// Upsert creates a write transaction for a specific topic and partition.
// If the messages does not exists it will create them, otherwise their body will be replaced
// the Keys of KVPairs should NOT have the topic/partition prefixes, they are prepended in this method
func (m *LocalStorageMessages) Upsert(topicHash uint32, partition uint16, batch []Message) error {
	kvs := make([]KVPair, len(batch))
	for i := range batch {
		if !IsMsgBodyValid(batch[i].Body) {
			return ErrMessageInvalid
		}
		kvs[i] = KVPair{
			Key: m.keyForMessage(topicHash, partition, batch[i]),
			Val: batch[i].Body,
		}
	}

	return m.parent.WriteBatch(nil, kvs)
}

// Ack removes the messages to be seen by any consumer
// The Body of the Message can be empty (Ack operation does not need it)
func (m *LocalStorageMessages) Ack(topicHash uint32, partition uint16, batch []Message) error {
	kvs := make([][]byte, len(batch))
	for i := range batch {
		kvs[i] = m.keyForMessage(topicHash, partition, batch[i])
	}

	return m.parent.DeleteBatch(nil, kvs)
}

// GetLowestPriority reads in a transaction the messages with the lowest priority
// the Keys of KVPairs should NOT have the topic/partition prefixes, they are prepended in this method
func (m *LocalStorageMessages) GetLowestPriority(topicHash uint32, partition uint16, limit int) ([]Message, error) {
	prefix := m.prefixForTopicAndPart(topicHash, partition)

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

// GetLowestPriorities checks which partition has the
// message with the lowest priority.
// It avoids accessing the Disk by leveraging the Key Only Badger iteration
func (m *LocalStorageMessages) GetPartitionWithLowestPriority(topicHash uint32, partitions []uint16) (uint16, error) {
	var lowestPartition uint16
	var lowestPriority uint16
	var previousPartition uint16
	var atLeastOne bool

	//for fast query of the partitions list
	dict := make(map[uint16]struct{}, len(partitions))
	for i := range partitions {
		dict[partitions[i]] = struct{}{}
	}

	//this can also be achieved using Seek()s

	err := m.parent.db.View(func(txn *badger.Txn) error {
		prefix := m.prefixForTopicAndPart(topicHash, 0)
		prefix = prefix[:len(prefix)-partitionSize]

		it := txn.NewIterator(badger.IteratorOptions{
			//very important to do a KEY ONLY iteration
			//this way we only traverse the inmemory LSM tree
			// all the info we need can be found in the KEYs
			PrefetchValues: false,
			Reverse:        false,
			AllVersions:    false,
			Prefix:         prefix,
			InternalAccess: false,
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			//we only care for the first message of each partition
			//but for only a subset of partitions
			//first 2 bytes of the key is the partition
			partition := binary.BigEndian.Uint16(it.Item().Key())
			if atLeastOne && partition == previousPartition {
				//we rely on the fact that partitions are sorted
				// and we traverse ASC
				//so we only need th first message from each partition
				continue
			}
			if _, weAreInterested := dict[partition]; !weAreInterested {
				continue
			}
			previousPartition = partition
			atLeastOne = true

			priority := binary.BigEndian.Uint16(it.Item().Key()[partitionSize:])
			if priority >= lowestPriority {
				continue
			}
			lowestPriority = priority
			lowestPartition = partition
		}
		return nil
	})
	return lowestPartition, err
}

func (m *LocalStorageMessages) prefixForTopicAndPart(topicHash uint32, partition uint16) []byte {
	prefix := make([]byte, len(m.prefix)+topicHashSize+partitionSize)
	offset := 0
	copy(prefix[offset:], m.prefix)
	offset += len(m.prefix)
	binary.BigEndian.PutUint32(prefix[offset:], topicHash)
	offset += topicHashSize
	binary.BigEndian.PutUint16(prefix[offset:], partition)
	return prefix
}

func (m *LocalStorageMessages) keyForMessage(topicHash uint32, partition uint16, msg Message) []byte {
	return append(m.prefixForTopicAndPart(topicHash, partition), msg.Key()...)
}
