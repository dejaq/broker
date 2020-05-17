package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/twmb/murmur3"

	"github.com/dgraph-io/badger/v2"
	"github.com/sirupsen/logrus"
)

const (
	//length in bytes of topicHash
	topicHashSize = 4
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")

	prefixTopicsMetaKeyByID = []byte("topics:mt:") //nolint:gochecknoglobals //reusable prefixes
	prefixConsMetaByUUID    = []byte("topics:cm:") //nolint:gochecknoglobals //reusable prefixes
	prefixConsPartsByUUID   = []byte("topics:cp:") //nolint:gochecknoglobals //reusable prefixes
)

/* LocalStorageMetadata handles all the cluster metadata that is stored on this node.
 All the Input and Output keys of all methods will NOT contain the prefix (they are relative keys)

The data is kept under these format/prefixes (Key => Value)

topics:cm:<topicHash>:<consumerID> => metadata like lastseen
topics:cp:<topicHash>:<consumerID> => the assigned partitions
topics:mt:<topicID> => topics metadata like the topicHash and no of partitions

topicHash is 4bytes (murmur3 32bit of the topicID)
For limits of consumerID and topicID see limits.go


When calling *LocalStorage the struct prefix has to be used. In badgerDB the full key would be
a:topics:cm:<topicHash>:<consumerID> because this struct receives a prefix of "a:"
*/
type LocalStorageMetadata struct {
	db *badger.DB
	// prefix is used for any data handled by this struct
	// acts like a namespace for metadata in badger
	prefix []byte
	logger logrus.FieldLogger
	parent *LocalStorage
}

// CreateTopic appends an immutable Topic.
// It will fail if the topic already exists
func (t *LocalStorageMetadata) CreateTopic(topicID string, partitionsCount int) (TopicMetadata, error) {
	if !IsTopicIDValid(topicID) {
		return TopicMetadata{}, ErrTopicIDInvalid
	}
	if !IsTopicPartitionsCountValid(partitionsCount) {
		return TopicMetadata{}, ErrTopicPartitionsInvalid
	}

	mt := TopicMetadata{
		TopicID:         topicID,
		TopicHash:       murmur3.Sum32([]byte(topicID)),
		PartitionsCount: partitionsCount,
	}
	body, err := json.Marshal(mt)
	if err != nil {
		return TopicMetadata{}, err
	}
	prefix := concatSlices(t.prefix, prefixTopicsMetaKeyByID)
	return mt, t.parent.Insert(prefix, KVPair{
		Key: []byte(topicID),
		Val: body,
	})
}

// TopicMetadata returns the topic info, an error or ErrNotFound
func (t *LocalStorageMetadata) TopicMetadata(topicID string) (TopicMetadata, error) {
	//todo avoid memory allocs use unsafe for topicID
	prefix := concatSlices(t.prefix, prefixTopicsMetaKeyByID, []byte(topicID))
	topicsAsKV, err := t.parent.ReadPaginate(prefix, 1, 0)
	if err != nil {
		return TopicMetadata{}, err
	}
	if len(topicsAsKV) == 0 {
		return TopicMetadata{}, ErrNotFound
	}

	result := TopicMetadata{
		TopicID: topicID,
	}
	err = json.Unmarshal(topicsAsKV[0].Val, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

// Topics returns all the topics. Error can signal that the request failed or that
// at least one topic is malformed
func (t *LocalStorageMetadata) Topics() ([]TopicMetadata, error) {
	prefix := concatSlices(t.prefix, prefixTopicsMetaKeyByID)
	kvs, err := t.parent.ReadPaginate(prefix, MaxNumberOfTopics, 0)
	if err != nil {
		return nil, err
	}

	var rerr error
	result := make([]TopicMetadata, len(kvs))
	for i := range kvs {
		mt := TopicMetadata{
			TopicID: string(kvs[i].Key),
		}
		err = json.Unmarshal(kvs[i].Val, &mt)
		if err != nil {
			t.logger.WithError(err).Errorf("topic %s has malformed data", mt.TopicID)
			rerr = errors.New("at least one topic has malformed data")
			continue
		}
		result[i] = mt
	}

	return result, rerr
}

// UpsertConsumersMetadata can be used to update a consumer's presence.
// If it does not exists it will insert it.
// The batch is written as a transaction.
// It does NOT alter any input property like LastSeen
func (t *LocalStorageMetadata) UpsertConsumersMetadata(cons []ConsumerMetadata) error {
	kvs := make([]KVPair, len(cons))
	for i := range cons {
		if !IsConsumerIDValid(cons[i].ConsumerID) {
			return ErrConsumerInvalid
		}

		body, err := json.Marshal(cons[i])
		if err != nil {
			return err
		}
		kvs[i] = KVPair{
			Key: t.keyForConsumerMetadata(cons[i]),
			Val: body,
		}
	}

	return t.parent.WriteBatch(nil, kvs)
}

// RemoveConsumers deletes all the metadata entries of a consumerID
// in the context of a topic. It uses a single transaction.
func (t *LocalStorageMetadata) RemoveConsumers(cons []ConsumerMetadata) error {
	kvs := make([][]byte, 0, len(cons)*2)

	for i := range cons {
		//remove: topics:cm<topicHash>:<consumerID> => metadata like lastseen
		kvs = append(kvs, t.keyForConsumerMetadata(cons[i]))

		//remove: topics:cp:<topicHash>:<consumerID> => the assigned partitions
		kvs = append(kvs, t.keyForConsumerPartitions(ConsumerPartitions{
			TopicHash:  cons[i].TopicHash,
			ConsumerID: cons[i].ConsumerID,
		}))
	}

	return t.parent.DeleteBatch(nil, kvs)
}

// ConsumersMetadata can retrieve ALL the consumers
func (t *LocalStorageMetadata) ConsumersMetadata(topicHash uint32) ([]ConsumerMetadata, error) {
	//topics:cm:<topicHash>:<consumerID> => metadata like lastseen

	//since we do not have a pagination system just get them all 1M hard limit for now
	kvs, err := t.parent.ReadPaginate(t.prefixConsumerData(prefixConsMetaByUUID, topicHash), MaxConsumersPerTopic, 0)
	if err != nil {
		return nil, err
	}
	result := make([]ConsumerMetadata, len(kvs))
	for i := range kvs {
		cm := ConsumerMetadata{
			TopicHash:  topicHash,
			ConsumerID: string(kvs[i].Key), //the prefix is already removed by ReadPaginate
		}

		//this will populate the rest of the values like LastSeen
		err := json.Unmarshal(kvs[i].Val, &cm)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshall: %w", err)
		}
		result[i] = cm
	}

	return result, nil
}

// ConsumerPartitions fetch all the consumers for this topic.
// It may return consumers with no partitions assigned.
// Consumers may have Metadata but still missing from this response.
// To get all the consumers use ConsumersMetadata
func (t *LocalStorageMetadata) ConsumerPartitions(topicHash uint32) ([]ConsumerPartitions, error) {
	kvs, err := t.parent.ReadPaginate(t.prefixConsumerData(prefixConsPartsByUUID, topicHash), MaxConsumersPerTopic, 0)
	if err != nil {
		return nil, err
	}

	result := make([]ConsumerPartitions, len(kvs))
	for i := range kvs {
		result[i] = ConsumerPartitions{
			TopicHash: topicHash,
			//the prefix is removed by the parent
			ConsumerID: string(kvs[i].Key),
			//each partition has 2bytes
			Partitions: bytesToSliceUInt16(kvs[i].Val),
		}
	}
	return result, nil
}

// UpsertConsumerPartitions replaces assigned partitions to specific consumers
// To remove a consumer completely use RemoveConsumers
func (t *LocalStorageMetadata) UpsertConsumerPartitions(cons []ConsumerPartitions) error {
	kvs := make([]KVPair, len(cons))
	for i := range cons {
		kvs[i] = KVPair{
			Key: t.keyForConsumerPartitions(cons[i]),
			Val: sliceUInt16ToBytes(cons[i].Partitions),
		}
	}

	return t.parent.WriteBatch(nil, kvs)
}

func (t *LocalStorageMetadata) prefixConsumerData(dataPrefix []byte, topicHash uint32) []byte {
	keyPrefix := make([]byte, len(t.prefix)+len(dataPrefix)+topicHashSize+1)
	copy(keyPrefix, t.prefix)
	offset := len(t.prefix)
	copy(keyPrefix[offset:], dataPrefix)
	offset += len(dataPrefix)
	binary.BigEndian.PutUint32(keyPrefix[offset:], topicHash)
	offset += topicHashSize
	keyPrefix[offset] = ':'
	return keyPrefix
}

func (t *LocalStorageMetadata) keyForConsumerMetadata(cm ConsumerMetadata) []byte {
	//TODO avoid 2x memory allocs by doing the work of append ourselves
	return append(
		t.prefixConsumerData(prefixConsMetaByUUID, cm.TopicHash),
		cm.ConsumerIDBytes()...)
}

func (t *LocalStorageMetadata) keyForConsumerPartitions(cm ConsumerPartitions) []byte {
	//TODO avoid 2x memory allocs by doing the work of append ourselves
	return append(
		t.prefixConsumerData(prefixConsPartsByUUID, cm.TopicHash),
		cm.ConsumerIDBytes()...)
}
