package storage

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v2"
	"github.com/sirupsen/logrus"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")

	//nolint:gochecknoglobals //reusable prefixes
	prefixTopicsMetaKeyByID = []byte("topics:mt:")
	//nolint:gochecknoglobals //reusable prefixes
	prefixConsMetaByUUID = []byte("topics:cm:")
	//nolint:gochecknoglobals //reusable prefixes
	prefixConsPartsByUUID = []byte("topics:cp:")
)

/* LocalStorageMetadata handles all the cluster metadata that is stored on this node.
 All the Input and Output keys of all methods will NOT contain the prefix (they are relative keys)

The data is kept under these format/prefixes (Key => Value)

topics:cm:<topicUUID>:<consumerID> => metadata like lastseen
topics:cp:<topicUUID>:<consumerID> => the assigned partitions
topics:mt:<topicID> => topics metadata like the UUID and no of partitions

TopicUUID is 16bytes
For limits of consumerID and topicID see limits.go


When calling *LocalStorage the struct prefix has to be used. In badgerDB the full key would be
a:topics:cm:<topicUUID>:<consumerID> because this struct receives a prefix of "a:"
*/
type LocalStorageMetadata struct {
	db *badger.DB
	// prefix is used for any data handled by this struct
	// acts like a namespace for metadata in badger
	prefix []byte
	logger logrus.FieldLogger
	parent *LocalStorage
}

// CreateTopic appends an immutable Topic
func (t *LocalStorageMetadata) CreateTopic(topicID string, partitionsCount int) (TopicMetadata, error) {
	if !IsTopicIDValid(topicID) {
		return TopicMetadata{}, ErrTopicIDInvalid
	}
	if !IsTopicPartitionsCountValid(partitionsCount) {
		return TopicMetadata{}, ErrTopicPartitionsInvalid
	}

	mt := TopicMetadata{
		TopicID:         topicID,
		TopicUUID:       newUUID(),
		PartitionsCount: partitionsCount,
	}
	key := concatSlices(prefixTopicsMetaKeyByID, []byte(topicID))
	body, err := json.Marshal(mt)
	if err != nil {
		return TopicMetadata{}, err
	}

	return mt, t.parent.Insert(t.prefix, KVPair{
		Key: key,
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
	kvs, err := t.parent.ReadPaginate(t.prefix, MaxNumberOfTopics, 0)
	if err != nil {
		return nil, err
	}
	var rerr error
	result := make([]TopicMetadata, len(kvs))
	for i := range kvs {
		id := string(kvs[i].Key)
		mt := TopicMetadata{
			TopicID: id,
		}
		err = json.Unmarshal(kvs[i].Val, &mt)
		if err != nil {
			t.logger.WithError(err).Errorf("topic %s has malformed data", id)
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
	var err error

	for i := range cons {
		if !IsConsumerIDValid(cons[i].ConsumerID) {
			return ErrConsumerInvalid
		}
		kvs[i], err = consMetaToKVPair(cons[i])
		if err != nil {
			return fmt.Errorf("failed transforming to KV: %w", err)
		}
	}

	return t.parent.WriteBatch(t.prefix, kvs)
}

// RemoveConsumers deletes all the metadata entries of a consumerID
// in the context of a topic. It uses a single transaction.
func (t *LocalStorageMetadata) RemoveConsumers(cons []ConsumerMetadata) error {
	kvs := make([][]byte, 0, len(cons)*2)

	for i := range cons {
		partsPrefixTopic := []byte(cons[i].TopicUUID + ":" + cons[i].ConsumerID)

		//remove: topics:cm<topicUUID>:<consumerID> => metadata like lastseen
		kvs = append(kvs, concatSlices(prefixConsMetaByUUID, partsPrefixTopic))

		//remove: topics:cp:<topicUUID>:<consumerID> => the assigned partitions
		kvs = append(kvs, concatSlices(prefixConsPartsByUUID, partsPrefixTopic))
	}

	return t.parent.DeleteBatch(t.prefix, kvs)
}

// ConsumersMetadata can retrieve ALL the consumers
func (t *LocalStorageMetadata) ConsumersMetadata(topicUUID string) ([]ConsumerMetadata, error) {
	//remove: topics:cm:<topicUUID>:<consumerID> => metadata like lastseen
	keyPrefix := []byte(fmt.Sprintf("topics:cm:%s:", topicUUID))

	//since we do not have a pagination system just get them all 1M hard limit for now
	kvs, err := t.parent.ReadPaginate(concatSlices(t.prefix, keyPrefix), MaxConsumersPerTopic, 0)
	if err != nil {
		return nil, err
	}
	result := make([]ConsumerMetadata, len(kvs))
	for i := range kvs {
		cm := ConsumerMetadata{
			TopicUUID:  topicUUID,
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
func (t *LocalStorageMetadata) ConsumerPartitions(topicUUID string) ([]ConsumerPartitions, error) {
	topicPrefix := []byte(topicUUID + ":")
	kvs, err := t.parent.ReadPaginate(concatSlices(t.prefix, prefixConsPartsByUUID, topicPrefix), MaxConsumersPerTopic, 0)
	if err != nil {
		return nil, err
	}

	result := make([]ConsumerPartitions, len(kvs))
	for i := range kvs {
		result[i] = ConsumerPartitions{
			//the prefix is removed by the parent
			ConsumerID: string(kvs[i].Key),
		}
		//each partition has 2bytes
		src := kvs[i].Val
		dest := bytesToSliceUInt16(src)

		result[i].Partitions = dest
	}
	return result, nil
}

// UpsertConsumerPartitions replaces assigned partitions to specific consumers
// To remove a consumer completely use RemoveConsumers
func (t *LocalStorageMetadata) UpsertConsumerPartitions(topicUUID string, cps []ConsumerPartitions) error {
	topicPrefix := []byte(topicUUID + ":")

	kvs := make([]KVPair, len(cps))
	for i := range cps {
		kvs[i] = KVPair{
			Key: []byte(cps[i].ConsumerID),
			Val: sliceUInt16ToBytes(cps[i].Partitions),
		}
	}

	return t.parent.WriteBatch(concatSlices(t.prefix, prefixConsPartsByUUID, topicPrefix), kvs)
}

// asKV is an internal helper to convert into a stored KV with prefix
func consMetaToKVPair(cm ConsumerMetadata) (KVPair, error) {
	body, err := json.Marshal(cm)
	if err != nil {
		return KVPair{}, err
	}
	keyStr := fmt.Sprintf("%s:%s", cm.TopicUUID, cm.ConsumerID)
	return KVPair{
		//todo avoid memory allocs by using unsafe cast
		Key: concatSlices(prefixConsMetaByUUID, []byte(keyStr)),
		Val: body,
	}, nil
}
