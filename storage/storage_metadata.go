package storage

import (
	//nolint:gosec //md5 not used for security
	"crypto/md5"
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v2"
	"github.com/sirupsen/logrus"
)

/* LocalStorageMetadata handles all the cluster metadata that is stored on this node.
 All the Input and Output keys of all methods will NOT contain the prefix (they are relative keys)

The data is kept under these format/prefixes (Key => Value)

topics:<topicUUID>:consmt_<consumerID> => metadata like lastseen
topics:<topicUUID>:conspa_<consumerID> => the assigned partitions
topics:<topicUUID>:mt => topics metadata like no of partitions

TopicUUID is 16bytes
consumerID is not bounded (provided by the clients)

*/
type LocalStorageMetadata struct {
	db     *badger.DB
	prefix []byte
	logger logrus.FieldLogger
	parent *LocalStorage
}

func (t *LocalStorageMetadata) GetTopicUUID(topic string) ([]byte, error) {
	result := make([]byte, 16)
	//nolint:gosec //md5 not used for security
	hash := md5.Sum([]byte(topic))
	for i := range hash {
		result[i] = hash[i]
	}
	return result, nil
}

// UpsertConsumersMetadata can be used to update a consumer's presence.
// If it does not exists it will insert it.
// The batch is written as a transaction.
// It does NOT alter any input property like LastSeen
func (t *LocalStorageMetadata) UpsertConsumersMetadata(cons []ConsumerMetadata) error {
	kvs := make([]KVPair, len(cons))
	var err error

	for i := range cons {
		kvs[i], err = cons[i].asKV()
		if err != nil {
			return fmt.Errorf("failed transforming to KV: %w", err)
		}
	}

	return t.parent.WriteBatch(t.prefix, kvs)
}

// RemoveConsumers deletes all the metadata entries of a consumerID
// in the context of a topic. It uses a single transaction.
func (t *LocalStorageMetadata) RemoveConsumers(cons []ConsumerMetadata) error {
	kvs := make([][]byte, len(cons))

	//remove: topics:<topicUUID>:consmt_<consumerID> => metadata like lastseen
	for i := range cons {
		kv, err := cons[i].asKV()
		if err != nil {
			return fmt.Errorf("failed transforming to KV: %w", err)
		}
		kvs[i] = kv.Key
	}

	//TODO remove also its assigned
	//remove: topics:<topicUUID>:conspa_<consumerID> => the assigned partitions

	return t.parent.DeleteBatch(t.prefix, kvs)
}

// ConsumersMetadata can paginate trough the consumers of a topic
// Consumers can change between 2 calls so pages can overlap or have missing consumers
// Recommendation: use large limits (>1000)
func (t *LocalStorageMetadata) ConsumersMetadata(topicUUID string, limit, offset int) ([]ConsumerMetadata, error) {
	//remove: topics:<topicUUID>:consmt_<consumerID> => metadata like lastseen
	keyPrefix := []byte(fmt.Sprintf("topics:%s:consmt_", topicUUID))

	kvs, err := t.parent.ReadPaginate(concatSlices(t.prefix, keyPrefix), limit, offset)
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
