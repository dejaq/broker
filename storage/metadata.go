package storage

import (
	//nolint:gosec //md5 not used for security
	"crypto/md5"
	"errors"

	"github.com/dgraph-io/badger/v2"
	"github.com/sirupsen/logrus"
)

// LocalStorageMetadata handles all the cluster metadata that is stored on this node.
// All the Input and Output keys of all methods will NOT contain the prefix (they are relative keys)
type LocalStorageMetadata struct {
	db     *badger.DB
	prefix []byte
	logger logrus.FieldLogger
	parent *LocalStorage
}

func (t *LocalStorageMetadata) WriteBatch(batch []KVPair) error {
	return t.parent.WriteBatch(t.prefix, batch)
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

func (t *LocalStorageMetadata) UpsertConsumer(consumerID string) error {
	return errors.New("not implemented")
}

func (t *LocalStorageMetadata) UpsertTopic(topic string) error {
	return errors.New("not implemented")
}
