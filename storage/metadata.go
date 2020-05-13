package storage

import (
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
	return nil, errors.New("not implemented")
}

func (t *LocalStorageMetadata) UpsertConsumer(consumerID string) error {
	return errors.New("not implemented")
}

func (t *LocalStorageMetadata) UpsertTopic(topic string) error {
	return errors.New("not implemented")
}
