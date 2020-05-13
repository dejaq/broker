package storage

import (
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/dgraph-io/badger/v2"
	"github.com/sirupsen/logrus"
)

/* LocalStorage is responsible for all the instances of BadgerDB on this instance.
 It knows how to init them, where they are and where each subset of data can be found

We rely on the Prefix feature of BadgeDB and keep multiple types of data in a shared instance.
If we want to split them or distribute different data on each node we only need to change this component. */
type LocalStorage struct {
	//for now is only a local DB per node
	db     *badger.DB
	logger logrus.FieldLogger

	metadataSingleton *LocalStorageMetadata
	messagesSingleton *LocalStorageMessages
}

func (s *LocalStorage) LocalMetadata() *LocalStorageMetadata {
	if s.metadataSingleton == nil {
		s.metadataSingleton = &LocalStorageMetadata{
			db:     s.db,
			prefix: []byte("a_"),
			logger: s.logger.WithField("component", "LocalStorageMetadata"),
			parent: s,
		}
	}
	return s.metadataSingleton
}

func (s *LocalStorage) LocalMessages() *LocalStorageMessages {
	if s.messagesSingleton == nil {
		s.messagesSingleton = &LocalStorageMessages{
			db:     s.db,
			prefix: []byte("t_"),
			logger: s.logger.WithField("component", "LocalStorageMessages"),
			parent: s,
		}
	}
	return s.messagesSingleton
}

// NewLocalStorageInMemory is used when persistence/durability is not required, good for integration tests
func NewLocalStorageInMemory(logger logrus.FieldLogger) (*LocalStorage, error) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		return nil, fmt.Errorf("failed to open inmemory DB err: %w", err)
	}

	return &LocalStorage{
		db:     db,
		logger: logger,
	}, nil
}

// NewLocalStorage spawns a new local messages instance with disk persistence. One node/process should have only one instance of this.
func NewLocalStorage(dataDirectory string, logger logrus.FieldLogger) (*LocalStorage, error) {
	partitionDBDirectory := fmt.Sprintf("%s/allinone", dataDirectory)
	//group can have read it for backups, only us for execute
	err := os.MkdirAll(partitionDBDirectory, 0740)
	if err != nil {
		return nil, fmt.Errorf("failed to mkdir %s err: %w", partitionDBDirectory, err)
	}
	db, err := badger.Open(badger.DefaultOptions(partitionDBDirectory))
	if err != nil {
		return nil, fmt.Errorf("failed to open DB %s err: %w", partitionDBDirectory, err)
	}
	logger.Infof("opened local badgerDB at %s", partitionDBDirectory)

	//TODO add a ticker goroutine that calls garbage collector!

	return &LocalStorage{
		db:     db,
		logger: logger,
	}, nil
}

// UpsertMessages writes all the entries in a Write Transaction.
// It prepends the Prefix to all KEYS!
func (s *LocalStorage) WriteBatch(prefix []byte, batch []KVPair) error {
	//write to DB
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, msg := range batch {
		key := make([]byte, len(prefix)+len(msg.Key))
		copy(key[:len(prefix)], prefix)
		copy(key[len(prefix):], msg.Key)

		err := wb.Set(key, msg.Val)
		if err != nil {
			return errors.Wrap(err, "failed to write")
		}
	}
	err := wb.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush")
	}
	return nil
}

// ReadFirstsKVPairs get the firsts KV pairs with a prefix.
// It uses a Read Transaction and provides snapshot consistency at the begining of the transaction.
// It returns 0 or maxCount elements.
// It removes the Prefix from the keys!
func (s *LocalStorage) ReadFirstsKVPairs(prefix []byte, maxCount int) ([]KVPair, error) {
	result := make([]KVPair, 0, maxCount)
	prefixLength := len(prefix)

	return result, s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   maxCount,
			Reverse:        false,
			AllVersions:    false,
			Prefix:         prefix,
			InternalAccess: false,
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result = append(result, KVPair{
				//remove the prefix
				Key: item.Key()[prefixLength:],
				Val: val,
			})
			if len(result) >= maxCount {
				break
			}
		}
		return nil
	})
}

// DeleteBatch can be used to remove a series of keys in a transaction.
func (s *LocalStorage) DeleteBatch(prefix []byte, keys [][]byte) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, origKey := range keys {
		key := make([]byte, len(prefix)+len(origKey))
		copy(key[:len(prefix)], prefix)
		copy(key[len(prefix):], origKey)

		err := wb.Delete(key)
		if err != nil {
			return errors.Wrap(err, "failed to delete")
		}
	}
	err := wb.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush")
	}
	return nil
}

// Close will shutdown and release the lock on all local messages instances derived from it.
func (s *LocalStorage) Close() error {
	return s.db.Close()
}
