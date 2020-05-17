package storage

import (
	"fmt"
	"os"

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

// LocalMetadata is the constructor for a Metadata storage
func (s *LocalStorage) LocalMetadata() *LocalStorageMetadata {
	if s.metadataSingleton == nil {
		s.metadataSingleton = &LocalStorageMetadata{
			db:     s.db,
			prefix: []byte("a:"),
			logger: s.logger.WithField("component", "LocalStorageMetadata"),
			parent: s,
		}
	}
	return s.metadataSingleton
}

// LocalMessages constructs a storage for metadata
func (s *LocalStorage) LocalMessages() *LocalStorageMessages {
	if s.messagesSingleton == nil {
		s.messagesSingleton = &LocalStorageMessages{
			db:     s.db,
			prefix: []byte("t:"),
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

// NewLocalStorage spawns a new local database instance with disk persistence.
// One node/process should have only one instance of this.
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

// WriteBatch writes all the entries in a Write Transaction.
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
			return fmt.Errorf("failed to set: %w", err)
		}
	}
	err := wb.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	return nil
}

// ReadFirstsKVPairs get the firsts KV pairs with a prefix.
// It uses a Read Transaction and provides snapshot consistency at the beginning of the transaction.
// It returns 0...<limit> elements.
// It removes the given Prefix from the keys!
func (s *LocalStorage) ReadFirstsKVPairs(prefix []byte, limit int) ([]KVPair, error) {
	return s.ReadPaginate(prefix, limit, 0)
}

// ReadPaginate is a brutal way of paginating. It does not guarantee that data does not change
// between calls so items may be missing or overlap.
// TODO implement a proper pagination, one example would be to make a snapshot but it will work for one node only.
func (s *LocalStorage) ReadPaginate(prefix []byte, limit int, offset int) ([]KVPair, error) {
	if limit < 1 {
		s.logger.Warn("received 0 limit for ReadFirstsKVPairs")
		return []KVPair{}, nil
	}
	result := make([]KVPair, 0)
	prefixLength := len(prefix)

	return result, s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			//only prefetch all values if this is the first page
			PrefetchValues: offset == 0,
			PrefetchSize:   limit,
			Reverse:        false,
			AllVersions:    false,
			Prefix:         prefix,
			InternalAccess: false,
		})
		defer it.Close()
		index := 0
		for it.Rewind(); it.Valid(); it.Next() {
			index++
			if index <= offset {
				continue
			}
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result = append(result, KVPair{
				//remove the prefix
				Key: item.KeyCopy(nil)[prefixLength:],
				Val: val,
			})
			if len(result) >= limit {
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
			return fmt.Errorf("failed to delete: %w", err)
		}
	}
	err := wb.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}
	return nil
}

// Insert will fail if the key already exists
func (s *LocalStorage) Insert(prefix []byte, kv KVPair) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	key := concatSlices(prefix, kv.Key)

	_, err := txn.Get(key)
	if err != badger.ErrKeyNotFound {
		return ErrAlreadyExists
	}

	err = txn.Set(key, kv.Val)
	if err != nil {
		return err
	}
	return txn.Commit()
}

// Close will shutdown and release the lock on all local metadata instances derived from it.
func (s *LocalStorage) Close() error {
	return s.db.Close()
}
