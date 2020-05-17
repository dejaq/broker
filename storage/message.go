package storage

import (
	"encoding/binary"
	"errors"
	"math/rand"

	"github.com/sirupsen/logrus"
)

/* Message represents a KV but with Message logic
 The uniqueness of a message is based on the <Priority,ID> Tuple in the context
of a partition of a topic.
Meaning you can have the same messageID in different priorities, or same priority
but different partitions, or same priority and same partition but in a different topic.
*/
type Message struct {
	Priority uint16
	ID       []byte
	// Body is only required for the Storage.Upsert operation, otherwise omit it
	Body []byte
}

// Key prepends the priority to the messageID to be stored in
// a KV store ordered by priority.
// Its size is 8bytes, 2 for the priority and 6 for the entropy
func (m Message) Key() []byte {
	key := make([]byte, prioritySize+len(m.ID))
	binary.BigEndian.PutUint16(key, m.Priority)
	copy(key[prioritySize:], m.ID)
	return key
}

// NewMessageFromKV constructs a message from a BadgerKV
// It does not allocate memory
func NewMessageFromKV(kv KVPair) (Message, error) {
	if len(kv.Key) != prioritySize+msgIDSize {
		return Message{}, errors.New("invalid key")
	}

	return Message{
		//first 2 bytes represent the priority
		Priority: binary.BigEndian.Uint16(kv.Key),
		ID:       kv.Key[prioritySize:],
		Body:     kv.Val,
	}, nil
}

// KVPair is a simple KeyValue pair
type KVPair struct {
	// For message the  Key is a 64bit (8 bytes) that contains 2 bytes priority + random bytes
	Key []byte
	// For Message is the body
	Val []byte
}

// Clone ensures a copy that do not share the underlying arrays
// It allocates memory!
func (p KVPair) Clone() KVPair {
	result := KVPair{
		Key: make([]byte, len(p.Key)),
		Val: make([]byte, len(p.Val)),
	}

	copy(result.Key, p.Key)
	copy(result.Val, p.Val)

	return result
}

/* NewMessageWithRandomID generates a new random ID
We have 6bytes of entropy, that means 2^48 unique possibilities
for each partition of a topic (200.000.000.000.000 values).
*/
func NewMessageWithRandomID(priority uint16, body []byte) Message {
	randomMsgID := make([]byte, msgIDSize)

	//if using global rand singleton source is found to be a Mutex bottleneck
	// move this in a struct with its
	// own pool of random sources, but probably this will never happen.
	size, err := rand.Read(randomMsgID) //nolint:gosec
	if err != nil || size != msgIDSize {
		// this means the node has bigger issues than this, most likely it needs to be terminated
		logrus.StandardLogger().WithError(err).Error("the node ran out of entropy")
	}

	return Message{
		Priority: priority,
		ID:       randomMsgID,
		Body:     body,
	}
}
