package consensus

import (
	"context"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/dgraph/raftwal"
	"go.etcd.io/etcd/raft"
)

type RaftWrapper struct {
	db         *badger.DB
	walStorage *raftwal.DiskStorage
	node       raft.Node
}

/* NewRaftWrapper wraps the dgraph implementation of
a wall-ahead-log storage on top of badgerDB and
the /etcd/raft go library.

The operational overhead of a DejaQ node will be simplified
since we use BadgerDB for Raft, Messages and metadata, especially
the backups/recovery.

It Panics on any failure, including data integrity sanity checks.
 A raft nodeID must always be correlated with the same badgerDB
instance.

see https://godoc.org/github.com/dgraph-io/dgraph/raftwal#Init

It implements the raft Storage interface https://github.com/etcd-io/etcd/blob/master/raft/storage.go
It also starts the raft node.
Call Close() for gracefully shutdown
*/
func NewRaftWrapper(db *badger.DB, raftNodeID uint64, peersNodeIDs []uint64) *RaftWrapper {
	result := &RaftWrapper{
		//we only have one raft group for now
		walStorage: raftwal.Init(db, raftNodeID, 0),
	}

	/* is this needed for restart not Init?
	// Recover the in-memory storage from persistent snapshot, state and entries.
	storage.ApplySnapshot(snapshot)
	storage.SetHardState(state)
	storage.Append(entries)

	  n := raft.RestartNode(c)

	*/
	c := &raft.Config{
		ID:              0x01,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         result.walStorage,
		MaxSizePerMsg:   65000 * 10,
		MaxInflightMsgs: 256,
	}
	result.node = raft.StartNode(c, nil)

	//add the node to the cluster
	result.node.ProposeConfChange(context.Background(), nil)
	return result
}

func (rw RaftWrapper) Close() {
	rw.node.Stop()
	rw.walStorage.Sync()
	rw.db.Close()
}
