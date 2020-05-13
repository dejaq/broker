package storage

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type InMemoryStorageSuite struct {
	suite.Suite
	storage *LocalStorage
}

func (suite *InMemoryStorageSuite) SetupTest() {
	var err error
	suite.storage, err = NewLocalStorageInMemory(logrus.StandardLogger())
	if err != nil {
		suite.Error(err)
	}
}

func (suite *InMemoryStorageSuite) TestStorageReadAfterWrite() {
	sentMsgs := []KVPair{
		{Key: []byte("key1"), Val: []byte("val1")},
		{Key: []byte("key2"), Val: []byte("val2")},
	}
	prefix := []byte("readafterwrite")

	//we clone it to also check for side effects in the messages
	err := suite.storage.WriteBatch(prefix, cloneKVSlice(sentMsgs))
	suite.Assert().NoError(err)

	result, err := suite.storage.ReadFirstsKVPairs(prefix, 3)
	suite.Assert().NoError(err)

	suite.Assert().ElementsMatch(sentMsgs, result)

	clonedKey := sentMsgs[0].Clone().Key
	err = suite.storage.DeleteBatch(prefix, [][]byte{clonedKey})
	suite.Assert().NoError(err)

	result, err = suite.storage.ReadFirstsKVPairs(prefix, 3)
	suite.Assert().Len(result, 1)
	suite.Assert().Equal(sentMsgs[1], result[0])
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInMemoryStorageSuite(t *testing.T) {
	suite.Run(t, new(InMemoryStorageSuite))
}

func cloneKVSlice(orig []KVPair) []KVPair {
	result := make([]KVPair, len(orig))
	for i := range orig {
		result[i] = orig[i].Clone()
	}
	return result
}
