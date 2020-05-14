package storage

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type InMemoryMessagesSuite struct {
	suite.Suite
	messages *LocalStorageMessages
}

func (suite *InMemoryMessagesSuite) SetupTest() {
	storage, err := NewLocalStorageInMemory(logrus.StandardLogger())
	suite.Assert().NoError(err)

	suite.messages = storage.LocalMessages()
}

func (suite *InMemoryMessagesSuite) TestProduceAndConsume() {
	k1 := []byte("key1")
	k1a := []byte("key1_a")
	k1b := []byte("key1_b")
	k1c := []byte("key1_c")
	k2 := []byte("key2")
	k3 := []byte("key3")
	k4 := []byte("key4")

	tests := []struct {
		name          string
		input         [][]byte
		limit         int
		orderedOutput [][]byte
	}{
		{
			name:          "zero limit",
			input:         [][]byte{k2, k4, k1, k3},
			limit:         0,
			orderedOutput: [][]byte{},
		},
		{
			name:          "larger limit",
			input:         [][]byte{k2, k4, k1, k3},
			limit:         5,
			orderedOutput: [][]byte{k1, k2, k3, k4},
		},
		{
			name:          "only first 2",
			input:         [][]byte{k2, k4, k1, k3},
			limit:         2,
			orderedOutput: [][]byte{k1, k2},
		},
		{
			name:          "prefixed first 2",
			input:         [][]byte{k1c, k1a, k1b, k3, k2, k4},
			limit:         2,
			orderedOutput: [][]byte{k1a, k1b},
		},
	}

	//this also tests Message constructors
	for _, test := range tests {
		suite.Run(test.name, func() {
			err := suite.messages.UpsertMessages(test.name, 42, keysToMsgs(test.input))
			suite.Assert().NoError(err)

			got, err := suite.messages.LowestPriorityMsgs(test.name, 42, test.limit)
			suite.Assert().NoError(err)
			if suite.Assert().Equal(len(test.orderedOutput), len(got)) {
				for i := range got {
					suite.Assert().Equal(test.orderedOutput[i], got[i].ID)
				}
			}
		})
	}
}

func keysToMsgs(in [][]byte) []Message {
	result := make([]Message, len(in))
	for i := range in {
		result[i] = Message{ID: in[i], Priority: 1}
	}
	return result
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInMemoryMessagesSuite(t *testing.T) {
	suite.Run(t, new(InMemoryMessagesSuite))
}
