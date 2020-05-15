package storage

import (
	"fmt"
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

// Test that removing a msg does not affect other partition or priority
func (suite *InMemoryMessagesSuite) TestDelete() {
	key := []byte("key3")
	topic := "singleton"

	p1 := keysToMsgs([][]byte{key}, 87)
	err := suite.messages.Upsert(topic, 42, p1)
	suite.Assert().NoError(err)

	//different priority
	p2 := keysToMsgs([][]byte{key}, 0)
	err = suite.messages.Upsert(topic, 42, p2)
	suite.Assert().NoError(err)

	//different partition
	p3 := keysToMsgs([][]byte{key}, 87)
	err = suite.messages.Upsert(topic, 87, p3)
	suite.Assert().NoError(err)

	err = suite.messages.Ack(topic, 42, p1)
	suite.Assert().NoError(err)

	//this should have the priority 0 left in it
	result, err := suite.messages.GetLowestPriority(topic, 42, 100)
	suite.Assert().NoError(err)
	suite.Assert().Len(result, 1)
	suite.Assert().Equal(key, result[0].ID)
	suite.Assert().Equal(uint16(0), result[0].Priority)

	//the other should still exists on different partition
	result, err = suite.messages.GetLowestPriority(topic, 87, 100)
	suite.Assert().NoError(err)
	suite.Assert().Len(result, 1)
	suite.Assert().Equal(key, result[0].ID)
	suite.Assert().Equal(uint16(87), result[0].Priority)
}

func (suite *InMemoryMessagesSuite) TestSeparationOfTopics() {
	key := []byte("key3")
	topic1 := "topic1"
	topic2 := "topic2"

	for _, t := range []string{topic1, topic2} {
		p1 := keysToMsgs([][]byte{key}, 42)
		err := suite.messages.Upsert(t, 42, p1)
		suite.Assert().NoError(err)

		result, err := suite.messages.GetLowestPriority(t, 42, 100)
		suite.Assert().NoError(err)
		suite.Assert().Len(result, 1, "msg from another topic was read")
	}

	err := suite.messages.Ack(topic2, 42, keysToMsgs([][]byte{key}, 42))
	suite.Assert().NoError(err)

	result, err := suite.messages.GetLowestPriority(topic2, 42, 100)
	suite.Assert().NoError(err)
	suite.Assert().Len(result, 0)

	result, err = suite.messages.GetLowestPriority(topic1, 42, 100)
	suite.Assert().NoError(err)
	suite.Assert().Len(result, 1)
	suite.Assert().Equal(result[0].ID, key)
	suite.Assert().Equal(result[0].Priority, uint16(42))
}

func (suite *InMemoryMessagesSuite) TestOrderedKeysInAPriority() {
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
	for _, t := range tests {
		test := t
		suite.Run(test.name, func() {
			err := suite.messages.Upsert(test.name, 42, keysToMsgs(test.input, 1))
			suite.Assert().NoError(err)

			got, err := suite.messages.GetLowestPriority(test.name, 42, test.limit)
			suite.Assert().NoError(err)
			if suite.Assert().Equal(len(test.orderedOutput), len(got)) {
				for i := range got {
					suite.Assert().Equal(test.orderedOutput[i], got[i].ID)
				}
			}
		})
	}
}

func (suite *InMemoryMessagesSuite) TestOrderedKeysInMultiplePrioritiesAndPartitions() {
	k1 := []byte("key1")
	k2 := []byte("key2")
	k3 := []byte("key3")

	//each test will be run for each partition / topic
	topics := []string{"topic1"} //, "megatopic"}
	partitions := []uint16{0, 87, 42}

	tests := []struct {
		name          string
		input         []Message
		orderedOutput []Message
	}{
		{
			name: "3priorities1msg",
			input: flattenSlices(
				keysToMsgs([][]byte{k3}, 3),
				keysToMsgs([][]byte{k1}, 5),
				keysToMsgs([][]byte{k2}, 1),
			),
			orderedOutput: flattenSlices(
				keysToMsgs([][]byte{k2}, 1),
				keysToMsgs([][]byte{k3}, 3),
				keysToMsgs([][]byte{k1}, 5),
			),
		},
		{
			name: "3priorities3msgs",
			input: flattenSlices(
				keysToMsgs([][]byte{k3, k1, k2}, 3),
				keysToMsgs([][]byte{k1, k2, k3}, 5),
				keysToMsgs([][]byte{k3, k2, k1}, 1),
			),
			orderedOutput: flattenSlices(
				keysToMsgs([][]byte{k1, k2, k3}, 1),
				keysToMsgs([][]byte{k1, k2, k3}, 3),
				keysToMsgs([][]byte{k1, k2, k3}, 5),
			),
		},
	}

	for _, test := range tests {
		for _, topic := range topics {
			//this also tests Message constructors
			test := test

			//we put ALL the data in and then check if it comes out right
			for _, partition := range partitions {
				err := suite.messages.Upsert(topic, partition, test.input)
				suite.Assert().NoError(err)
			}
		}

		for _, topic := range topics {
			test := test
			topic := topic
			for _, partition := range partitions {
				partition := partition
				suite.Run(fmt.Sprintf("%s-%d-%s", topic, partition, test.name), func() {
					got, err := suite.messages.GetLowestPriority(topic, partition, 100)
					suite.Assert().NoError(err)
					if suite.Assert().Equal(len(test.orderedOutput), len(got)) {
						for i := range got {
							suite.Assert().Equal(test.orderedOutput[i].ID, got[i].ID)
							suite.Assert().Equal(test.orderedOutput[i].Priority, got[i].Priority)
							suite.Assert().Equal(test.orderedOutput[i].Body, got[i].Body)
						}
					}
				})
			}
		}
	}
}

func keysToMsgs(in [][]byte, priority uint16) []Message {
	result := make([]Message, len(in))
	for i := range in {
		result[i] = Message{
			ID:       in[i],
			Priority: priority,
			Body:     append([]byte("body_"), in[i]...),
		}
	}
	return result
}

func flattenSlices(list ...[]Message) []Message {
	var result []Message
	for _, cont := range list {
		result = append(result, cont...)
	}
	return result
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInMemoryMessagesSuite(t *testing.T) {
	suite.Run(t, new(InMemoryMessagesSuite))
}
