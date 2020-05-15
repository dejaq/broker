package storage

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type consumerMetadataSet map[string]ConsumerMetadata

func (cs consumerMetadataSet) Contains(c ConsumerMetadata) bool {
	_, ok := cs[c.ConsumerID]
	return ok
}
func (cs consumerMetadataSet) Remove(c ConsumerMetadata) {
	delete(cs, c.ConsumerID)
}

type InMemoryMetadataSuite struct {
	suite.Suite
	messages *LocalStorageMetadata
}

func (suite *InMemoryMetadataSuite) SetupTest() {
	storage, err := NewLocalStorageInMemory(logrus.StandardLogger())
	suite.Require().NoError(err)

	suite.messages = storage.LocalMetadata()
}

func (suite *InMemoryMetadataSuite) TestConsumersMetadata() {
	topics := []string{"topic1", "lastone"}

	c1 := ConsumerMetadata{
		TopicUUID:  topics[0],
		ConsumerID: "cons1",
		LastSeen:   time.Now().UTC().Round(time.Second),
	}
	c2 := ConsumerMetadata{
		TopicUUID:  topics[0],
		ConsumerID: "cons2",
		LastSeen:   time.Now().UTC().Round(time.Second).Add(time.Hour),
	}
	c3 := ConsumerMetadata{
		TopicUUID:  topics[1],
		ConsumerID: "cons1TTopic2",
		LastSeen:   time.Now().UTC().Round(time.Second).Add(3 * time.Hour),
	}

	tests := []struct {
		name  string
		input []ConsumerMetadata
	}{
		{name: "fewConsumers",
			input: []ConsumerMetadata{c3, c1, c2},
		},
		{name: "onlyOneTopic",
			input: []ConsumerMetadata{c3},
		},
	}

	//this also tests Message constructors
	for _, t := range tests {
		test := t
		suite.Run(test.name, func() {
			//add all of them
			err := suite.messages.UpsertConsumersMetadata(test.input)
			suite.Require().NoError(err)

			//test GET for each topic
			var gotCons []ConsumerMetadata
			for _, topic := range topics {
				got, err := suite.messages.ConsumersMetadata(topic)
				suite.Require().NoError(err)
				gotCons = append(gotCons, got...)
			}
			suite.Require().ElementsMatch(test.input, gotCons)

			//remove all of them
			err = suite.messages.RemoveConsumers(test.input)
			suite.Require().NoError(err)

			//test that DELETE worked
			for _, topic := range topics {
				got, err := suite.messages.ConsumersMetadata(topic)
				suite.Require().NoError(err)

				suite.Require().Len(got, 0)
			}
		})
	}
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInMemoryMetadataSuite(t *testing.T) {
	suite.Run(t, new(InMemoryMetadataSuite))
}
