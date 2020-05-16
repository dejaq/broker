package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

//
//type consumerMetadataSet map[string]ConsumerMetadata
//
//func (cs consumerMetadataSet) Contains(c ConsumerMetadata) bool {
//	_, ok := cs[c.ConsumerID]
//	return ok
//}
//func (cs consumerMetadataSet) Remove(c ConsumerMetadata) {
//	delete(cs, c.ConsumerID)
//}

type InMemoryMetadataSuite struct {
	suite.Suite
	metadata *LocalStorageMetadata
}

func (suite *InMemoryMetadataSuite) SetupTest() {
	storage, err := NewLocalStorageInMemory(logrus.StandardLogger())
	suite.Require().NoError(err)

	suite.metadata = storage.LocalMetadata()
}

func (suite *InMemoryMetadataSuite) TestCreateTopicsMetadata() {
	tests := []struct {
		name       string
		inputID    string
		inputParts int
		shouldErr  error
	}{
		{
			name:       "noName",
			inputID:    "",
			inputParts: 42,
			shouldErr:  ErrTopicIDInvalid,
		},
		{
			name:       "nameTooLong",
			inputID:    strings.Repeat("a", 1025),
			inputParts: 42,
			shouldErr:  ErrTopicIDInvalid,
		},
		{
			name:       "nameOK",
			inputID:    "perfectName01_-[]',./(){}",
			inputParts: 42,
			shouldErr:  nil,
		},
		{
			//this relies on the order of tests
			name:       "duplicate",
			inputID:    "perfectName01_-[]',./(){}",
			inputParts: 42,
			shouldErr:  ErrAlreadyExists,
		},
	}

	//a set of topicIDs to be checked later for existence
	successIDs := map[string]struct{}{tests[2].inputID: {}}

	for _, t := range tests {
		test := t
		suite.Run(test.name, func() {
			mtInsert, gotErr := suite.metadata.CreateTopic(test.inputID, test.inputParts)
			suite.Require().Equal(test.shouldErr, gotErr)

			if gotErr != nil {
				return
			}
			suite.Require().Equal(test.inputID, mtInsert.TopicID)
			suite.Require().Equal(test.inputParts, mtInsert.PartitionsCount)
			suite.Require().NotEmpty(mtInsert.TopicUUID)

			mt, err := suite.metadata.TopicMetadata(test.inputID)
			suite.Require().NoError(err)
			suite.Require().Equal(test.inputID, mt.TopicID)
			suite.Require().Equal(test.inputParts, mt.PartitionsCount)
			suite.Require().Equal(mtInsert.TopicUUID, mt.TopicUUID)
		})
	}

	//now we test the topic()
	topics, err := suite.metadata.Topics()
	suite.Require().NoError(err)
	suite.Require().Len(topics, len(successIDs))
	for topicID := range successIDs {
		var found bool
		for _, t := range topics {
			if t.TopicID == topicID {
				found = true
				break
			}
		}
		if !found {
			suite.Require().Failf("topics() failed", "topic %s was not returned by topics()", topicID)
		}
	}
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
			err := suite.metadata.UpsertConsumersMetadata(test.input)
			suite.Require().NoError(err)

			//test GET for each topic
			var gotCons []ConsumerMetadata
			for _, topic := range topics {
				got, errGet := suite.metadata.ConsumersMetadata(topic)
				suite.Require().NoError(errGet)
				gotCons = append(gotCons, got...)
			}
			suite.Require().ElementsMatch(test.input, gotCons)

			//remove all of them
			err = suite.metadata.RemoveConsumers(test.input)
			suite.Require().NoError(err)

			//test that DELETE worked
			for _, topic := range topics {
				got, err := suite.metadata.ConsumersMetadata(topic)
				suite.Require().NoError(err)

				suite.Require().Len(got, 0)
			}
		})
	}
}

func (suite *InMemoryMetadataSuite) TestConsumersPartitions() {
	topics := []string{"topic1", "lastone"}

	cp1 := ConsumerPartitions{
		ConsumerID: "cons1",
		Partitions: []uint16{42, 1, 99},
	}
	cp2 := ConsumerPartitions{
		ConsumerID: "cons2",
		Partitions: []uint16{},
	}
	cp3 := ConsumerPartitions{
		ConsumerID: "cons3",
		Partitions: []uint16{22, 0},
	}
	cons := []ConsumerPartitions{cp2, cp3, cp1}

	for _, topic := range topics {
		err := suite.metadata.UpsertConsumerPartitions(topic, cons)
		suite.Require().NoError(err)
	}

	for _, topic := range topics {
		got, err := suite.metadata.ConsumerPartitions(topic)
		suite.Require().NoError(err)
		suite.Require().Len(got, len(cons), "more or less consumers returned")

		for _, c := range cons {
			//we need to search for it
			var found bool
			for _, cg := range got {
				if cg.ConsumerID == c.ConsumerID {
					suite.Assert().ElementsMatch(cg.Partitions, c.Partitions, "partitions for cons %s in topic %s were lost", cg.ConsumerID, topic)
					found = true
					break
				}
			}
			suite.Require().True(found, "consumer lost %s in topic %s", c.ConsumerID, topic)
		}
	}
}

func (suite *InMemoryMetadataSuite) TestDeleteConsumer() {
	topics := []string{"topic1", "topic2"}

	cp1 := ConsumerPartitions{
		ConsumerID: "cons1",
		Partitions: []uint16{42, 1, 99},
	}
	cp2 := ConsumerPartitions{
		ConsumerID: "cons2",
		Partitions: []uint16{},
	}
	c1 := ConsumerMetadata{
		ConsumerID: "cons1",
		LastSeen:   time.Now().UTC().Round(time.Second),
	}
	c2 := ConsumerMetadata{
		ConsumerID: "cons2",
		LastSeen:   time.Now().UTC().Round(time.Second).Add(time.Hour),
	}

	consExistsIn := func(shouldExists bool, topic string, consumerID string) {
		consMeta, err := suite.metadata.ConsumersMetadata(topic)
		suite.Require().NoError(err)

		var found bool
		for _, cn := range consMeta {
			if cn.ConsumerID == consumerID {
				found = true
				break
			}
		}
		suite.Require().Equal(shouldExists, found, "state of consumer in consumer metadata is wrong exp %v got %v for  %s topic %s", shouldExists, found, consumerID, topic)

		consParts, err := suite.metadata.ConsumerPartitions(topic)
		suite.Require().NoError(err)

		for _, cn := range consParts {
			if cn.ConsumerID == consumerID {
				found = true
				break
			}
		}
		suite.Require().Equal(shouldExists, found, "state of consumer in consumer partitions is wrong exp %v got %v for  %s topic %s", shouldExists, found, consumerID, topic)
	}

	//add all of them
	for _, topic := range topics {
		c1.TopicUUID = topic
		c2.TopicUUID = topic
		err := suite.metadata.UpsertConsumersMetadata([]ConsumerMetadata{c1, c2})
		suite.Require().NoError(err)

		err = suite.metadata.UpsertConsumerPartitions(topic, []ConsumerPartitions{cp1, cp2})
		suite.Require().NoError(err)
	}

	//check if exists, delete and then check again
	//this way we also verify that the removing the consumer from one topic
	//does not affect its state on other topics he is subscribed too
	consExistsIn(true, topics[0], c1.ConsumerID)
	consExistsIn(true, topics[0], c2.ConsumerID)
	consExistsIn(true, topics[1], c1.ConsumerID)
	consExistsIn(true, topics[1], c2.ConsumerID)

	c2.TopicUUID = topics[1]
	err := suite.metadata.RemoveConsumers([]ConsumerMetadata{c2})
	suite.Require().NoError(err)
	consExistsIn(true, topics[0], c1.ConsumerID)
	consExistsIn(true, topics[0], c2.ConsumerID)
	consExistsIn(true, topics[1], c1.ConsumerID)
	consExistsIn(false, topics[1], c2.ConsumerID)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInMemoryMetadataSuite(t *testing.T) {
	suite.Run(t, new(InMemoryMetadataSuite))
}
