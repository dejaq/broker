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

func (suite *InMemoryMessagesSuite) TestMessagesSmoke() {
	//TODO

}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInMemoryMessagesSuite(t *testing.T) {
	suite.Run(t, new(InMemoryMessagesSuite))
}
