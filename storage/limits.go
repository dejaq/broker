package storage

import (
	"errors"
	"math"
	"unicode"
)

const (
	// MaxConsumersPerTopic is a hard limit on active consumers
	MaxConsumersPerTopic int = 1000000
)

var (
	ErrTopicIDInvalid         = errors.New("topic ID must have 1-1024 length consists of AlphaNumeric and Control characters. L,N and C Unicode tables")
	ErrConsumerInvalid        = errors.New("consumer ID must have 1-256 length consists of AlphaNumeric and Control characters. L,N and C Unicode tables")
	ErrMessageInvalid         = errors.New("message must have under 64Kb in size")
	ErrTopicPartitionsInvalid = errors.New("topic must have between 1 and 65535 partitions")
)

// IsTopicIDValid used to check for a topic name before creating
func IsTopicIDValid(topic string) bool {
	if len(topic) < 1 || len(topic) > 1024 {
		return false
	}

	for _, r := range topic {
		if unicode.IsLetter(r) || unicode.IsDigit(r) ||
			unicode.IsNumber(r) || unicode.IsControl(r) {
			continue
		}
		return false
	}
	return true
}

// IsTopicPartitionsCountValid used to limit the number of partitions
func IsTopicPartitionsCountValid(l int) bool {
	return l > 0 && l < math.MaxUint16
}

// IsConsumerIDValid is used before any consumer Upsert
func IsConsumerIDValid(consumerID string) bool {
	if len(consumerID) < 1 || len(consumerID) > 256 {
		return false
	}

	for _, r := range consumerID {
		if unicode.IsLetter(r) || unicode.IsDigit(r) ||
			unicode.IsNumber(r) || unicode.IsControl(r) {
			continue
		}
		return false
	}
	return true
}

// IsMsgBodyValid is used before any msg upsert
func IsMsgBodyValid(body []byte) bool {
	return len(body) < 64*1024
}
