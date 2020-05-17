package dealer

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Storage need to know about topics, consumers on topic
//  and set new allocations consumer/partition
type Storage interface {
	TopicsIds() []string
	PartitionsIds() []string
	ConsumersIdsByTopic(topicID string) []string
	ConsumerLastSeen(consumerID string) time.Time
	SetAllocations(allocations map[string]string) error
	RemoveConsumer(consumerID string) error
}

// Consensus if current broker is leader for a topic
type Consensus interface {
	isLeader(topic string) bool
}

// Config holds all dependencies for new Dealer creation
type Config struct {
	Storage      Storage
	TickDuration time.Duration
}

// Allocation holds assignment of one consumer to a partition
type Allocation struct {
	ConsumerID, PartitionID string
}

// Dealer it is responsible with assignments partition(s)/consumers(s)
// at each tick duration or on demand, it depends on his storage interface
type Dealer struct {
	ctx                         context.Context
	logger                      logrus.FieldLogger
	m                           sync.RWMutex
	storage                     Storage
	consensus                   Consensus
	tickDuration                time.Duration
	consumerMaxInactiveDuration time.Duration
}

// New returns a Dealer ready to Start, Stop and RunNow
func New(ctx context.Context, conf Config, logger logrus.FieldLogger) *Dealer {
	return &Dealer{
		ctx:          ctx,
		logger:       logger,
		m:            sync.RWMutex{},
		storage:      conf.Storage,
		tickDuration: conf.TickDuration,
	}
}

func (d *Dealer) tick() map[string][]Allocation {
	topicAllocations := make(map[string][]Allocation)

	for _, topicID := range d.storage.TopicsIds() {
		if !d.consensus.isLeader(topicID) {
			continue
		}

		consumersIds := d.removeUnseenConsumers(d.storage.ConsumersIdsByTopic(topicID))
		partitionsIds := d.storage.PartitionsIds()

		if len(consumersIds) < 1 || len(partitionsIds) < 1 {
			continue
		}
		topicAllocations[topicID] = allocate(consumersIds, partitionsIds)
	}

	return topicAllocations
}

func allocate(cIds, pIds []string) []Allocation {
	var allocations []Allocation
	cn := len(cIds)
	var i int
	for _, pId := range pIds {
		if i > cn-1 {
			i = 0
		}
		allocations = append(allocations, Allocation{
			ConsumerID:  cIds[i],
			PartitionID: pId,
		})
	}
	return allocations
}

func (d *Dealer) removeUnseenConsumers(consumersIds []string) []string {
	var activeConsumers []string
	for _, consumerID := range consumersIds {
		lastActive := d.storage.ConsumerLastSeen(consumerID)
		if lastActive.Add(d.consumerMaxInactiveDuration).Before(time.Now().UTC()) {
			err := d.storage.RemoveConsumer(consumerID)
			d.logger.WithError(err).Error("remove inactive consumer")
			continue
		}
		activeConsumers = append(activeConsumers, consumerID)
	}
	return activeConsumers
}

func (d *Dealer) Start() {
	// TODO implement
}

func (d *Dealer) RunNow() {
	// TODO implement me
}

func (d *Dealer) Stop() {
	// TODO implement me
}
