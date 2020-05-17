package dealer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type Allocate struct {
	suite.Suite
}

func (suite *Allocate) TestAllocate() {
	var table = []struct {
		name          string
		consumersIds  []string
		partitionsIds []string
		result        []Allocation
	}{
		{
			"ZeroConsumersZeroPartitions",
			createIds("c", 0),
			createIds("p", 0),
			nil,
		},
		{
			"ZeroConsumersManyPartitions",
			createIds("c", 0),
			createIds("p", 3),
			nil,
		},
		{
			"ManyConsumersZeroPartitions",
			createIds("c", 3),
			createIds("p", 0),
			nil,
		},
		{
			"OneConsumerManyPartitions",
			createIds("c", 1),
			createIds("p", 3),
			[]Allocation{
				{"c0", "p0"},
				{"c0", "p1"},
				{"c0", "p2"},
			},
		},
		{
			"ManyConsumersOnePartitions",
			createIds("c", 3),
			createIds("p", 1),
			[]Allocation{
				{"c0", "p0"},
			},
		},
		{
			"ConsumersEqualPartitionsMany",
			createIds("c", 3),
			createIds("p", 3),
			[]Allocation{
				{"c0", "p0"},
				{"c1", "p1"},
				{"c2", "p2"},
			},
		},
		{
			"ManyConsumersLessPartitions",
			createIds("c", 5),
			createIds("p", 3),
			[]Allocation{
				{"c0", "p0"},
				{"c1", "p1"},
				{"c2", "p2"},
			},
		},
		{
			"ManyPartitionsLessConsumers",
			createIds("c", 3),
			createIds("p", 5),
			[]Allocation{
				{"c0", "p0"},
				{"c1", "p1"},
				{"c2", "p2"},
				{"c0", "p3"},
				{"c1", "p4"},
			},
		},
		{
			"ManyConsumersDoubleThanPartitions",
			createIds("c", 4),
			createIds("p", 2),
			[]Allocation{
				{"c0", "p0"},
				{"c1", "p1"},
			},
		},
		{
			"ManyPartitionsDoubleThanConsumers",
			createIds("c", 2),
			createIds("p", 4),
			[]Allocation{
				{"c0", "p0"},
				{"c1", "p1"},
				{"c0", "p2"},
				{"c1", "p3"},
			},
		},
	}

	for _, tt := range table {
		result := allocate(tt.consumersIds, tt.partitionsIds)
		suite.Equal(tt.result, result, tt.name)
	}
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestAllocate(t *testing.T) {
	suite.Run(t, new(Allocate))
}

func createIds(t string, n int) []string {
	r := make([]string, n, n)
	for i := 0; i < n; i++ {
		r[i] = fmt.Sprintf("%s%d", t, i)
	}
	return r
}
