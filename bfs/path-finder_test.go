package bfs

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestNetworkConnections(t *testing.T) {

	n := NewNetworkConnections(2)
	n.AddConnections("A", "set-A", "B", "set-B", []Path{NewPath("A", "B", "C")})
	n.AddConnections("A", "set-A2", "C", "set-C", []Path{NewPath("A", "D", "C")})
	n.AddConnections("E", "set-E", "B", "set-B", []Path{NewPath("E", "B"), NewPath("E", "A", "B")})

	expected := NetworkConnections{
		EntityIdToSetNames: map[string]*set.Set[string]{
			"A": set.NewPopulatedSet("set-A", "set-A2"),
			"B": set.NewPopulatedSet("set-B"),
			"C": set.NewPopulatedSet("set-C"),
			"E": set.NewPopulatedSet("set-E"),
		},
		Connections: map[string]map[string][]Path{
			"A": {
				"B": []Path{NewPath("A", "B", "C")},
				"C": []Path{NewPath("A", "D", "C")},
			},
			"E": {
				"B": []Path{NewPath("E", "B"), NewPath("E", "A", "B")},
			},
		},
		MaxDepth: 2,
	}

	assert.True(t, expected.Equal(n))
}
