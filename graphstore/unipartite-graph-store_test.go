package graphstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func AddSelfConnection(t *testing.T, g UnipartiteGraphStorage) {
	g.Clear()
	assert.Error(t, g.AddDirected("a", "a"))
	assert.Error(t, g.AddUndirected("a", "a"))
}

// AddSimpleGraph with the structure:
//
//         A--B----
//         |      |
//   C--D--E--F---G
//         |      |
//         H-------
func AddSimpleGraph(t *testing.T, g UnipartiteGraphStorage) {
	g.Clear()

}

func TestInMemory(t *testing.T) {
	g := NewInMemoryUnipartiteGraphStorage()
	AddSelfConnection(t, g)
}
