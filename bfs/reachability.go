package bfs

import (
	"fmt"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/golang-collections/collections/queue"
)

// ReachableVertices from a root vertex up to a maximum depth.
func ReachableVertices(g graphstore.UnipartiteGraphStore, root string,
	maxDepth int) (*set.Set[string], error) {

	// Preconditions
	found, err := g.HasEntity(root)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("Root vertex not found: %v", root)
	}

	if maxDepth < 0 {
		return nil, fmt.Errorf("Invalid maximum depth: %v", maxDepth)
	}

	// Set of identifiers of discovered vertices
	discovered := set.NewSet[string]()
	discovered.Add(root)

	// Queue to hold the vertices to visit
	q := queue.New()
	q.Enqueue(NewVertex(root, 0))

	// While there are vertices on the queue to check
	for q.Len() > 0 {

		// Take the vertex off the queue
		v := q.Dequeue().(Vertex)

		// If the connections from vertex v would have too high a depth, then
		// skip the vertex
		if v.Depth+1 > maxDepth {
			continue
		}

		// Find all of the vertices adjacent to vertex v
		adjacentVertices, err := g.EntityIdsAdjacentTo(v.Identifier)
		if err != nil {
			return nil, err
		}

		// Walk through each adjacent vertex
		for _, adjacentIdentifier := range adjacentVertices.ToSlice() {

			// If the vertex has been seen before, then skip it
			if discovered.Has(adjacentIdentifier) {
				continue
			}

			// Record that the vertex has been seen
			discovered.Add(adjacentIdentifier)

			// Put a vertex onto the queue
			w := NewVertex(adjacentIdentifier, v.Depth+1)
			w.Parent = &v
			q.Enqueue(w)
		}
	}

	return discovered, nil
}
