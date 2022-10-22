package bfs

// A Vertex within the graph used for shortest path analysis.
type Vertex struct {
	Identifier string  // Unique identifier for the vertex
	Depth      int     // Distance of the vertex from the root vertex
	Parent     *Vertex // Parent vertex
}

// NewVertex within the graph.
func NewVertex(identifier string, depth int) Vertex {
	return Vertex{
		Identifier: identifier,
		Depth:      depth,
		Parent:     nil,
	}
}
