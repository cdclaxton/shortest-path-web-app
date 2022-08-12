package bfs

// A vertex within the graph used for shortest path analysis.
type Vertex struct {
	Identifier string
	Depth      int
	Parent     *Vertex
}

// NewVertex within the graph.
func NewVertex(identifier string, depth int) Vertex {
	return Vertex{
		Identifier: identifier,
		Depth:      depth,
		Parent:     nil,
	}
}
