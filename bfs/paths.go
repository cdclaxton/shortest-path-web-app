package bfs

import (
	"fmt"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/golang-collections/collections/queue"
)

type Path struct {
	route []string // Route from the root to the goal vertex
}

func NewPath(route ...string) Path {
	return Path{
		route: route,
	}
}

// Equal returns true if two paths have the same route.
func (p Path) Equal(other Path) bool {
	if len(p.route) != len(other.route) {
		return false
	}

	for idx := range p.route {
		if p.route[idx] != other.route[idx] {
			return false
		}
	}

	return true
}

// PathsEqual returns true if the paths are identical, regardless of order.
func PathsEqual(p1 []Path, p2 []Path) bool {
	if len(p1) != len(p2) {
		return false
	}

	p2PathSeenInP1 := make([]bool, len(p1))

	for p1Index := range p1 {

		found := false
		for p2Index := range p2 {
			if p1[p1Index].Equal(p2[p2Index]) {
				found = true
				p2PathSeenInP1[p1Index] = true
				break
			}
		}

		if !found {
			return false
		}
	}

	for _, seen := range p2PathSeenInP1 {
		if !seen {
			return false
		}
	}

	return true
}

// treeNodesToPaths converts tree nodes to paths.
func treeNodesToPaths(nodes []*TreeNode) []Path {
	paths := []Path{}

	for _, node := range nodes {
		route := node.Flatten()
		paths = append(paths, NewPath(route...))
	}

	return paths
}

// AllPaths from a root vertex to a goal vertex up to a maximum depth.
//
// The function assumes that the root and goal vertices are present in the graph.
func AllPaths(graph graphstore.UnipartiteGraphStore, root string, goal string,
	maxDepth int) ([]Path, error) {

	// Preconditions
	if !graph.HasEntity(root) {
		return nil, fmt.Errorf("Root vertex not found: %v", root)
	}

	if !graph.HasEntity(goal) {
		return nil, fmt.Errorf("Goal vertex not found: %v", goal)
	}

	if maxDepth < 0 {
		return nil, fmt.Errorf("Invalid maximum depth: %v", maxDepth)
	}

	// Number of steps traversed from root vertex
	numSteps := 0

	// If the root is the goal, return without traversing the graph
	treeNode := NewTreeNode(root, root == goal)
	if treeNode.marked {
		return []Path{NewPath(root)}, nil
	}

	// Nodes to spider out from on the current iteration
	qCurrent := queue.New()
	qCurrent.Enqueue(treeNode)

	// Nodes to spider out from on the next iteration
	qNext := queue.New()

	// List of complete nodes, i.e. those where the goal has been found
	complete := []*TreeNode{}

	for numSteps < maxDepth {
		for qCurrent.Len() > 0 {

			// Take a tree node from the queue that represents a vertex
			node := qCurrent.Dequeue().(*TreeNode)

			// Check the node
			if node.marked {
				return nil, fmt.Errorf("Trying to traverse from a marked node: %v", node.name)
			}

			// Get the vertices adjacent to the node
			w, err := graph.EntityIdsAdjacentTo(node.name)
			if err != nil {
				return nil, err
			}

			// Walk through each of the adjacent vertices
			for _, adjIdentifier := range w.ToSlice() {

				// If the adjacent vertex is a new connection for the node,
				// then add it and check whether the goal has been reached
				if !node.ContainsParentNode(adjIdentifier) {
					child, err := node.MakeChild(adjIdentifier, adjIdentifier == goal)
					if err != nil {
						return nil, err
					}

					if child.marked {
						complete = append(complete, child)
					} else {
						qNext.Enqueue(child)
					}
				}
			}
		}

		qCurrent = qNext
		qNext = queue.New()
		numSteps++
	}

	// Flatten the paths
	return treeNodesToPaths(complete), nil
}
