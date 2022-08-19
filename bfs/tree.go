package bfs

import (
	"fmt"
)

// TreeNode represents a node in a tree data structure. A node can
// have zero or one parents and zero or more children.
//
// An example of a tree is:
//
//   a --> b --> c
//         |
//         ----> d
//
// Node 'b' has the parent 'a' and children 'c' and 'd'.
type TreeNode struct {
	name     string      // Name of the node from the graph
	parent   *TreeNode   // Parent of the node
	children []*TreeNode // Children of the node
	marked   bool        // Boolean flag to 'mark' the node
}

// NewTreeNode given the node's name and whether it is marked.
func NewTreeNode(name string, marked bool) *TreeNode {
	return &TreeNode{
		name:     name,
		parent:   nil,
		children: []*TreeNode{},
		marked:   marked,
	}
}

// MakeChild makes a child node in the tree.
func (t *TreeNode) MakeChild(name string, marked bool) (*TreeNode, error) {

	// Ensure the node is not in the lineage
	if t.ContainsParentNode(name) {
		return nil, fmt.Errorf("Lineage already contains %v", name)
	}

	// Make the new node
	node := NewTreeNode(name, marked)
	node.parent = t

	// Add the node
	t.children = append(t.children, node)

	// Return the newly created child node
	return node, nil
}

// Contains a parent node of a given name?
func (t *TreeNode) ContainsParentNode(name string) bool {

	p := t

	for p.parent != nil {
		if p.parent.name == name {
			return true
		}

		p = p.parent
	}

	return false
}

// Flatten the lineage of a child node.
func (t *TreeNode) Flatten() []string {

	lineage := []string{}

	p := t
	for p != nil {
		// Prepend the lineage
		lineage = append([]string{p.name}, lineage...)
		p = p.parent
	}

	return lineage
}

// FlattenAll the paths.
func FlattenAll(paths []*TreeNode) [][]string {
	flattened := [][]string{}

	for _, node := range paths {
		flattened = append(flattened, node.Flatten())
	}

	return flattened
}
