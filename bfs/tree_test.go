package bfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestTree uses the tree structure:
//
//   a --> b --> c
//         |
//         ----> d
func TestTree(t *testing.T) {

	a := NewTreeNode("a", false)

	b, err := a.MakeChild("b", false)
	assert.NoError(t, err)

	c, err := b.MakeChild("c", false)
	assert.NoError(t, err)

	d, err := b.MakeChild("d", false)
	assert.NoError(t, err)

	assert.False(t, a.ContainsParentNode("a"))
	assert.False(t, a.ContainsParentNode("b"))
	assert.False(t, a.ContainsParentNode("c"))
	assert.False(t, a.ContainsParentNode("d"))

	assert.True(t, b.ContainsParentNode("a"))
	assert.False(t, b.ContainsParentNode("b"))
	assert.False(t, b.ContainsParentNode("c"))
	assert.False(t, b.ContainsParentNode("d"))

	assert.True(t, c.ContainsParentNode("a"))
	assert.True(t, c.ContainsParentNode("b"))
	assert.False(t, c.ContainsParentNode("c"))
	assert.False(t, c.ContainsParentNode("d"))

	assert.True(t, d.ContainsParentNode("a"))
	assert.True(t, d.ContainsParentNode("b"))
	assert.False(t, d.ContainsParentNode("c"))
	assert.False(t, d.ContainsParentNode("d"))

	assert.Equal(t, []string{"a"}, a.Flatten())
	assert.Equal(t, []string{"a", "b"}, b.Flatten())
	assert.Equal(t, []string{"a", "b", "c"}, c.Flatten())
	assert.Equal(t, []string{"a", "b", "d"}, d.Flatten())
}
