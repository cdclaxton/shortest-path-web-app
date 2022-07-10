package graphstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildEntity(t *testing.T) {
	e, err := NewEntity("id-1", "person", map[string]string{"name": "Bob"})
	assert.NoError(t, err)
	assert.Equal(t, "id-1", e.Id)
	assert.Equal(t, "person", e.EntityType)
	assert.Equal(t, map[string]string{"name": "Bob"}, e.Attributes)
	assert.Equal(t, 0, e.LinkedDocumentIds.Len())

	assert.Equal(t, 0, e.LinkedDocumentIds.Len())
	e.AddDocument("doc-1")
	assert.Equal(t, 1, e.LinkedDocumentIds.Len())
}

func TestBuildInvalidEntity(t *testing.T) {
	// Blank identifier
	_, err := NewEntity(" ", "person", map[string]string{"name": "Bob"})
	assert.Error(t, err)

	// Blank entity type
	_, err = NewEntity("id-1", "", map[string]string{"name": "Bob"})
	assert.Error(t, err)
}

func TestEntityEquality(t *testing.T) {
	e1, err := NewEntity("id-1", "person", map[string]string{"name": "Bob"})
	assert.NoError(t, err)

	// Same entity
	e2, err := NewEntity("id-1", "person", map[string]string{"name": "Bob"})
	assert.NoError(t, err)
	assert.True(t, e1.Equal(&e2))

	// Different identifier
	e3, err := NewEntity("id-2", "person", map[string]string{"name": "Bob"})
	assert.NoError(t, err)
	assert.False(t, e1.Equal(&e3))

	// Different type
	e4, err := NewEntity("id-1", "address", map[string]string{"name": "Bob"})
	assert.NoError(t, err)
	assert.False(t, e1.Equal(&e4))

	// Different attributes
	e5, err := NewEntity("id-1", "person", map[string]string{"name": "Bob", "age": "23"})
	assert.NoError(t, err)
	assert.False(t, e1.Equal(&e5))

	// Add a linked document
	e1.AddDocument("doc-1")
	assert.False(t, e1.Equal(&e2))

	e2.AddDocument("doc-1")
	assert.True(t, e1.Equal(&e2))
}
