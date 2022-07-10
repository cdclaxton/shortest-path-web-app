package graphstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildDocument(t *testing.T) {
	doc, err := NewDocument("doc-1", "bank details", map[string]string{"name": "Bank of Acorns"})
	assert.NoError(t, err)
	assert.Equal(t, "doc-1", doc.Id)
	assert.Equal(t, "bank details", doc.DocumentType)
	assert.Equal(t, map[string]string{"name": "Bank of Acorns"}, doc.Attributes)

	// Add an entity
	assert.Equal(t, 0, doc.LinkedEntityIds.Len())
	doc.AddEntity("e-1")
	assert.Equal(t, 1, doc.LinkedEntityIds.Len())
}

func TestBuildInvalidDocument(t *testing.T) {
	// Blank identifier
	_, err := NewDocument("     ", "bank details", map[string]string{"name": "Bank of Acorns"})
	assert.Error(t, err)

	// Blank type
	_, err = NewDocument("doc-1", " ", map[string]string{"name": "Bank of Acorns"})
	assert.Error(t, err)
}

func TestDocumentEquality(t *testing.T) {
	doc1, err := NewDocument("doc-1", "bank details", map[string]string{"name": "Bank of Acorns"})
	assert.NoError(t, err)

	// Same document
	doc2, err := NewDocument("doc-1", "bank details", map[string]string{"name": "Bank of Acorns"})
	assert.NoError(t, err)
	assert.True(t, doc1.Equal(&doc2))

	// Different identifier
	doc3, err := NewDocument("doc-2", "bank details", map[string]string{"name": "Bank of Acorns"})
	assert.NoError(t, err)
	assert.False(t, doc1.Equal(&doc3))

	// Different document type
	doc4, err := NewDocument("doc-1", "source", map[string]string{"name": "Bank of Acorns"})
	assert.NoError(t, err)
	assert.False(t, doc1.Equal(&doc4))

	// Different attributes
	doc5, err := NewDocument("doc-1", "bank details", map[string]string{"name": "Bank of Acorns", "date": "2022-07-10"})
	assert.NoError(t, err)
	assert.False(t, doc1.Equal(&doc5))

	// Add a linked entity
	doc1.AddEntity("e-1")
	assert.False(t, doc1.Equal(&doc2))

	doc2.AddEntity("e-1")
	assert.True(t, doc1.Equal(&doc2))
}
