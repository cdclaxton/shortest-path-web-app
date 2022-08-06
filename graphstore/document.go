package graphstore

import (
	"fmt"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/set"
)

// A Document represents a source of information about entities.
type Document struct {
	Id              string            // Unique document ID
	DocumentType    string            // Document type
	Attributes      map[string]string // Document attributes (e.g. date)
	LinkedEntityIds set.Set[string]   // IDs of entities to which the document is connected
}

// NewDocument with a given identifier, type and attributes
func NewDocument(identifier string, documentType string, attributes map[string]string) (Document, error) {

	// Preconditions
	if len(strings.TrimSpace(identifier)) == 0 {
		return Document{}, fmt.Errorf("Document identifier is blank")
	}

	if len(strings.TrimSpace(documentType)) == 0 {
		return Document{}, fmt.Errorf("Document type is blank")
	}

	// Build and return the document
	return Document{
		Id:              identifier,
		DocumentType:    documentType,
		Attributes:      attributes,
		LinkedEntityIds: set.NewSet[string](),
	}, nil
}

// AddEntity linked to the document.
func (d *Document) AddEntity(id string) {
	d.LinkedEntityIds.Add(id)
}

func (d *Document) HasEntity(id string) bool {
	return d.LinkedEntityIds.Has(id)
}

func (e *Document) Equal(other *Document) bool {

	// Check the unique identifier
	if e.Id != other.Id {
		return false
	}

	// Check the document type
	if e.DocumentType != other.DocumentType {
		return false
	}

	// Check the attributes
	if !attributesEqual(e.Attributes, other.Attributes) {
		return false
	}

	// Check the linked entities
	if !e.LinkedEntityIds.Equal(&other.LinkedEntityIds) {
		return false
	}

	return true
}
