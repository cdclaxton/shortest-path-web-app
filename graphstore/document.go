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
	Attributes      map[string]string // Document attributes (e.g. name, date)
	LinkedEntityIds *set.Set[string]  // IDs of entities to which the document is connected
}

var (
	ErrDocumentIdIsBlank       = fmt.Errorf("Document ID is blank")
	ErrDocumentTypeIsBlank     = fmt.Errorf("Document type is blank")
	ErrDocumentAttributesIsNil = fmt.Errorf("Document attributes is nil")
)

// ValidateDocumentId to determine if the document ID passes minimum validation criteria.
func ValidateDocumentId(documentId string) error {

	if len(strings.TrimSpace(documentId)) == 0 {
		return ErrDocumentIdIsBlank
	}

	return nil
}

// ValidateDocumentType to determine if the document type passes minimum validation criteria.
func ValidateDocumentType(documentType string) error {

	if len(strings.TrimSpace(documentType)) == 0 {
		return ErrDocumentTypeIsBlank
	}

	return nil
}

// ValidateDocumentAttributes to determine if the document attributes pass minimum validation criteria.
func ValidateDocumentAttributes(attributes map[string]string) error {

	if attributes == nil {
		return ErrDocumentAttributesIsNil
	}

	return nil
}

// NewDocument with a given identifier, type and attributes
func NewDocument(identifier string, documentType string, attributes map[string]string) (Document, error) {

	// Preconditions
	err := ValidateDocumentId(identifier)
	if err != nil {
		return Document{}, err
	}

	err = ValidateDocumentType(documentType)
	if err != nil {
		return Document{}, err
	}

	err = ValidateDocumentAttributes(attributes)
	if err != nil {
		return Document{}, err
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

// HasEntity returns true if the document has the linked entity ID.
func (d *Document) HasEntity(id string) bool {
	return d.LinkedEntityIds.Has(id)
}

// Equal returns true if two documents are identical.
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
	if !e.LinkedEntityIds.Equal(other.LinkedEntityIds) {
		return false
	}

	return true
}
