package graphstore

import (
	"fmt"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/set"
)

// An Entity represents a specific instance of a person, vehicle, address, etc.
type Entity struct {
	Id                string            // Unique entity ID
	EntityType        string            // Entity type, e.g. address
	Attributes        map[string]string // Entity attributes
	LinkedDocumentIds set.Set[string]   // IDs of documents to which the entity is connected
}

// NewEntity with a given identifier, type and attributes.
func NewEntity(identifier string, entityType string, attributes map[string]string) (Entity, error) {

	// Preconditions
	if len(strings.TrimSpace(identifier)) == 0 {
		return Entity{}, fmt.Errorf("Entity identifier is blank")
	}

	if len(strings.TrimSpace(entityType)) == 0 {
		return Entity{}, fmt.Errorf("Entity type is blank")
	}

	// Build and return the entity
	return Entity{
		Id:                identifier,
		EntityType:        entityType,
		Attributes:        attributes,
		LinkedDocumentIds: set.NewSet[string](),
	}, nil
}

// AddDocument linked to the entity.
func (e *Entity) AddDocument(id string) {
	e.LinkedDocumentIds.Add(id)
}

func (e *Entity) HasDocument(id string) bool {
	return e.LinkedDocumentIds.Has(id)
}

func (e *Entity) Equal(other *Entity) bool {

	// Check the unique identifier
	if e.Id != other.Id {
		return false
	}

	// Check the entity type
	if e.EntityType != other.EntityType {
		return false
	}

	// Check the attributes
	if !attributesEqual(e.Attributes, other.Attributes) {
		return false
	}

	// Check the linked documents
	if !e.LinkedDocumentIds.Equal(&other.LinkedDocumentIds) {
		return false
	}

	return true
}

func (e *Entity) String() string {
	return fmt.Sprintf("Entity[id=%v, type=%v, attributes=%v, documents=%v]",
		e.Id, e.EntityType, e.Attributes, &e.LinkedDocumentIds)
}
