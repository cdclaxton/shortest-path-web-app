package graphstore

import (
	"errors"
	"fmt"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/set"
)

// An Entity represents a specific instance of a person, vehicle, address, etc.
type Entity struct {
	Id                string            // Unique entity ID
	EntityType        string            // Entity type, e.g. address
	Attributes        map[string]string // Entity attributes
	LinkedDocumentIds *set.Set[string]  // IDs of documents to which the entity is connected
}

var (
	ErrEntityIdIsBlank       = errors.New("entity ID is blank")
	ErrEntityTypeIsBlank     = errors.New("entity type is blank")
	ErrEntityAttributesIsNil = errors.New("entity attributes is nil")
)

// ValidateEntityId to determine if the document ID passes minimum validation criteria.
func ValidateEntityId(entityId string) error {

	if len(strings.TrimSpace(entityId)) == 0 {
		return ErrEntityIdIsBlank
	}

	return nil
}

// ValidateEntityId to determine if the document ID passes minimum validation criteria.
func ValidateEntityType(entityType string) error {

	if len(strings.TrimSpace(entityType)) == 0 {
		return ErrEntityTypeIsBlank
	}

	return nil
}

// ValidateEntityId to determine if the document ID passes minimum validation criteria.
func ValidateEntityAttributes(attributes map[string]string) error {

	if attributes == nil {
		return ErrEntityAttributesIsNil
	}

	return nil
}

// NewEntity with a given identifier, type and attributes.
func NewEntity(identifier string, entityType string, attributes map[string]string) (Entity, error) {

	// Preconditions
	err := ValidateEntityId(identifier)
	if err != nil {
		return Entity{}, err
	}

	err = ValidateEntityType(entityType)
	if err != nil {
		return Entity{}, err
	}

	err = ValidateEntityAttributes(attributes)
	if err != nil {
		return Entity{}, err
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

// HasDocument returns true if the entity has a linked document with the given ID.
func (e *Entity) HasDocument(id string) bool {
	return e.LinkedDocumentIds.Has(id)
}

// Equal returns true if two entities are identical.
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
	if !e.LinkedDocumentIds.Equal(other.LinkedDocumentIds) {
		return false
	}

	return true
}

// String representation of an entity (for debugging purposes).
func (e *Entity) String() string {
	return fmt.Sprintf("Entity[id=%v, type=%v, attributes=%v, documents=%v]",
		e.Id, e.EntityType, e.Attributes, e.LinkedDocumentIds.String())
}

// IsValidEntityId returns true if the entity ID passes minimum validation criteria.
func IsValidEntityId(entityId string) bool {

	// Entity ID cannot be blank
	return len(strings.TrimSpace(entityId)) > 0
}
