package set

import (
	"fmt"
	"strings"
)

// Set of type T.
type Set[T comparable] struct {
	values map[T]bool
}

// NewSet of a given type T.
func NewSet[T comparable]() Set[T] {
	return Set[T]{
		values: map[T]bool{},
	}
}

// Add an element to the set.
func (s *Set[T]) Add(element T) {
	s.values[element] = true
}

// AddAll of the elements to the set.
func (s *Set[T]) AddAll(elements []T) {
	for _, element := range elements {
		s.Add(element)
	}
}

// Has the set got a specific element?
func (s *Set[T]) Has(element T) bool {
	return s.values[element]
}

// Intersection of the set with another set.
func (s *Set[T]) Intersection(s2 *Set[T]) Set[T] {
	common := NewSet[T]()

	for key := range s.values {
		_, found := s2.values[key]
		if found {
			common.Add(key)
		}
	}

	return common
}

// Length (cardinality) of the set.
func (s *Set[T]) Len() int {
	return len(s.values)
}

// Does the set have the same elements as the other set?
func (s *Set[T]) Equal(other *Set[T]) bool {
	if s.Len() != other.Len() {
		return false
	}

	for key := range s.values {
		if !other.Has(key) {
			return false
		}
	}

	return true
}

func (s *Set[T]) String() string {
	values := []string{}
	for key := range s.values {
		values = append(values, fmt.Sprint(key))
	}

	return fmt.Sprintf("{%v}", strings.Join(values, ","))
}
