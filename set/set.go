package set

import (
	"fmt"
	"strings"
)

// Set of type T.
// Values has to be exported to the serialised by the gob library.
type Set[T comparable] struct {
	Values map[T]bool
}

// NewSet of a given type T.
func NewSet[T comparable]() *Set[T] {
	return &Set[T]{
		Values: map[T]bool{},
	}
}

// NewPopulatedSet of type T.
func NewPopulatedSet[T comparable](elements ...T) *Set[T] {
	s := NewSet[T]()
	s.AddAll(elements)
	return s
}

// Add an element to the set.
func (s *Set[T]) Add(element T) {
	s.Values[element] = true
}

// AddAll of the elements to the set.
func (s *Set[T]) AddAll(elements []T) {
	for _, element := range elements {
		s.Add(element)
	}
}

func (s *Set[T]) Remove(element T) {
	delete(s.Values, element)
}

// Has the set got a specific element?
func (s *Set[T]) Has(element T) bool {
	return s.Values[element]
}

// Intersection of the set with another set.
func (s *Set[T]) Intersection(s2 *Set[T]) *Set[T] {
	common := NewSet[T]()

	for key := range s.Values {
		_, found := s2.Values[key]
		if found {
			common.Add(key)
		}
	}

	return common
}

// Difference is defined as those in the set, but not in the other s2.
func (s *Set[T]) Difference(s2 *Set[T]) *Set[T] {

	diff := NewSet[T]()

	for key := range s.Values {
		if !s2.Has(key) {
			diff.Add(key)
		}
	}

	return diff
}

// Union of two sets.
func (s *Set[T]) Union(s2 *Set[T]) *Set[T] {

	union := NewSet[T]()

	for key := range s.Values {
		union.Add(key)
	}

	for key := range s2.Values {
		union.Add(key)
	}

	return union
}

// Length (cardinality) of the set.
func (s *Set[T]) Len() int {
	return len(s.Values)
}

// Does the set have the same elements as the other set?
func (s *Set[T]) Equal(other *Set[T]) bool {
	if s.Len() != other.Len() {
		return false
	}

	for key := range s.Values {
		if !other.Has(key) {
			return false
		}
	}

	return true
}

// String representation of the set.
func (s *Set[T]) String() string {
	values := []string{}
	for key := range s.Values {
		values = append(values, fmt.Sprint(key))
	}

	return fmt.Sprintf("{%v}", strings.Join(values, ","))
}

// ToSlice converts the set to a slice. The usage of this function is sub-optimal
// due to the conversion. This could be improved by changing the implementation to
// an iterator.
func (s *Set[T]) ToSlice() []T {
	ret := make([]T, len(s.Values), len(s.Values))

	i := 0
	for key := range s.Values {
		ret[i] = key
		i += 1
	}

	return ret
}
