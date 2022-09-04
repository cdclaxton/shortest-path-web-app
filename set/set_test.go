package set

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmptySet(t *testing.T) {
	s := NewSet[int]()
	assert.Equal(t, 0, s.Len())

	s2 := NewSet[string]()
	assert.Equal(t, 0, s2.Len())
}

func TestAddSingleElement(t *testing.T) {
	s := NewSet[int]()
	s.Add(2)
	assert.Equal(t, 1, s.Len())
	assert.True(t, s.Has(2))
	assert.False(t, s.Has(1))
}

func TestEqual(t *testing.T) {
	s1 := NewSet[int]()
	s1.AddAll([]int{1, 2})

	s2 := NewSet[int]()
	s2.AddAll([]int{1, 2})

	s3 := NewSet[int]()
	s3.AddAll([]int{1, 2, 3})

	s4 := NewSet[int]()
	s4.AddAll([]int{2, 3})

	assert.True(t, s1.Equal(s2))
	assert.False(t, s1.Equal(s3))
	assert.False(t, s1.Equal(s4))
}

func TestIntersection(t *testing.T) {
	s1 := NewSet[int]()
	s1.AddAll([]int{1, 2, 3})
	assert.Equal(t, 3, s1.Len())

	s2 := NewSet[int]()
	s2.AddAll([]int{2, 3, 4, 5})
	assert.Equal(t, 4, s2.Len())

	s3 := s1.Intersection(s2)
	assert.Equal(t, 2, s3.Len())
}

func TestString(t *testing.T) {
	s1 := NewSet[string]()
	s1.AddAll([]string{"A", "B", "C"})

	assert.True(t, s1.Has("A"))
	assert.True(t, s1.Has("B"))
	assert.True(t, s1.Has("C"))
	assert.False(t, s1.Has("D"))

	s2 := NewSet[string]()
	s2.AddAll([]string{"A", "B", "C"})
	assert.True(t, s1.Equal(s2))

	s3 := NewSet[string]()
	s3.AddAll([]string{"A", "B", "D", "E"})
	assert.False(t, s1.Equal(s3))

	s1_3 := s1.Intersection(s3)
	expected := NewSet[string]()
	expected.AddAll([]string{"A", "B"})
	assert.True(t, s1_3.Equal(expected))
}

func TestToSlice(t *testing.T) {
	s := NewPopulatedSet("A", "B", "C")
	actual := s.ToSlice()
	sort.Strings(actual)
	expected := []string{"A", "B", "C"}
	assert.Equal(t, expected, actual)
}

func TestDifference(t *testing.T) {
	s1 := NewPopulatedSet("A", "B", "C", "D")
	s2 := NewPopulatedSet("B", "C", "D", "E", "F")
	s3 := NewPopulatedSet("A", "B", "C", "D")

	// Set difference s1\s2
	expected1 := NewPopulatedSet("A")
	assert.True(t, expected1.Equal(s1.Difference(s2)))

	// Set difference s2\s1
	expected2 := NewPopulatedSet("E", "F")
	assert.True(t, expected2.Equal(s2.Difference(s1)))

	// Set difference s3\s1
	assert.Equal(t, 0, s3.Difference(s1).Len())
	assert.Equal(t, 0, s1.Difference(s3).Len())
}
