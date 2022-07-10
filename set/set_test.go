package set

import (
	"fmt"
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

	assert.True(t, s1.Equal(&s2))
	assert.False(t, s1.Equal(&s3))
	assert.False(t, s1.Equal(&s4))
}

func TestIntersection(t *testing.T) {
	s1 := NewSet[int]()
	s1.AddAll([]int{1, 2, 3})
	assert.Equal(t, 3, s1.Len())

	s2 := NewSet[int]()
	s2.AddAll([]int{2, 3, 4, 5})
	assert.Equal(t, 4, s2.Len())

	s3 := s1.Intersection(&s2)
	assert.Equal(t, 2, s3.Len())
}

func TestString(t *testing.T) {
	s1 := NewSet[int]()
	s1.AddAll([]int{1, 2, 3})
	fmt.Println(s1)
	assert.True(t, false)
}
