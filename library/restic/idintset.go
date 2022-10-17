package restic

// Set implementation for IntID

// IntSet is a set of IntID(s).
type IntID int
type IntSet map[IntID]struct{}

// NewIntSet returns a new IntSet, populated with ids.
func NewIntSet(ids ...IntID) IntSet {
	m := make(IntSet)
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

// Has returns true iff id is contained in the set.
func (s IntSet) Has(id IntID) bool {
	_, ok := s[id]
	return ok
}

// Insert adds id to the set.
func (s IntSet) Insert(id IntID) {
	s[id] = struct{}{}
}

// Delete removes id from the set.
func (s IntSet) Delete(id IntID) {
	delete(s, id)
}

// Equals returns true iff s equals other.
func (s IntSet) Equals(other IntSet) bool {
	if len(s) != len(other) {
		return false
	}

	for id := range s {
		if _, ok := other[id]; !ok {
			return false
		}
	}

	// length + one-way comparison is sufficient implication of equality
	return true
}

// Merge adds the blobs in other to the current set.
func (s IntSet) Merge(other IntSet) {
	for id := range other {
		s.Insert(id)
	}
}

// Intersect returns a new set containing the IDs that are present in both sets.
func (s IntSet) Intersect(other IntSet) (result IntSet) {
	result = NewIntSet()

	set1 := s
	set2 := other

	// iterate over the smaller set
	if len(set2) < len(set1) {
		set1, set2 = set2, set1
	}

	for id := range set1 {
		if set2.Has(id) {
			result.Insert(id)
		}
	}
	return result
}

// Sub returns a new set containing all IDs that are present in s but not in
// other.
func (s IntSet) Sub(other IntSet) (result IntSet) {
	result = NewIntSet()
	for id := range s {
		if !other.Has(id) {
			result.Insert(id)
		}
	}
	return result
}
