package util

type StringSet map[string]struct{}

func NewStringSet() StringSet {
	return make(map[string]struct{})
}

func NewStringSetWith(size int) StringSet {
	return make(map[string]struct{}, size)
}

func NewStringSetFrom(values ...string) StringSet {
	s := NewStringSet()
	s.MergeFromSlice(values)
	return s
}

func (s StringSet) Clear() {
	for k := range s {
		delete(s, k)
	}
}

func (s StringSet) Clone() StringSet {
	clone := NewStringSetWith(len(s))
	for k := range s {
		clone.Add(k)
	}
	return clone
}

func (s StringSet) Has(v string) bool {
	_, ok := s[v]
	return ok
}

func (s StringSet) Add(v string) {
	if _, ok := s[v]; !ok {
		s[v] = struct{}{}
	}
}

func (s StringSet) Merge(another StringSet) {
	for k := range another {
		s.Add(k)
	}
}

func (s StringSet) MergeFromSlice(values []string) {
	for _, v := range values {
		s.Add(v)
	}
}

func (s StringSet) EqualSlice(values []string) bool {
	if len(s) != len(values) {
		return false
	}
	for _, v := range values {
		if !s.Has(v) {
			return false
		}
	}
	return true
}

func (s StringSet) ToSlice() []string {
	keys := make([]string, len(s), len(s))
	i := 0
	for e := range s {
		keys[i] = e
		i++
	}
	return keys
}

func (s StringSet) NotIn(another StringSet) (notIns []string) {
	for k := range s {
		if _, ok := another[k]; !ok {
			notIns = append(notIns, k)
		}
	}
	return notIns
}

type Int64Set map[int64]struct{}

func NewInt64Set() Int64Set {
	return make(map[int64]struct{})
}

func NewInt64SetWith(size int) Int64Set {
	return make(map[int64]struct{}, size)
}

func NewInt64SetFrom(values ...int64) Int64Set {
	s := NewInt64Set()
	s.MergeFromSlice(values)
	return s
}

func (s Int64Set) Clear() {
	for k := range s {
		delete(s, k)
	}
}

func (s Int64Set) Clone() Int64Set {
	clone := NewInt64SetWith(len(s))
	for k := range s {
		clone.Add(k)
	}
	return clone
}

func (s Int64Set) Has(val int64) bool {
	_, ok := s[val]
	return ok
}

func (s Int64Set) Add(newItem int64) {
	if _, ok := s[newItem]; !ok {
		s[newItem] = struct{}{}
	}
}

func (s Int64Set) Merge(another Int64Set) {
	for k := range another {
		s.Add(k)
	}
}

func (s Int64Set) MergeFromSlice(values []int64) {
	for _, v := range values {
		s.Add(v)
	}
}

func (s Int64Set) EqualSlice(values []int64) bool {
	if len(s) != len(values) {
		return false
	}
	for _, v := range values {
		if !s.Has(v) {
			return false
		}
	}
	return true
}

func (s Int64Set) ToSlice() []int64 {
	keys := make([]int64, len(s), len(s))
	i := 0
	for e := range s {
		keys[i] = e
		i++
	}
	return keys
}

func (s Int64Set) NotIn(another Int64Set) (notIns []int64) {
	for k := range s {
		if _, ok := another[k]; !ok {
			notIns = append(notIns, k)
		}
	}
	return notIns
}
