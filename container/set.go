package container

import "sync"

type Set[T comparable] struct {
	data map[T]struct{}
}

func NewSet[T comparable]() *Set[T] {
	return &Set[T]{
		data: make(map[T]struct{}),
	}
}

func NewSetWithCapacity[T comparable](capacity int) *Set[T] {
	if capacity < 0 {
		capacity = 0
	}
	return &Set[T]{
		data: make(map[T]struct{}, capacity),
	}
}

func (s *Set[T]) Add(value T) {
	s.data[value] = struct{}{}
}

func (s *Set[T]) Remove(value T) {
	delete(s.data, value)
}

func (s *Set[T]) Contain(value T) bool {
	_, exist := s.data[value]
	return exist
}

func (s *Set[T]) Length() int {
	return len(s.data)
}

func (s *Set[T]) Reserve(capacity int) {
	if capacity < s.Length() {
		return
	}
	m := make(map[T]struct{}, capacity)
	for k, v := range s.data {
		m[k] = v
	}
	s.data = m
}

func (s *Set[T]) Range() map[T]struct{} {
	return s.data
}

func (s *Set[T]) ToSlice() []T{
	var result []T
	for k := range s.data{
		result = append(result, k)
	}
	return result
}

type SyncSet[T comparable] struct {
	m sync.Map
}

func NewSyncSet[T comparable]() *SyncSet[T] {
	return &SyncSet[T]{
		m: sync.Map{},
	}
}

func (s *SyncSet[T]) Add(value T) {
	s.m.Store(value, struct{}{})
}

func (s *SyncSet[T]) Remove(value T) {
	s.m.Delete(value)
}

func (s *SyncSet[T]) Contain(value T) bool {
	_, exist := s.m.Load(value)
	return exist
}
