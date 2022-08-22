package kiecommon

import "sync"

type Set[T comparable] struct{
	m map[T]struct{}
}


func NewSet[T comparable]() *Set[T]{
	return &Set[T]{
		m: make(map[T]struct{}),
	}
}

func NewSetWithCapacity[T comparable](capacity int) *Set[T]{
	if capacity < 0{
		capacity = 0
	}
	return &Set[T]{
		m: make(map[T]struct{}, capacity),
	}
}

func (s *Set[T]) Add(value T){
	s.m[value] = struct{}{}
}

func (s *Set[T]) Remove(value T){
	delete(s.m, value)
}

func (s *Set[T]) Contain(value T)bool{
	_, exist := s.m[value]
	return exist
}

func (s *Set[T]) Size()int{
	return len(s.m)
}

func (s *Set[T]) Reserve(capacity int){
	if capacity <= s.Size(){
		return
	}
	m := make(map[T]struct{}, capacity)
	for k,v := range s.m{
		m[k] = v
	}
	s.m = m
}

func (s *Set[T]) Range() map[T]struct{}{
	return s.m
}


type SyncSet[T comparable] struct{
	m sync.Map
}


func NewSyncSet[T comparable]() *SyncSet[T]{
	return &SyncSet[T]{
		m: sync.Map{},
	}
}

func (s *SyncSet[T]) Add(value T){
	s.m.Store(value, struct{}{})
}

func (s *SyncSet[T]) Remove(value T){
	s.m.Delete(value)
}

func (s *SyncSet[T]) Contain(value T)bool{
	_, exist := s.m.Load(value)
	return exist
}
