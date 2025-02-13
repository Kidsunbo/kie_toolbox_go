package kflow

import (
	"fmt"
)

func contains[K comparable, T any](m map[K]T, key K) bool {
	_, exist := m[key]
	return exist
}

func safeRun(f func() error) (err error, isPanic bool) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("panic: %v", e)
			isPanic = true
		}
	}()

	err = f()
	return
}

func ofType[T any](value any) bool {
	_, ok := value.(T)
	return ok
}

func keys[K comparable, T any](m map[K]T) []K {
	result := make([]K, 0, len(m))
	for key := range m {
		result = append(result, key)
	}
	return result
}

func values[K comparable, T any](m map[K]T) []T {
	result := make([]T, 0, len(m))
	for _, value := range m {
		result = append(result, value)
	}
	return result
}
