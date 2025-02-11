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
