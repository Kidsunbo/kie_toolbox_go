package kflow

import (
	"fmt"
	"reflect"
)

func deferenceToNonePtr(config *config, t reflect.Type) (reflect.Type, int) {
	if t.Kind() != reflect.Pointer {
		return t, 0
	}
	i := 0
	for t.Kind() == reflect.Pointer {
		i++
		if i == 11 {
			panic(fmt.Sprintf(errorMessage(config.errMsgMap, pointerTooDeep), 10))
		}
		t = t.Elem()
	}
	return t, i
}

func toPtr[T any](t T) *T {
	return &t
}