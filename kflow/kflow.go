package kflow

import (
	"context"
	"reflect"
)

func Add[T INode](f iAddable, constructor nodeConstructor[T]) error {
	var output T
	rawType := reflect.TypeOf(&output).Elem()
	nodeType, ptrDepth := deferenceToNonePtr(f.getConfig(), rawType)
	if nodeType.Kind() == reflect.Interface {
		panic(errorMessage(f.getConfig().errMsgMap, interfaceAsReturnValueOfConstructor))
	}

	constructorWrapper := func(ctx context.Context) INode {
		return constructor(ctx)
	}
	return f.addNodeConstructor(nodeType, ptrDepth, constructorWrapper)
}
