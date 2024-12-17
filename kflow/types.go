package kflow

import (
	"context"
	"reflect"
)

type nameType string

type fieldType int

type nodeConstructor[T INode] func(ctx context.Context) T
type nodeConstructorWrapper func(ctx context.Context) INode

type iAddable interface {
	addNodeConstructor(nodeType reflect.Type, ptrDepth int, constructor nodeConstructorWrapper) error
	getConfig() *config
}

type iCustomName interface {
	Name() string
}

type INode interface {
	Run() error
}

type IBefore interface {
	Before() error
}

type IAfter interface {
	After() error
}

type metaDependency struct {
	nodeName   nameType
	fieldsName []nameType
	missing    bool
}

type metaField struct {
	name         nameType     // the name of the field assigned by the tag, it needs to be unique in one node
	fieldName    nameType     // the name of the field which will not be influenced by the tag
	typ          reflect.Type // the type of the field
	tagString    string       // the original string of the tag
	fieldType    fieldType    // the type of the field
	refNodeName  *nameType    // the name of the referenced node
	refFieldName *nameType    // the name of the referenced field name
}

type metaNode struct {
	name        nameType                     // the name of the node without path, it needs to be unique all around
	nodeName    nameType                     // the name of the field which will not be influenced by the Name()
	typ         reflect.Type                 // the type of the node
	isPtr       bool                         // if the type is of ptr
	ptrDepth    int                          // the depth from the ptr to the underline data
	outFields   map[nameType]*metaField      // all the field used as the output
	inFields    map[nameType]*metaField      // all the field used as the input
	constructor nodeConstructorWrapper       // the constructor to generate a new node
	dependency  map[nameType]*metaDependency // the directly dependent nodes
}
