package kflow

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

type flowBuilder struct {
	name   nameType
	config *config

	nodes map[nameType]*metaNode
}

func NewFlowBuilder(name nameType) *flowBuilder {
	return &flowBuilder{
		name:   name,
		config: NewConfig(),
		nodes:  make(map[nameType]*metaNode),
	}
}

func (f *flowBuilder) SetConfig(config *config) *flowBuilder {
	if config == nil {
		return f
	}
	f.config = config
	return f
}

func (f *flowBuilder) Prepare() error {
	for _, node := range f.nodes {
		f.collectDependency(node)
	}

	return nil
}

func (f *flowBuilder) collectDependency(node *metaNode) {
	dependency := make(map[nameType]*metaDependency)
	for _, field := range node.inFields {
		fieldType := field.fieldType
		if fieldType != fieldTypeAuto && fieldType != fieldTypeDelay {
			continue
		}
		// get the dependency object
		dep, exist := dependency[*field.refNodeName]
		if !exist {
			dependency[*field.refNodeName] = &metaDependency{
				nodeName:   *field.refNodeName,
				fieldsName: []nameType{},
				missing:    true,
			}
			dep = dependency[*field.refNodeName]
		}

		// get the dependent node, if not exist, just save the relied field name and keep the missing field true.
		// otherwise, make missing field to false and check if field exist.
		var fieldName nameType
		if field.refFieldName == nil {
			fieldName = defaultOutputFieldName
		} else {
			fieldName = *field.refFieldName
		}
		refNode, exist := f.nodes[dep.nodeName]
		if exist {
			dep.missing = false
			refField, exist := refNode.outFields[fieldName]
			if !exist {
				panic(fmt.Sprintf(errorMessage(f.config.errMsgMap, fieldIsMissing), fieldName, dep.nodeName))
			}
			if refField.typ != field.typ {
				panic(fmt.Sprintf(errorMessage(f.config.errMsgMap, typeMismatched), node.name, field.name, refNode.name, refField.name))
			}
		}
		dep.fieldsName = append(dep.fieldsName, fieldName)
	}
	node.dependency = dependency
}

func (f *flowBuilder) Build(ctx context.Context) *flow {
	return &flow{
		ctx:    ctx,
		builder: f,
	}
}

func (f *flowBuilder) getConfig() *config {
	return f.config
}

func (f *flowBuilder) addNodeConstructor(nodeType reflect.Type, ptrDepth int, constructor nodeConstructorWrapper) error {
	parsedNode := f.parseNodeConstructor(nodeType, constructor)
	parsedNode.isPtr = ptrDepth != 0
	parsedNode.ptrDepth = ptrDepth
	if existedNode, exist := f.nodes[parsedNode.name]; exist {
		panic(fmt.Sprintf(errorMessage(f.config.errMsgMap, nodeAlreadyExist), parsedNode.name, existedNode.nodeName, parsedNode.nodeName))
	}
	f.nodes[parsedNode.name] = parsedNode
	return nil
}

func (f *flowBuilder) parseNodeConstructor(nodeType reflect.Type, constructor nodeConstructorWrapper) *metaNode {
	nodeName := nameType(nodeType.String())
	name := nameType(nodeType.Name())
	inst := constructor(context.Background())
	if namedNode, ok := inst.(iCustomName); ok {
		name = nameType(namedNode.Name())
	}

	outFields := make(map[nameType]*metaField, 0)
	inFields := make(map[nameType]*metaField, 0)
	for i := 0; i < nodeType.NumField(); i++ {
		fieldType := nodeType.Field(i)
		if _, ok := fieldType.Tag.Lookup("k"); !ok {
			continue
		}
		var fields map[nameType]*metaField
		field := f.parseField(string(name), fieldType)
		switch field.fieldType {
		case fieldTypeAuto, fieldTypeDelay, fieldTypeInput:
			fields = inFields
		case fieldTypeOutput:
			fields = outFields
		default:
			panic(errorMessage(f.config.errMsgMap, frameworkPanic))
		}
		if existedField, exist := fields[field.name]; exist {
			panic(fmt.Sprintf(errorMessage(f.config.errMsgMap, fieldAlreadyExist), field.name, name, existedField.fieldName, field.fieldName))
		}
		fields[field.name] = field
	}

	node := &metaNode{
		name:        name,
		nodeName:    nodeName,
		typ:         nodeType,
		inFields:    inFields,
		outFields:   outFields,
		constructor: constructor,
	}

	return node
}

func (f *flowBuilder) parseField(nodeName string, fieldType reflect.StructField) *metaField {
	fieldName := fieldType.Name

	if !fieldType.IsExported() {
		panic(fmt.Sprintf(errorMessage(f.config.errMsgMap, fieldIsNotExported), nodeName, fieldName))
	}

	tagString := fieldType.Tag.Get("k")
	field := &metaField{
		fieldName: nameType(fieldName),
		typ:       fieldType.Type,
		tagString: tagString,
	}

	f.parseTag(nodeName, field)

	return field
}

// parseTag handles the tags, for example:
//   - `k:"type: auto; name: node1.field1"`
//   - `k:"type: delay; name: node1.field1"`
//   - `k:"type: auto`
func (f *flowBuilder) parseTag(nodeName string, field *metaField) {
	items := strings.Split(field.tagString, ";")
	allTags := make(map[string]string)
	for _, item := range items {
		parts := strings.SplitN(item, ":", 2)
		if len(parts) != 2 {
			panic(fmt.Sprintf(errorMessage(f.config.errMsgMap, invalidTag), nodeName, field.fieldName, field.tagString))
		}
		allTags[strings.Trim(parts[0], " ")] = strings.Trim(parts[1], " ")
	}

	tagType := allTags["type"]
	if tagType == "" {
		panic(fmt.Sprintf(errorMessage(f.config.errMsgMap, missingTypeInTag), nodeName, field.fieldName))
	}
	switch tagType {
	case "auto":
		f.fillTagForAuto(field, allTags)
	case "input":
		f.fillTagForInput(field, allTags)
	case "delay":
		f.fillTagForDelay(field, allTags)
	case "output":
		f.fillTagForOutput(field, allTags)
	default:
		panic(fmt.Sprintf(errorMessage(f.config.errMsgMap, unsupportedTagType), nodeName, field.fieldName, tagType))
	}
}

func (f *flowBuilder) fillTagForAuto(field *metaField, allTags map[string]string) {
	field.fieldType = fieldTypeAuto
	field.name = field.fieldName
	if name, exist := allTags["name"]; exist {
		parts := strings.SplitN(name, ".", 2)
		if len(parts) == 1 {
			field.refNodeName = toPtr(nameType(parts[0]))
		} else {
			field.refNodeName = toPtr(nameType(parts[0]))
			field.refFieldName = toPtr(nameType(parts[1]))
		}
	} else {
		field.refNodeName = toPtr(field.fieldName)
	}
}

func (f *flowBuilder) fillTagForInput(field *metaField, allTags map[string]string) {
	field.fieldType = fieldTypeInput
	field.name = nameType(allTags["name"])
	if field.name == "" {
		field.name = nameType(field.fieldName)
	}
}

func (f *flowBuilder) fillTagForDelay(field *metaField, allTags map[string]string) {
	field.fieldType = fieldTypeDelay
	field.name = field.fieldName
	if name, exist := allTags["name"]; exist {
		parts := strings.SplitN(name, ".", 2)
		if len(parts) == 1 {
			field.refNodeName = toPtr(nameType(parts[0]))
		} else {
			field.refNodeName = toPtr(nameType(parts[0]))
			field.refFieldName = toPtr(nameType(parts[1]))
		}
	} else {
		field.refNodeName = toPtr(field.fieldName)
	}
}

func (f *flowBuilder) fillTagForOutput(field *metaField, allTags map[string]string) {
	field.fieldType = fieldTypeOutput
	field.name = nameType(allTags["name"])
	if field.name == "" {
		field.name = defaultOutputFieldName
	}
}
