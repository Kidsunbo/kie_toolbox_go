package kflow

type errorIndex int

const (
	pointerTooDeep errorIndex = iota
	interfaceAsReturnValueOfConstructor
	fieldIsNotExported
	invalidTag
	missingTypeInTag
	unsupportedTagType
	fieldAlreadyExist
	nodeAlreadyExist
	fieldIsMissing
	typeMismatched
	frameworkPanic
)

var chinese = map[errorIndex]string{
	pointerTooDeep:                      "指针层级不可超过%v层",
	interfaceAsReturnValueOfConstructor: "节点构造函数不可使用接口类型",
	fieldIsNotExported:                  "属性%v.%v对外不可见",
	invalidTag:                          "属性%v.%v具有不合法的标签(%v)",
	missingTypeInTag:                    "属性%v.%v在标签中缺失类型",
	unsupportedTagType:                  "属性%v.%v在标签中具有不支持的类型%v",
	fieldAlreadyExist:                   "字段%v已经存在于节点%v, 分别为%v和%v",
	nodeAlreadyExist:                    "节点%v已经存在, 分别为%v和%v",
	fieldIsMissing:                      "字段%v在节点%v中不存在",
	typeMismatched:                      "%v.%v的类型和已注册的字段%v.%v的类型不匹配",
	frameworkPanic:                      "框架出错了",
}

var english = map[errorIndex]string{
	pointerTooDeep:                      "the level of pointer should not be greater than %v",
	interfaceAsReturnValueOfConstructor: "should not use interface as the return value of the constructor",
	fieldIsNotExported:                  "%v.%v is not a public field",
	invalidTag:                          "field %v.%v has invalid tag (%v)",
	missingTypeInTag:                    "%v.%v misses type in tag",
	unsupportedTagType:                  "%v.%v has unsupported type %v in tag",
	fieldAlreadyExist:                   "field %v has already existed in node %v, field names are %v and %v",
	nodeAlreadyExist:                    "node %v has already existed, node type are %v and %v",
	fieldIsMissing:                      "there is no field named %v in node %v",
	typeMismatched:                      "the type of %v.%v doesn't match the one of registered field %v.%v",
	frameworkPanic:                      "the framework has panic",
}

func errorMessage(pack map[errorIndex]string, idx errorIndex) string {
	return pack[idx]
}
