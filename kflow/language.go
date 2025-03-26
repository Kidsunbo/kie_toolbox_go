package kflow

const (
	chinese int8 = iota
	english
)

type languagePack struct {
	Chinese string
	English string
}

type textIndexType int32

const (
	cycleDetectedError textIndexType = iota
	notPreparedError
	nodeNotExist
	nodeHasFailedDependence
	underlineNodeHasExecuted
	conditionEvaludateToFalse
	unsupportedNodeType
	nodeTimeoutError
	operationNotSupportedInParallel
	noTargetToRun
)

var msg = map[textIndexType]languagePack{
	cycleDetectedError:              {Chinese: "检测到环存在, %v", English: "cycles detected, %v"},
	notPreparedError:                {Chinese: "调用前未准备, 请先调用Prepare方法", English: "Not prepare before run, please call Prepare method before"},
	nodeNotExist:                    {Chinese: "节点[%v]不存在", English: "node [%v] does not exist"},
	nodeHasFailedDependence:         {Chinese: "节点[%v]存在执行失败的依赖节点[%v]", English: "node [%v] has failed dependent node [%v]"},
	underlineNodeHasExecuted:        {Chinese: "底层节点[%v]已经执行完成", English: "underline node [%v] has already been executed"},
	conditionEvaludateToFalse:       {Chinese: "节点[%v]要求的条件不满足", English: "the condition result doesn't meet the requirement of node [%v]"},
	unsupportedNodeType:             {Chinese: "节点[%v]的类型不支持", English: "the type of node [%v] isn't supported"},
	nodeTimeoutError:                {Chinese: "节点运行超时", English: "node exeuctes timeout"},
	operationNotSupportedInParallel: {Chinese: "操作不支持在并行环境中运行", English: "operation is not allowed in parallel"},
	noTargetToRun:                   {Chinese: "没有目标节点可以运行", English: "no target node to run"},
}

func message(lang int8, text textIndexType) string {
	switch lang {
	case chinese:
		return msg[text].Chinese
	case english:
		return msg[text].English
	default:
		return "error: unknown language"
	}
}
