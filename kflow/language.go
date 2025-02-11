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
)

var msg = map[textIndexType]languagePack{
	cycleDetectedError: {Chinese: "监测到环存在, %v", English: "cycles detected, %v"},
	notPreparedError:   {Chinese: "调用前未准备, 请先调用Prepare方法", English: "Not prepare before run, please call Prepare method before"},
	nodeNotExist:       {Chinese: "节点[%v]不存在", English: "node [%v] does not exist"},
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
