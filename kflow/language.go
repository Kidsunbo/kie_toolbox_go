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
)

var msg = map[textIndexType]languagePack{
	cycleDetectedError: {Chinese: "监测到环存在, %v", English: "cycles detected, %v"},
	notPreparedError:   {Chinese: "调用前未准备, 请先调用Prepare方法", English: "Not prepare before run, please call Prepare method before"},
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
