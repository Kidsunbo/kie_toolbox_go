package kflow

type config struct {
	errMsgMap map[errorIndex]string
}

func NewConfig()*config{
	return &config{
		errMsgMap: chinese,
	}
}

func (c *config) ReportErrorInEnglish() {
	c.errMsgMap = english
}

func (c *config) ReportErrorInChinese() {
	c.errMsgMap = chinese
}