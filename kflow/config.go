package kflow

import "time"

type flag struct{ id int }

var ReportInEnglish = flag{id: 1} // Report error and message in english
var ReportInChinese = flag{id: 2} // Report error and message in chinese

type AsyncTimeout int64

type config struct {
	Name     string
	Language int8
	Timeout  time.Duration
}
