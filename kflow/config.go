package kflow

type flag struct{ id int }

var ReportInEnglish = flag{id: 1}   // Report error and message in english
var ReportInChinese = flag{id: 2}   // Report error and message in chinese

type config struct {
	Language          int8
}
