package kflow

type flag struct{ id int }

var StaticNode = flag{id: 1}      // The nodes will never changed in parallel and will prepare only once.
var ReportInEnglish = flag{id: 2} // Report error and message in english
var ReportInChinese = flag{id: 3} // Report error and message in chinese

type config struct {
	Static   bool
	Language int8
}
