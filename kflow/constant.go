package kflow

const (
	SkippedByFailedParent int = iota // node skipped because the dependence is failed
	SkippedByCondition               // node skipped because the condition is evaluated to false
)
