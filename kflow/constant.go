package kflow

const (
	// fieldTypeAuto field type means that the dependency will be injected automatically
	fieldTypeAuto fieldType = iota

	// fieldTypeInput field type means that the dependency should be given in the configuration files
	fieldTypeInput

	// fieldTypeDelay field type means that the dependency is injected later by a manually call
	fieldTypeDelay

	// fieldTypeOutput field hold the result that one node produce
	fieldTypeOutput
)

const defaultOutputFieldName nameType = "default"
