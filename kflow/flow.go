package kflow

import (
	"context"
)

type flow struct {
	ctx     context.Context
	builder *flowBuilder
}

func (*flow) Run() error {
	panic("unimplement")
}
