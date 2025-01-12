package kflow

import "github.com/Kidsunbo/kie_toolbox_go/container"

type Engine[T any, R any, C any] struct {
	nodes *container.Dag[string, INode]
}
