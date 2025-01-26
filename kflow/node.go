package kflow

type Node[T any] struct{}

func (n *Node[T]) ConditionDependence(name string, when Condition[T], conditionDependence []dependenceReturnType[T]) *dependence[T] {
	return &dependence[T]{
		DependenceName:      name,
		Condition:           when,
		ConditionDependence: conditionDependence,
	}
}

func (n *Node[T]) StaticDependence(name string) *dependence[T] {
	return &dependence[T]{
		DependenceName:      name,
		Condition:           nil,
		ConditionDependence: nil,
	}
}

type NodeBox[T any] struct {
	node       INode[T, dependenceReturnType[T]]
	conditions []Condition[T]
	nodeType   int64
}
