package kflow

type Node[T any] struct{}

func (n *Node[T]) ConditionalDependence(name string, when Condition[T], conditionDependence []string) *Dependence[T] {
	return &Dependence[T]{
		DependenceName:      name,
		Condition:           when,
		ConditionDependence: conditionDependence,
	}
}

func (n *Node[T]) StaticDependence(name string) *Dependence[T] {
	return &Dependence[T]{
		DependenceName:      name,
		Condition:           nil,
		ConditionDependence: nil,
	}
}
