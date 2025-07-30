package kflow

type Node[T any] struct{}

func (n *Node[T]) ConditionalDependency(name string, when Condition[T], conditionDependencies []string) *Dependency[T] {
	return &Dependency[T]{
		DependencyName:        name,
		Condition:             when,
		ConditionDependencies: conditionDependencies,
	}
}

func (n *Node[T]) StaticDependency(name string) *Dependency[T] {
	return &Dependency[T]{
		DependencyName:        name,
		Condition:             nil,
		ConditionDependencies: nil,
	}
}
