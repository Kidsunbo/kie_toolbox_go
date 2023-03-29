package container

import (
	"fmt"
)

type Vector[T any] struct {
	data []T
}

func newVector[T any](data []T) Vector[T] {
	return Vector[T]{data: data}
}

func NewVector[T any]() Vector[T] {
	return newVector(make([]T, 0))
}

func NewVectorWithCapacity[T any](capacity int) Vector[T] {
	return newVector(make([]T, 0, capacity))
}

func NewVectorWithSize[T any](size int) Vector[T] {
	return newVector(make([]T, size))
}

func NewVectorFromSlice[T any](data []T) Vector[T] {
	if data == nil {
		return NewVector[T]()
	}
	d := make([]T, 0, len(data))
	return newVector(append(d, data...))
}

func NewVectorWithDefaultValue[T any](defaultValue T, size int) Vector[T] {
	data := make([]T, size)
	for i := range data {
		data[i] = defaultValue
	}
	return newVector(data)
}

func NewVectorWithOtherVector[T any](vector Vector[T]) Vector[T] {
	return NewVectorFromSlice(vector.data)
}

func (v *Vector[T]) Length() int {
	return len(v.data)
}

func (v *Vector[T]) Capacity() int {
	return cap(v.data)
}

func (v *Vector[T]) At(index int) T {
	return v.data[index]
}

func (v *Vector[T]) Front() T {
	return v.data[0]
}

func (v *Vector[T]) Back() T {
	return v.data[v.Length()-1]
}

func (v *Vector[T]) Data() []T {
	return v.data
}

func (v *Vector[T]) Empty() bool {
	return len(v.data) == 0
}

func (v *Vector[T]) Reserve(capacity int) {
	if capacity < v.Length() {
		return
	}
	newData := make([]T, v.Length(), capacity)
	for k, value := range v.data {
		newData[k] = value
	}
	v.data = newData
}

func (v *Vector[T]) ShrinkToFit() {
	v.Reserve(v.Length())
}

func (v *Vector[T]) Clear() {
	v.data = make([]T, 0)
}

func (v *Vector[T]) Append(value T) {
	v.data = append(v.data, value)
}

func (v *Vector[T]) Prepend(value T) {
	v.data = append(v.data, value)
	copy(v.data[1:], v.data)
	v.data[0] = value
}

func (v *Vector[T]) Insert(index int, value T) {
	v.data = append(v.data, value)
	copy(v.data[index+1:], v.data[index:])
	v.data[index] = value
}

func (v *Vector[T]) PopBack() T {
	if v.Length() == 0 {
		panic("length of vector is zero")
	}
	t := v.data[v.Length()-1]
	v.data = v.data[:v.Length()-1]
	return t
}

func (v *Vector[T]) PopFront() T {
	if v.Length() == 0 {
		panic("length of vector is zero")
	}
	t := v.data[0]
	v.data = v.data[1:]
	return t
}

func (v *Vector[T]) Remove(index int) T {
	t := v.data[index]
	v.data = append(v.data[:index], v.data[index+1:]...)
	return t
}

func (v *Vector[T]) String() string {
	return fmt.Sprintf("%v", v.data)
}

func (v *Vector[T]) Range() []T {
	return v.data
}

func (v *Vector[T]) ForEach(f func(i int, v T) bool) {
	for i, value := range v.data {
		if !f(i, value) {
			return
		}
	}
}
