package container

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"unsafe"
)

func TestNewVector(t *testing.T) {
	vi := NewVector[int]()
	assert.Equal(t, len(vi.data), 0)
	assert.Equal(t, reflect.TypeOf(vi.data).Kind(), reflect.Slice)
	assert.Equal(t, reflect.TypeOf(vi.data).Elem().Kind(), reflect.Int)

	vs := NewVector[string]()
	assert.Equal(t, len(vs.data), 0)
	assert.Equal(t, reflect.TypeOf(vs.data).Kind(), reflect.Slice)
	assert.Equal(t, reflect.TypeOf(vs.data).Elem().Kind(), reflect.String)

	assert.NotSame(t, vi.data, vs.data)
}

func TestNewVectorWithSize(t *testing.T) {
	vi := NewVectorWithSize[int](10)
	assert.Equal(t, len(vi.data), 10)
	assert.Equal(t, cap(vi.data), 10)
	assert.Equal(t, reflect.TypeOf(vi.data).Kind(), reflect.Slice)
	assert.Equal(t, reflect.TypeOf(vi.data).Elem().Kind(), reflect.Int)

	vs := NewVectorWithSize[string](8)
	assert.Equal(t, len(vs.data), 8)
	assert.Equal(t, cap(vs.data), 8)
	assert.Equal(t, reflect.TypeOf(vs.data).Kind(), reflect.Slice)
	assert.Equal(t, reflect.TypeOf(vs.data).Elem().Kind(), reflect.String)

	assert.NotSame(t, vi.data, vs.data)
}

func TestNewVectorWithCapacity(t *testing.T) {
	vi := NewVectorWithCapacity[int](10)
	assert.Equal(t, len(vi.data), 0)
	assert.Equal(t, cap(vi.data), 10)
	assert.Equal(t, reflect.TypeOf(vi.data).Kind(), reflect.Slice)
	assert.Equal(t, reflect.TypeOf(vi.data).Elem().Kind(), reflect.Int)

	vs := NewVectorWithCapacity[string](8)
	assert.Equal(t, len(vs.data), 0)
	assert.Equal(t, cap(vs.data), 8)
	assert.Equal(t, reflect.TypeOf(vs.data).Kind(), reflect.Slice)
	assert.Equal(t, reflect.TypeOf(vs.data).Elem().Kind(), reflect.String)

	assert.NotSame(t, vi.data, vs.data)
}

func TestNewVectorWithDefaultValue(t *testing.T) {
	vi := NewVectorWithDefaultValue[int](10, 10)
	assert.Equal(t, len(vi.data), 10)
	assert.Equal(t, cap(vi.data), 10)
	for _, v := range vi.data {
		assert.Equal(t, 10, v)
	}
	assert.Equal(t, reflect.TypeOf(vi.data).Kind(), reflect.Slice)
	assert.Equal(t, reflect.TypeOf(vi.data).Elem().Kind(), reflect.Int)

	vs := NewVectorWithDefaultValue[string]("hello", 8)
	assert.Equal(t, len(vs.data), 8)
	assert.Equal(t, cap(vs.data), 8)
	for _, v := range vs.data {
		assert.Equal(t, "hello", v)
	}
	assert.Equal(t, reflect.TypeOf(vs.data).Kind(), reflect.Slice)
	assert.Equal(t, reflect.TypeOf(vs.data).Elem().Kind(), reflect.String)

	assert.NotSame(t, vi.data, vs.data)
}

func TestNewVectorFromSlice(t *testing.T) {
	data := make([]int, 0, 10)
	for i := 0; i < 10; i++ {
		data = append(data, i)
	}

	vi := NewVectorFromSlice[int](data)
	assert.Equal(t, len(vi.data), 10)
	assert.Equal(t, cap(vi.data), 10)
	assert.Equal(t, data, vi.data)
	assert.Equal(t, reflect.TypeOf(vi.data).Kind(), reflect.Slice)
	assert.Equal(t, reflect.TypeOf(vi.data).Elem().Kind(), reflect.Int)

	assert.NotSame(t, vi.data, data)
}

func TestNewVectorWithOtherVector(t *testing.T) {
	v1 := NewVectorFromSlice([]int{1, 2, 3, 4, 5, 6, 7})
	v1.Reserve(100)
	v2 := NewVectorWithOtherVector(v1)
	assert.Equal(t, v1.data, v2.data)
	assert.Equal(t, len(v1.data), len(v2.data))
	assert.Equal(t, 100, cap(v1.data))
	assert.Equal(t, 7, cap(v2.data))
	assert.NotSame(t, v1.data, v2.data)
}

func TestVectorRead(t *testing.T) {
	data := make([]int, 0, 10)
	for i := 0; i < 10; i++ {
		data = append(data, i)
	}
	v := NewVectorFromSlice(data)
	assert.Equal(t, 1, v.At(1))
	assert.Panics(t, func() {
		v.At(10)
	})
	assert.Panics(t, func() {
		v.At(-1)
	})
	assert.Equal(t, 10, v.Length())
	assert.Equal(t, 10, v.Capacity())
	assert.Equal(t, 0, v.Front())
	assert.Equal(t, 9, v.Back())
	d := v.Data()
	assert.Equal(t, (*reflect.SliceHeader)(unsafe.Pointer(&v.data)).Data, (*reflect.SliceHeader)(unsafe.Pointer(&d)).Data)
	assert.NotEqual(t, (*reflect.SliceHeader)(unsafe.Pointer(&data)).Data, (*reflect.SliceHeader)(unsafe.Pointer(&d)).Data)
	assert.False(t, v.Empty())
	assert.Equal(t, v.String(), "[0 1 2 3 4 5 6 7 8 9]")
	assert.True(t, v.Any(func(value int) bool {return value == 7}))


	vv := NewVector[string]()
	assert.True(t, vv.Empty())
	assert.Equal(t, vv.String(), "[]")
	assert.False(t, v.Any(func(value int) bool {return value == 10}))
}

func TestVectorWrite(t *testing.T) {
	data := make([]int, 0, 10)
	for i := 0; i < 10; i++ {
		data = append(data, i)
	}
	v := NewVectorFromSlice(data)
	assert.Equal(t, 10, v.Capacity())
	assert.Equal(t, 10, v.Length())

	v.Append(10)
	assert.Equal(t, 20, v.Capacity())
	assert.Equal(t, 11, v.Length())
	assert.Equal(t, 10, v.At(10))

	v.Reserve(10)
	assert.Equal(t, 20, v.Capacity())
	assert.Equal(t, 11, v.Length())
	assert.Equal(t, 10, v.At(10))

	v.Reserve(100)
	assert.Equal(t, 100, v.Capacity())
	assert.Equal(t, 11, v.Length())
	assert.Equal(t, 10, v.At(10))
	assert.Panics(t, func() {
		v.At(11)
	})

	v.ShrinkToFit()
	assert.Equal(t, 11, v.Capacity())
	assert.Equal(t, 11, v.Length())
	assert.Equal(t, 10, v.At(10))
	assert.Panics(t, func() {
		v.At(11)
	})

	v.Reserve(20)
	v.Prepend(-1)
	assert.Equal(t, 20, v.Capacity())
	assert.Equal(t, 12, v.Length())
	assert.Equal(t, -1, v.At(0))
	assert.Equal(t, 9, v.At(10))
	assert.Equal(t, 10, v.At(11))
	assert.Panics(t, func() {
		v.At(12)
	})
	assert.Panics(t, func() {
		v.At(-1)
	})
	assert.Equal(t, v.String(), "[-1 0 1 2 3 4 5 6 7 8 9 10]")

	v.Insert(3, 99)
	assert.Equal(t, 20, v.Capacity())
	assert.Equal(t, 13, v.Length())
	assert.Equal(t, -1, v.At(0))
	assert.Equal(t, 99, v.At(3))
	assert.Equal(t, 8, v.At(10))
	assert.Equal(t, 9, v.At(11))
	assert.Panics(t, func() {
		v.At(13)
	})
	assert.Panics(t, func() {
		v.At(-1)
	})
	assert.Equal(t, v.String(), "[-1 0 1 99 2 3 4 5 6 7 8 9 10]")

	v.PopFront()
	assert.Equal(t, 19, v.Capacity())
	assert.Equal(t, 12, v.Length())
	assert.Equal(t, 0, v.At(0))
	assert.Equal(t, 2, v.At(3))
	assert.Equal(t, 9, v.At(10))
	assert.Equal(t, 10, v.At(11))
	assert.Panics(t, func() {
		v.At(12)
	})
	assert.Panics(t, func() {
		v.At(-1)
	})
	assert.Equal(t, v.String(), "[0 1 99 2 3 4 5 6 7 8 9 10]")

	v.PopBack()
	assert.Equal(t, 19, v.Capacity())
	assert.Equal(t, 11, v.Length())
	assert.Equal(t, 0, v.At(0))
	assert.Equal(t, 2, v.At(3))
	assert.Equal(t, 9, v.At(10))
	assert.Panics(t, func() {
		v.At(11)
	})
	assert.Panics(t, func() {
		v.At(-1)
	})
	assert.Equal(t, v.String(), "[0 1 99 2 3 4 5 6 7 8 9]")

	v.Remove(2)
	assert.Equal(t, 19, v.Capacity())
	assert.Equal(t, 10, v.Length())
	assert.Equal(t, 0, v.At(0))
	assert.Equal(t, 3, v.At(3))
	assert.Equal(t, 9, v.At(9))
	assert.Panics(t, func() {
		v.At(10)
	})
	assert.Panics(t, func() {
		v.At(-1)
	})
	assert.Equal(t, v.String(), "[0 1 2 3 4 5 6 7 8 9]")

	for i, value := range v.Range() {
		assert.Equal(t, i, value)
	}

	count := 0
	v.ForEach(func(i int, v int) bool {
		count++
		assert.Equal(t, i, v)
		return true
	})
	assert.Equal(t, count, 10)

	count = 0
	v.ForEach(func(i int, v int) bool {
		count++
		assert.Equal(t, i, v)
		return false
	})
	assert.Equal(t, count, 1)
}
