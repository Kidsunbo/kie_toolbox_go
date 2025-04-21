package cast_test

import (
	"testing"

	"github.com/Kidsunbo/kie_toolbox_go/cast"
	"github.com/stretchr/testify/assert"
)

func TestToBool(t *testing.T) {
	s := "t"
	v, err := cast.To[bool](s)
	assert.Equal(t, v, true)
	assert.Equal(t, nil, err)

	s = "tr"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)

	s = "true"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, true)
	assert.Equal(t, nil, err)

	s = "f"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, false)
	assert.Equal(t, nil, err)

	s = "fa"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)

	s = "false"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, false)
	assert.Equal(t, nil, err)

	s = "0"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, false)
	assert.Equal(t, nil, err)

	s = "-1"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)

	s = "-"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)

	s = "1"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, true)
	assert.Equal(t, nil, err)

	s = "2"
	v, err = cast.To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)
}

func TestToString(t *testing.T) {
	s := "t"
	v, err := cast.To[string](s)
	assert.Equal(t, v, "t")
	assert.Equal(t, nil, err)

	s = "tr"
	v, err = cast.To[string](s)
	assert.Equal(t, v, "tr")
	assert.Equal(t, nil, err)
}

func TestToInt(t *testing.T) {
	s := "t"
	v, err := cast.To[int](s)
	assert.Equal(t, v, 0)
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[int](s)
	assert.Equal(t, v, 123)
	assert.Equal(t, nil, err)
}

func TestToInt8(t *testing.T) {
	s := "t"
	v, err := cast.To[int8](s)
	assert.Equal(t, v, int8(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[int8](s)
	assert.Equal(t, v, int8(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = cast.ToInteger[int8](s)
	assert.Equal(t, v, int8(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.ToInteger[int8](s)
	assert.Equal(t, v, int8(123))
	assert.Equal(t, nil, err)
}

func TestToInt16(t *testing.T) {
	s := "t"
	v, err := cast.To[int16](s)
	assert.Equal(t, v, int16(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[int16](s)
	assert.Equal(t, v, int16(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = cast.ToInteger[int16](s)
	assert.Equal(t, v, int16(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.ToInteger[int16](s)
	assert.Equal(t, v, int16(123))
	assert.Equal(t, nil, err)
}

func TestToInt32(t *testing.T) {
	s := "t"
	v, err := cast.To[int32](s)
	assert.Equal(t, v, int32(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[int32](s)
	assert.Equal(t, v, int32(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = cast.ToInteger[int32](s)
	assert.Equal(t, v, int32(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.ToInteger[int32](s)
	assert.Equal(t, v, int32(123))
	assert.Equal(t, nil, err)
}

func TestToInt64(t *testing.T) {
	s := "t"
	v, err := cast.To[int64](s)
	assert.Equal(t, v, int64(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[int64](s)
	assert.Equal(t, v, int64(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = cast.ToInteger[int64](s)
	assert.Equal(t, v, int64(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.ToInteger[int64](s)
	assert.Equal(t, v, int64(123))
	assert.Equal(t, nil, err)
}

func TestToUint(t *testing.T) {
	s := "t"
	v, err := cast.To[uint](s)
	assert.Equal(t, v, uint(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[uint](s)
	assert.Equal(t, v, uint(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = cast.ToInteger[uint](s)
	assert.Equal(t, v, uint(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.ToInteger[uint](s)
	assert.Equal(t, v, uint(123))
	assert.Equal(t, nil, err)
}

func TestToUint8(t *testing.T) {
	s := "t"
	v, err := cast.To[uint8](s)
	assert.Equal(t, v, uint8(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[uint8](s)
	assert.Equal(t, v, uint8(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = cast.ToInteger[uint8](s)
	assert.Equal(t, v, uint8(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.ToInteger[uint8](s)
	assert.Equal(t, v, uint8(123))
	assert.Equal(t, nil, err)
}

func TestToUint16(t *testing.T) {
	s := "t"
	v, err := cast.To[uint16](s)
	assert.Equal(t, v, uint16(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[uint16](s)
	assert.Equal(t, v, uint16(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = cast.ToInteger[uint16](s)
	assert.Equal(t, v, uint16(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.ToInteger[uint16](s)
	assert.Equal(t, v, uint16(123))
	assert.Equal(t, nil, err)
}

func TestToUint32(t *testing.T) {
	s := "t"
	v, err := cast.To[uint32](s)
	assert.Equal(t, v, uint32(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[uint32](s)
	assert.Equal(t, v, uint32(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = cast.ToInteger[uint32](s)
	assert.Equal(t, v, uint32(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.ToInteger[uint32](s)
	assert.Equal(t, v, uint32(123))
	assert.Equal(t, nil, err)
}

func TestToUint64(t *testing.T) {
	s := "t"
	v, err := cast.To[uint64](s)
	assert.Equal(t, v, uint64(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[uint64](s)
	assert.Equal(t, v, uint64(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = cast.ToInteger[uint64](s)
	assert.Equal(t, v, uint64(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.ToInteger[uint64](s)
	assert.Equal(t, v, uint64(123))
	assert.Equal(t, nil, err)
}

func TestToFloat32(t *testing.T) {
	s := "t"
	v, err := cast.To[float32](s)
	assert.Equal(t, v, float32(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[float32](s)
	assert.Equal(t, v, float32(123))
	assert.Equal(t, nil, err)

	s = "123.456"
	v, err = cast.To[float32](s)
	assert.Equal(t, v, float32(123.456))
	assert.Equal(t, nil, err)
}

func TestToFloat64(t *testing.T) {
	s := "t"
	v, err := cast.To[float64](s)
	assert.Equal(t, v, float64(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = cast.To[float64](s)
	assert.Equal(t, v, float64(123))
	assert.Equal(t, nil, err)

	s = "123.456"
	v, err = cast.To[float64](s)
	assert.Equal(t, v, 123.456)
	assert.Equal(t, nil, err)
}

func TestToDefault(t *testing.T) {
	right := "123"
	wrong := "abc"
	assert.Equal(t, int8(123), cast.ToWithDefault[int8](right, 0))
	assert.Equal(t, int8(0), cast.ToWithDefault[int8](wrong, 0))

	assert.Equal(t, int16(123), cast.ToWithDefault[int16](right, 0))
	assert.Equal(t, int16(0), cast.ToWithDefault[int16](wrong, 0))

	assert.Equal(t, uint8(123), cast.ToWithDefault[uint8](right, 0))
	assert.Equal(t, uint8(0), cast.ToWithDefault[uint8](wrong, 0))

	assert.Equal(t, uint16(123), cast.ToWithDefault[uint16](right, 0))
	assert.Equal(t, uint16(0), cast.ToWithDefault[uint16](wrong, 0))

	assert.Equal(t, uint16(123), cast.ToIntegerWithDefault[uint16](right, 0))
	assert.Equal(t, uint16(0), cast.ToIntegerWithDefault[uint16](wrong, 0))
}

func TestToUnderlyingType(t *testing.T) {
	type UnderlyingInt int
	type UnderlyingString string
	type UnderlyingFloat64 float64
	type UnderlyingFloat32 float32
	type UnderlyingBool bool

	s := "123"
	v, err := cast.To[UnderlyingInt](s)
	assert.Equal(t, UnderlyingInt(123), v)
	assert.Equal(t, nil, err)

	s = "t"
	vb, err := cast.To[UnderlyingBool](s)
	assert.Equal(t, UnderlyingBool(true), vb)
	assert.Equal(t, nil, err)

	s = "hello"
	vs, err := cast.To[UnderlyingString](s)
	assert.Equal(t, UnderlyingString("hello"), vs)
	assert.Equal(t, nil, err)

	s = "123.456"
	vf32, err := cast.To[UnderlyingFloat32](s)
	assert.Equal(t, UnderlyingFloat32(123.456), vf32)
	assert.Equal(t, nil, err)

	s = "123.456"
	vf64, err := cast.To[UnderlyingFloat64](s)
	assert.Equal(t, UnderlyingFloat64(123.456), vf64)
	assert.Equal(t, nil, err)
}

func TestJSONTo(t *testing.T) {
	type s struct {
		Hello string `json:"hello"`
		World int64  `json:"world"`
	}
	v, err := cast.JSONTo[*s](`{"world": 1234567, "hello":"hey"}`)
	assert.Equal(t, nil, err)
	assert.Equal(t, "hey", v.Hello)
	assert.Equal(t, int64(1234567), v.World)

	v, err = cast.JSONTo[*s](`{"cat":"wolf", "dog":"meow"}`)
	assert.Equal(t, nil, err)
	assert.Equal(t, "", v.Hello)
	assert.Equal(t, int64(0), v.World)

	v, err = cast.JSONTo[*s](`{"cat":"wolf", "dog":"meow"`)
	assert.NotEqual(t, nil, err)
	assert.True(t, v == nil)

	a, err := cast.JSONTo[[]string](`["hello", "world"]`)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(a))
	assert.Equal(t, "hello", a[0])
	assert.Equal(t, "world", a[1])
}
