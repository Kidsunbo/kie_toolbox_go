package cast

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestToBool(t *testing.T) {
	s := "t"
	v, err := To[bool](s)
	assert.Equal(t, v, true)
	assert.Equal(t, nil, err)

	s = "tr"
	v, err = To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)

	s = "true"
	v, err = To[bool](s)
	assert.Equal(t, v, true)
	assert.Equal(t, nil, err)

	s = "f"
	v, err = To[bool](s)
	assert.Equal(t, v, false)
	assert.Equal(t, nil, err)

	s = "fa"
	v, err = To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)

	s = "false"
	v, err = To[bool](s)
	assert.Equal(t, v, false)
	assert.Equal(t, nil, err)

	s = "0"
	v, err = To[bool](s)
	assert.Equal(t, v, false)
	assert.Equal(t, nil, err)

	s = "-1"
	v, err = To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)

	s = "-"
	v, err = To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)

	s = "1"
	v, err = To[bool](s)
	assert.Equal(t, v, true)
	assert.Equal(t, nil, err)

	s = "2"
	v, err = To[bool](s)
	assert.Equal(t, v, false)
	assert.NotEqual(t, nil, err)
}

func TestToString(t *testing.T) {
	s := "t"
	v, err := To[string](s)
	assert.Equal(t, v, "t")
	assert.Equal(t, nil, err)

	s = "tr"
	v, err = To[string](s)
	assert.Equal(t, v, "tr")
	assert.Equal(t, nil, err)
}

func TestToInt(t *testing.T) {
	s := "t"
	v, err := To[int](s)
	assert.Equal(t, v, 0)
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[int](s)
	assert.Equal(t, v, 123)
	assert.Equal(t, nil, err)
}

func TestToInt8(t *testing.T) {
	s := "t"
	v, err := To[int8](s)
	assert.Equal(t, v, int8(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[int8](s)
	assert.Equal(t, v, int8(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = ToInteger[int8](s)
	assert.Equal(t, v, int8(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = ToInteger[int8](s)
	assert.Equal(t, v, int8(123))
	assert.Equal(t, nil, err)
}

func TestToInt16(t *testing.T) {
	s := "t"
	v, err := To[int16](s)
	assert.Equal(t, v, int16(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[int16](s)
	assert.Equal(t, v, int16(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = ToInteger[int16](s)
	assert.Equal(t, v, int16(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = ToInteger[int16](s)
	assert.Equal(t, v, int16(123))
	assert.Equal(t, nil, err)
}

func TestToInt32(t *testing.T) {
	s := "t"
	v, err := To[int32](s)
	assert.Equal(t, v, int32(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[int32](s)
	assert.Equal(t, v, int32(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = ToInteger[int32](s)
	assert.Equal(t, v, int32(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = ToInteger[int32](s)
	assert.Equal(t, v, int32(123))
	assert.Equal(t, nil, err)
}

func TestToInt64(t *testing.T) {
	s := "t"
	v, err := To[int64](s)
	assert.Equal(t, v, int64(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[int64](s)
	assert.Equal(t, v, int64(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = ToInteger[int64](s)
	assert.Equal(t, v, int64(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = ToInteger[int64](s)
	assert.Equal(t, v, int64(123))
	assert.Equal(t, nil, err)
}

func TestToUint(t *testing.T) {
	s := "t"
	v, err := To[uint](s)
	assert.Equal(t, v, uint(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[uint](s)
	assert.Equal(t, v, uint(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = ToInteger[uint](s)
	assert.Equal(t, v, uint(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = ToInteger[uint](s)
	assert.Equal(t, v, uint(123))
	assert.Equal(t, nil, err)
}

func TestToUint8(t *testing.T) {
	s := "t"
	v, err := To[uint8](s)
	assert.Equal(t, v, uint8(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[uint8](s)
	assert.Equal(t, v, uint8(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = ToInteger[uint8](s)
	assert.Equal(t, v, uint8(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = ToInteger[uint8](s)
	assert.Equal(t, v, uint8(123))
	assert.Equal(t, nil, err)
}

func TestToUint16(t *testing.T) {
	s := "t"
	v, err := To[uint16](s)
	assert.Equal(t, v, uint16(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[uint16](s)
	assert.Equal(t, v, uint16(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = ToInteger[uint16](s)
	assert.Equal(t, v, uint16(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = ToInteger[uint16](s)
	assert.Equal(t, v, uint16(123))
	assert.Equal(t, nil, err)
}

func TestToUint32(t *testing.T) {
	s := "t"
	v, err := To[uint32](s)
	assert.Equal(t, v, uint32(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[uint32](s)
	assert.Equal(t, v, uint32(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = ToInteger[uint32](s)
	assert.Equal(t, v, uint32(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = ToInteger[uint32](s)
	assert.Equal(t, v, uint32(123))
	assert.Equal(t, nil, err)
}

func TestToUint64(t *testing.T) {
	s := "t"
	v, err := To[uint64](s)
	assert.Equal(t, v, uint64(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[uint64](s)
	assert.Equal(t, v, uint64(123))
	assert.Equal(t, nil, err)

	s = "t"
	v, err = ToInteger[uint64](s)
	assert.Equal(t, v, uint64(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = ToInteger[uint64](s)
	assert.Equal(t, v, uint64(123))
	assert.Equal(t, nil, err)
}

func TestToFloat32(t *testing.T) {
	s := "t"
	v, err := To[float32](s)
	assert.Equal(t, v, float32(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[float32](s)
	assert.Equal(t, v, float32(123))
	assert.Equal(t, nil, err)

	s = "123.456"
	v, err = To[float32](s)
	assert.Equal(t, v, float32(123.456))
	assert.Equal(t, nil, err)
}

func TestToFloat64(t *testing.T) {
	s := "t"
	v, err := To[float64](s)
	assert.Equal(t, v, float64(0))
	assert.NotEqual(t, nil, err)

	s = "123"
	v, err = To[float64](s)
	assert.Equal(t, v, float64(123))
	assert.Equal(t, nil, err)

	s = "123.456"
	v, err = To[float64](s)
	assert.Equal(t, v, 123.456)
	assert.Equal(t, nil, err)
}

func TestToDefault(t *testing.T) {
	right := "123"
	wrong := "abc"
	assert.Equal(t, int8(123), ToWithDefault[int8](right, 0))
	assert.Equal(t, int8(0), ToWithDefault[int8](wrong, 0))

	assert.Equal(t, int16(123), ToWithDefault[int16](right, 0))
	assert.Equal(t, int16(0), ToWithDefault[int16](wrong, 0))

	assert.Equal(t, uint8(123), ToWithDefault[uint8](right, 0))
	assert.Equal(t, uint8(0), ToWithDefault[uint8](wrong, 0))

	assert.Equal(t, uint16(123), ToWithDefault[uint16](right, 0))
	assert.Equal(t, uint16(0), ToWithDefault[uint16](wrong, 0))

	assert.Equal(t, uint16(123), ToIntegerWithDefault[uint16](right, 0))
	assert.Equal(t, uint16(0), ToIntegerWithDefault[uint16](wrong, 0))
}

func TestToUnderlyingType(t *testing.T) {
	type UnderlyingInt int
	type UnderlyingString string
	type UnderlyingFloat64 float64
	type UnderlyingFloat32 float32
	type UnderlyingBool bool

	s := "123"
	v, err := To[UnderlyingInt](s)
	assert.Equal(t, UnderlyingInt(123), v)
	assert.Equal(t, nil, err)

	s = "t"
	vb, err := To[UnderlyingBool](s)
	assert.Equal(t, UnderlyingBool(true), vb)
	assert.Equal(t, nil, err)

	s = "hello"
	vs, err := To[UnderlyingString](s)
	assert.Equal(t, UnderlyingString("hello"), vs)
	assert.Equal(t, nil, err)

	s = "123.456"
	vf32, err := To[UnderlyingFloat32](s)
	assert.Equal(t, UnderlyingFloat32(123.456), vf32)
	assert.Equal(t, nil, err)

	s = "123.456"
	vf64, err := To[UnderlyingFloat64](s)
	assert.Equal(t, UnderlyingFloat64(123.456), vf64)
	assert.Equal(t, nil, err)
}
