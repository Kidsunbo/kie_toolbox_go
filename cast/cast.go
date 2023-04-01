package cast

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

type SupportedType interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string | ~bool
}

type SupportedIntegerType interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func To[T SupportedType](s string) (T, error) {
	var result T
	var ok bool
	switch any(result).(type) {
	case int:
		temp, err := strconv.ParseInt(s, 10, 64)
		result, ok = any(int(temp)).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to int", s)
		}
		return result, err
	case int8:
		temp, err := strconv.ParseInt(s, 10, 64)
		result, ok = any(int8(temp)).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to int8", s)
		}
		return result, err
	case int16:
		temp, err := strconv.ParseInt(s, 10, 64)
		result, ok = any(int16(temp)).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to int16", s)
		}
		return result, err
	case int32:
		temp, err := strconv.ParseInt(s, 10, 64)
		result, ok = any(int32(temp)).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to int32", s)
		}
		return result, err
	case int64:
		temp, err := strconv.ParseInt(s, 10, 64)
		result, ok = any(temp).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to int64", s)
		}
		return result, err
	case uint:
		temp, err := strconv.ParseInt(s, 10, 64)
		result, ok = any(uint(temp)).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to uint", s)
		}
		return result, err
	case uint8:
		temp, err := strconv.ParseInt(s, 10, 64)
		result, ok = any(uint8(temp)).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to uint8", s)
		}
		return result, err
	case uint16:
		temp, err := strconv.ParseInt(s, 10, 64)
		result, ok = any(uint16(temp)).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to uint16", s)
		}
		return result, err
	case uint32:
		temp, err := strconv.ParseInt(s, 10, 64)
		result, ok = any(uint32(temp)).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to uint32", s)
		}
		return result, err
	case uint64:
		temp, err := strconv.ParseInt(s, 10, 64)
		result, ok = any(uint64(temp)).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to uint64", s)
		}
		return result, err
	case string:
		result, ok = any(s).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to string", s)
		}
		return result, nil
	case bool:
		temp, err := strconv.ParseBool(s)
		result, ok = any(temp).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to bool", s)
		}
		return result, err
	case float32:
		temp, err := strconv.ParseFloat(s, 64)
		result, ok = any(float32(temp)).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to float32", s)
		}
		return result, err
	case float64:
		temp, err := strconv.ParseFloat(s, 64)
		result, ok = any(temp).(T)
		if !ok {
			return result, fmt.Errorf("failed to cast %s to float64", s)
		}
		return result, err
	case T:
		switch reflect.TypeOf(result).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val, err := To[int64](s)
			value := reflect.ValueOf(&result).Elem()
			if value.CanSet() {
				value.SetInt(val)
			}
			result, ok = value.Interface().(T)
			if !ok {
				return result, fmt.Errorf("failed to cast %s to %v", s, reflect.TypeOf(result).Kind().String())
			}
			return result, err
		case reflect.Float32, reflect.Float64:
			val, err := To[float64](s)
			value := reflect.ValueOf(&result).Elem()
			if value.CanSet() {
				value.SetFloat(val)
			}
			result, ok = value.Interface().(T)
			if !ok {
				return result, fmt.Errorf("failed to cast %s to %v", s, reflect.TypeOf(result).Kind().String())
			}
			return result, err
		case reflect.Bool:
			val, err := To[bool](s)
			value := reflect.ValueOf(&result).Elem()
			if value.CanSet() {
				value.SetBool(val)
			}
			result, ok = value.Interface().(T)
			if !ok {
				return result, fmt.Errorf("failed to cast %s to %v", s, reflect.TypeOf(result).Kind().String())
			}
			return result, err
		case reflect.String:
			val, err := To[string](s)
			value := reflect.ValueOf(&result).Elem()
			if value.CanSet() {
				value.SetString(val)
			}
			result, ok = value.Interface().(T)
			if !ok {
				return result, fmt.Errorf("failed to cast %s to %v", s, reflect.TypeOf(result).Kind().String())
			}
			return result, err
		default:
			return result, fmt.Errorf("no supported type %v", reflect.TypeOf(result).Kind().String())
		}
	default:
		return result, errors.New("no supported type")
	}
}

func JSONTo[T any](s string) (T, error) {
	var result T
	err := json.Unmarshal([]byte(s), &result)
	return result, err
}

func ToWithDefault[T SupportedType](s string, defaultValue T) T {
	if v, err := To[T](s); err != nil {
		return defaultValue
	} else {
		return v
	}
}

func ToInteger[T SupportedIntegerType](s string) (T, error) {
	val, err := strconv.ParseInt(s, 10, 64)
	return T(val), err
}

func ToIntegerWithDefault[T SupportedIntegerType](s string, defaultValue T) T {
	if v, err := ToInteger[T](s); err != nil {
		return defaultValue
	} else {
		return v
	}
}
