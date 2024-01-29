package util

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// CastNumberToInt64+ casts a number into int64
func CastNumberToInt64(i interface{}) (int64, bool) {
	switch v := i.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	}
	return 0, false
}

// ParseInt64 converts an interface into int64
func ParseInt64(i interface{}) int64 {
	v, _ := ParseInt64Extended(i)
	return v
}

// ParseInt64Extended converts an interface into int64
func ParseInt64Extended(i interface{}) (int64, error) {
	if ans, ok := CastNumberToInt64(i); ok {
		return ans, nil
	}
	switch v := i.(type) {
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("failed to convert '%v' to int64", i)
	}
}

// CastNumberToFloat64 casts a number into float64
func CastNumberToFloat64(i interface{}) (float64, bool) {
	switch v := i.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	}
	return 0, false
}

// ParseFloat64 converts an interface into float64
func ParseFloat64(i interface{}) (float64, error) {
	if ans, ok := CastNumberToFloat64(i); ok {
		return ans, nil
	}
	switch v := i.(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("failed to convert '%v' to float64", i)
	}
}

// ParseString converts the object to string representation
func ParseString(object interface{}) string {
	switch v := object.(type) {
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	}
	return ""
}

func ParseStringExtended(obj interface{}) (string, bool) {
	switch v := obj.(type) {
	case int:
		return strconv.FormatInt(int64(v), 10), true
	case int8:
		return strconv.FormatInt(int64(v), 10), true
	case int16:
		return strconv.FormatInt(int64(v), 10), true
	case int32:
		return strconv.FormatInt(int64(v), 10), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case uint:
		return strconv.FormatUint(uint64(v), 10), true
	case uint8:
		return strconv.FormatUint(uint64(v), 10), true
	case uint16:
		return strconv.FormatUint(uint64(v), 10), true
	case uint32:
		return strconv.FormatUint(uint64(v), 10), true
	case uint64:
		return strconv.FormatUint(v, 10), true
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), true
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	case string:
		return v, true
	case bool:
		return strconv.FormatBool(v), true
	}
	return "", false
}

func ToJson(obj interface{}) string {
	bytes, err := json.MarshalIndent(obj, "", "\t")
	if err != nil {
		return err.Error()
	}
	return string(bytes)
}

func Interface2slice(value interface{}) []string {
	v, ok := value.([]interface{})
	if !ok {
		return []string{}
	}
	l := make([]string, len(v))
	for i := range v {
		l[i] = v[i].(string)
	}
	return l
}
