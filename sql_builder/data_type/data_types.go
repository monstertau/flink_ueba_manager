package data_type

import "fmt"

func STRING() string {
	return "STRING"
}

func BOOLEAN() string {
	return "BOOLEAN"
}

func BIGINT() string {
	return "BIGINT"
}

func TIMESTAMP_LTZ() string {
	return "TIMESTAMP_LTZ"
}

func FLOAT() string {
	return "FLOAT"
}

func TIMESTAMP() string {
	return "TIMESTAMP"
}

func TIMESTAMP_PRECISION(prec int) string {
	return fmt.Sprintf("TIMESTAMP(%v)", prec)
}
