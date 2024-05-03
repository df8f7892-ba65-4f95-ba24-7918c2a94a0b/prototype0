package scalar_test

import (
	"testing"

	"github.com/df8f7892-ba65-4f95-ba24-7918c2a94a0b/prototype0/scalar"

	"github.com/stretchr/testify/assert"
)

var scalarTestCases = []struct {
	name      string
	valueType scalar.Type
	value     any
}{
	{
		name:      "string",
		valueType: scalar.String,
		value:     string("hello world"),
	},
	{
		name:      "int64",
		valueType: scalar.Int64,
		value:     int64(-42),
	},
	{
		name:      "uint64",
		valueType: scalar.Uint64,
		value:     uint64(42),
	},
	{
		name:      "float64",
		valueType: scalar.Float64,
		value:     float64(3.14),
	},
	{
		name:      "byte slice",
		valueType: scalar.ByteSlice,
		value:     []byte("hello world"),
	},
	{
		name:      "bool",
		valueType: scalar.Bool,
		value:     true,
	},
}

func TestNew(t *testing.T) {
	for _, tt := range scalarTestCases {
		t.Run(tt.name, func(t *testing.T) {
			var v any
			var ok bool

			switch tt.valueType {
			case scalar.String:
				s := scalar.New(tt.value.(string))
				v, ok = any(s.Value).(string)
			case scalar.Int64:
				s := scalar.New(tt.value.(int64))
				v, ok = any(s.Value).(int64)
			case scalar.Uint64:
				s := scalar.New(tt.value.(uint64))
				v, ok = any(s.Value).(uint64)
			case scalar.Float64:
				s := scalar.New(tt.value.(float64))
				v, ok = any(s.Value).(float64)
			case scalar.ByteSlice:
				s := scalar.New(tt.value.([]byte))
				v, ok = any(s.Value).([]byte)
			case scalar.Bool:
				s := scalar.New(tt.value.(bool))
				v, ok = any(s.Value).(bool)
			}

			assert.True(t, ok)
			assert.Equal(t, tt.value, v)
		})
	}
}

func TestValue(t *testing.T) {
	for _, tt := range scalarTestCases {
		t.Run(tt.name, func(t *testing.T) {
			var got any
			var ok bool

			switch tt.valueType {
			case scalar.String:
				s := scalar.New(tt.value.(string))
				var v string
				ok = scalar.Value(s, &v)
				got = v
			case scalar.Int64:
				s := scalar.New(tt.value.(int64))
				var v int64
				ok = scalar.Value(s, &v)
				got = v
			case scalar.Uint64:
				s := scalar.New(tt.value.(uint64))
				var v uint64
				ok = scalar.Value(s, &v)
				got = v
			case scalar.Float64:
				s := scalar.New(tt.value.(float64))
				var v float64
				ok = scalar.Value(s, &v)
				got = v
			case scalar.ByteSlice:
				s := scalar.New(tt.value.([]byte))
				var v []byte
				ok = scalar.Value(s, &v)
				got = v
			case scalar.Bool:
				s := scalar.New(tt.value.(bool))
				var v bool
				ok = scalar.Value(s, &v)
				got = v
			}

			assert.True(t, ok)
			assert.Equal(t, tt.value, got)
		})
	}
}

func TestScalar_Type(t *testing.T) {
	for _, tt := range scalarTestCases {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.valueType {
			case scalar.String:
				s := scalar.New(tt.value.(string))
				assert.Equal(t, s.Type(), tt.valueType)
			case scalar.Int64:
				s := scalar.New(tt.value.(int64))
				assert.Equal(t, s.Type(), tt.valueType)
			case scalar.Uint64:
				s := scalar.New(tt.value.(uint64))
				assert.Equal(t, s.Type(), tt.valueType)
			case scalar.Float64:
				s := scalar.New(tt.value.(float64))
				assert.Equal(t, s.Type(), tt.valueType)
			case scalar.ByteSlice:
				s := scalar.New(tt.value.([]byte))
				assert.Equal(t, s.Type(), tt.valueType)
			case scalar.Bool:
				s := scalar.New(tt.value.(bool))
				assert.Equal(t, s.Type(), tt.valueType)
			}
		})
	}
}
