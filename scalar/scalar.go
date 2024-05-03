package scalar

type Type int

const (
	String Type = iota
	Int64
	Uint64
	Float64
	ByteSlice
	Bool
)

type ScalarValue interface {
	string | int64 | uint64 | float64 | []byte | bool
}

type Scalar[T ScalarValue] struct {
	Value T
}

func New[T ScalarValue](v T) *Scalar[T] {
	return &Scalar[T]{Value: v}
}

func Value[T ScalarValue, V ScalarValue](s *Scalar[T], v *V) bool {
	switch any(s.Value).(type) {
	case V:
		*v = any(s.Value).(V)

		return true
	default:
		var zero V

		*v = zero

		return false
	}
}

func (s *Scalar[T]) Type() Type {
	switch any(s.Value).(type) {
	case string:
		return String
	case int64:
		return Int64
	case uint64:
		return Uint64
	case float64:
		return Float64
	case []byte:
		return ByteSlice
	case bool:
		return Bool
	default:
		return -1
	}
}

// String returns if Scalar Value is a string.
func (s *Scalar[T]) String() (string, bool) {
	v, ok := any(s.Value).(string)

	return v, ok
}

// Int64 returns if Scalar Value is an int64.
func (s *Scalar[T]) Int64() (int64, bool) {
	v, ok := any(s.Value).(int64)

	return v, ok
}

// Uint64 returns if Scalar Value is a uint64.
func (s *Scalar[T]) Uint64() (uint64, bool) {
	v, ok := any(s.Value).(uint64)

	return v, ok
}

// Float64 returns if Scalar Value is a float64.
func (s *Scalar[T]) Float64() (float64, bool) {
	v, ok := any(s.Value).(float64)

	return v, ok
}

// ByteSlice returns if Scalar Value is a byte slice.
func (s *Scalar[T]) ByteSlice() ([]byte, bool) {
	v, ok := any(s.Value).([]byte)

	return v, ok
}

// Bool returns if Scalar Value is a bool.
func (s *Scalar[T]) Bool() (bool, bool) {
	v, ok := any(s.Value).(bool)

	return v, ok
}
