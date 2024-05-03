package schema

type Type int

const (
	String Type = iota
	Int64
	Uint64
	Float64
	ByteSlice
	Bool
)

func (t Type) IsScalar() bool {
	return t >= String && t <= Bool
}
