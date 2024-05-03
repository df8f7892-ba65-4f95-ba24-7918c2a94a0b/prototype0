package schema

type Schema struct {
	Name       string
	Version    int
	Attributes []Attribute
}

type Attribute struct {
	Name string
	Type Type
}

type Map struct {
	Key   Type
	Value Type
}
