package schema

import (
	"fmt"

	"github.com/alecthomas/participle/v2"
)

type (
	Schema struct {
		Context string   `parser:"'context' @Ident"`
		Records []Record `parser:"'{' @@* '}'"`
	}
	Record struct {
		Name       string      `parser:"'record' @Ident"`
		Type       string      `parser:"@Ident"`
		Attributes []Attribute `parser:"'{' @@* '}'"`
	}
	Attribute struct {
		Name       string     `parser:"'attribute' @Ident"`
		Repeated   bool       `parser:"@'repeated'?"`
		Type       string     `parser:"@Ident"`
		Tag        int        `parser:"'=' @Int"`
		Properties Properties `parser:"'{' '}'"`
	}
	Properties struct {
	}
)

type Parser struct {
	parser *participle.Parser[Schema]
}

func (p *Parser) ParseString(str string) (*Schema, error) {
	s, err := p.parser.ParseString("", str)
	if err != nil {
		return nil, fmt.Errorf("error parsing schema: %w", err)
	}

	return s, nil
}

func NewParser() (*Parser, error) {
	pp, err := participle.Build[Schema](
		participle.UseLookahead(2),
	)
	if err != nil {
		return nil, fmt.Errorf("error building parser: %w", err)
	}

	p := &Parser{
		parser: pp,
	}

	return p, nil
}
