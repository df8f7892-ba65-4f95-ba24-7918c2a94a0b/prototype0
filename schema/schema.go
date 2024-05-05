package schema

import (
	"fmt"

	"github.com/alecthomas/participle/v2"
)

type (
	Schema struct {
		Context string    `parser:"'context' @Ident '{'"`
		Version int       `parser:"('version' @Int ',')?"`
		Records []*Record `parser:"@@* '}'"`
	}
	Record struct {
		Name       string       `parser:"'record' @Ident"`
		Type       string       `parser:"@Ident"`
		Attributes []*Attribute `parser:"'{' @@* '}'"`
	}
	Attribute struct {
		Name       string      `parser:"'attribute' @Ident"`
		Repeated   bool        `parser:"@'repeated'?"`
		Type       string      `parser:"@Ident"`
		Tag        int         `parser:"'=' @Int"`
		Properties *Properties `parser:"'{' @@* '}'"`
	}
	Properties struct {
		Mutable          bool               `parser:"'mutable'':' (@'true' | 'false') ','?"`
		ValidationFields []*ValidationField `parser:"'validation'':' '{' @@* '}' ','?"`
		Validation       *Validation
	}
	ValidationField struct {
		Required  *bool `parser:"'required'':' (@'true' | 'false') ','?"`
		MaxLength *int  `parser:"| 'maxLen'':' @Int ','?"`
		MinLength *int  `parser:"| 'minLen'':' @Int ','?"`
	}
	Validation struct {
		Required  bool
		MaxLength *int
		MinLength *int
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

	// merge validation fields into validation
	for _, r := range s.Records {
		for _, a := range r.Attributes {
			if a.Properties == nil {
				continue
			}

			if len(a.Properties.ValidationFields) == 0 {
				continue
			}

			a.Properties.Validation = &Validation{}
			for _, f := range a.Properties.ValidationFields {
				switch {
				case f.Required != nil:
					a.Properties.Validation.Required = *f.Required
				case f.MaxLength != nil:
					a.Properties.Validation.MaxLength = f.MaxLength
				case f.MinLength != nil:
					a.Properties.Validation.MinLength = f.MinLength
				}
			}
		}
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
