package schema

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaParser(t *testing.T) {
	parser, err := NewParser()
	require.NoError(t, err)

	testCases := []struct {
		name      string
		schemaStr string
		expected  *Schema
	}{
		{
			name: "Basic Struct",
			schemaStr: `
context prototype0_blogging {
	record post Struct {
		attribute title string = 1 {}
		attribute body string = 2 {}
		attribute tags repeated string = 3 {}
	}
}`,
			expected: &Schema{
				Context: "prototype0_blogging",
				Records: []Record{{
					Name: "post",
					Type: "Struct",
					Attributes: []Attribute{{
						Name:       "title",
						Type:       "string",
						Tag:        1,
						Properties: Properties{},
					}, {
						Name:       "body",
						Type:       "string",
						Tag:        2,
						Properties: Properties{},
					}, {
						Name:       "tags",
						Type:       "string",
						Tag:        3,
						Repeated:   true,
						Properties: Properties{},
					}},
				}},
			},
		},
		{
			name: "Empty Struct",
			schemaStr: `
context prototype0_blogging {
	record empty Struct {}
}`,
			expected: &Schema{
				Context: "prototype0_blogging",
				Records: []Record{{
					Name:       "empty",
					Type:       "Struct",
					Attributes: nil,
				}},
			},
		},
		{
			name: "Multiple Records",
			schemaStr: `
context prototype0_blogging {
	record post Struct {
		attribute title string = 1 {}
	}
	record comment Struct {
		attribute content string = 1 {}
	}
}`,
			expected: &Schema{
				Context: "prototype0_blogging",
				Records: []Record{
					{
						Name: "post",
						Type: "Struct",
						Attributes: []Attribute{{
							Name:       "title",
							Type:       "string",
							Tag:        1,
							Properties: Properties{},
						}},
					},
					{
						Name: "comment",
						Type: "Struct",
						Attributes: []Attribute{{
							Name:       "content",
							Type:       "string",
							Tag:        1,
							Properties: Properties{},
						}},
					},
				},
			},
		},
		{
			name: "Properties true",
			schemaStr: `
context prototype0_blogging {
	record post Struct {
		attribute title string = 1 {
			required: false,
		}
		attribute body string = 2 {
			required: true,
		}
	}
}`,
			expected: &Schema{
				Context: "prototype0_blogging",
				Records: []Record{
					{
						Name: "post",
						Type: "Struct",
						Attributes: []Attribute{{
							Name: "title",
							Type: "string",
							Tag:  1,
							Properties: Properties{
								Required: false,
							},
						}, {
							Name: "body",
							Type: "string",
							Tag:  2,
							Properties: Properties{
								Required: true,
							},
						}},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := parser.ParseString(tc.schemaStr)
			require.NoError(t, err)

			b, _ := json.MarshalIndent(schema, "", "  ")
			fmt.Println(string(b))

			assert.Equal(t, tc.expected, schema)
		})
	}
}
