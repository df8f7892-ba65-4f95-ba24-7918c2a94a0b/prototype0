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
	version 1,
	record post Struct {
		attribute title string = 1 {}
		attribute body string = 2 {}
		attribute tags repeated string = 3 {}
	}
}`,
			expected: &Schema{
				Context: "prototype0_blogging",
				Version: 1,
				Records: []*Record{{
					Name: "post",
					Type: "Struct",
					Attributes: []*Attribute{{
						Name:       "title",
						Type:       "string",
						Tag:        1,
						Properties: nil,
					}, {
						Name:       "body",
						Type:       "string",
						Tag:        2,
						Properties: nil,
					}, {
						Name:       "tags",
						Type:       "string",
						Tag:        3,
						Repeated:   true,
						Properties: nil,
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
				Records: []*Record{{
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
				Records: []*Record{
					{
						Name: "post",
						Type: "Struct",
						Attributes: []*Attribute{{
							Name:       "title",
							Type:       "string",
							Tag:        1,
							Properties: nil,
						}},
					},
					{
						Name: "comment",
						Type: "Struct",
						Attributes: []*Attribute{{
							Name:       "content",
							Type:       "string",
							Tag:        1,
							Properties: nil,
						}},
					},
				},
			},
		},
		{
			name: "Properties and validation",
			schemaStr: `
context prototype0_blogging {
	record post Struct {
		attribute title string = 1 {
			mutable: false,
			validation: {
				required: false,
				maxLen: 100,
				minLen: 10,
			},
		}
		attribute body string = 2 {
			mutable: true,
			validation: {
				minLen: 100,
				required: true,
				maxLen: 1000,
			},
		}
	}
}`,
			expected: &Schema{
				Context: "prototype0_blogging",
				Records: []*Record{
					{
						Name: "post",
						Type: "Struct",
						Attributes: []*Attribute{{
							Name: "title",
							Type: "string",
							Tag:  1,
							Properties: &Properties{
								Mutable: false,
								ValidationFields: []*ValidationField{{
									// TODO: Required here should be false
									Required: nil,
								}, {
									MaxLength: ptrInt(100),
								}, {
									MinLength: ptrInt(10),
								}},
								Validation: &Validation{
									Required:  false,
									MaxLength: ptrInt(100),
									MinLength: ptrInt(10),
								},
							},
						}, {
							Name: "body",
							Type: "string",
							Tag:  2,
							Properties: &Properties{
								Mutable: true,
								ValidationFields: []*ValidationField{{
									MinLength: ptrInt(100),
								}, {
									Required: ptrBool(true),
								}, {
									MaxLength: ptrInt(1000),
								}},
								Validation: &Validation{
									Required:  true,
									MaxLength: ptrInt(1000),
									MinLength: ptrInt(100),
								},
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

func ptrBool(b bool) *bool {
	return &b
}

func ptrInt(i int) *int {
	return &i
}
