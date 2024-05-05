package crdt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/df8f7892-ba65-4f95-ba24-7918c2a94a0b/prototype0/scalar"
)

func TestORSetMapOperations(t *testing.T) {
	tests := []struct {
		name          string
		operations    func(set *ORSetMap)
		wantContains  map[string]bool
		wantList      map[string]Value
		testReplicate bool // Indicates if replication via log should be tested
	}{
		{
			name: "Add single elements",
			operations: func(set *ORSetMap) {
				set.Add("fruit", scalar.New("apple"))
				set.Add("computer", scalar.New("laptop"))
				set.Add("car", scalar.New("Toyota"))
			},
			wantContains: map[string]bool{
				"fruit":    true,
				"computer": true,
				"car":      true,
				"bike":     false,
			},
			wantList: map[string]Value{
				"fruit":    scalar.New("apple"),
				"computer": scalar.New("laptop"),
				"car":      scalar.New("Toyota"),
			},
			testReplicate: true,
		},
		{
			name: "Update existing element",
			operations: func(set *ORSetMap) {
				set.Add("fruit", scalar.New("apple"))
				set.Add("fruit", scalar.New("banana"))
			},
			wantContains: map[string]bool{
				"fruit": true,
			},
			wantList: map[string]Value{
				"fruit": scalar.New("banana"),
			},
			testReplicate: true,
		},
		{
			name: "Remove element",
			operations: func(set *ORSetMap) {
				set.Add("fruit", scalar.New("apple"))
				set.Remove("fruit")
			},
			wantContains: map[string]bool{
				"fruit": false,
			},
			wantList:      map[string]Value{},
			testReplicate: true,
		},
		{
			name: "Remove and re-add element",
			operations: func(set *ORSetMap) {
				set.Add("fruit", scalar.New("apple"))
				set.Remove("fruit")
				set.Add("fruit", scalar.New("cherry"))
			},
			wantContains: map[string]bool{
				"fruit": true,
			},
			wantList: map[string]Value{
				"fruit": scalar.New("cherry"),
			},
			testReplicate: true,
		},
		{
			name: "Remove non-existent element",
			operations: func(set *ORSetMap) {
				set.Remove("fruit")
			},
			wantContains: map[string]bool{
				"fruit": false,
			},
			wantList:      map[string]Value{},
			testReplicate: true,
		},
		{
			name: "Add, Update, Remove element",
			operations: func(set *ORSetMap) {
				set.Add("fruit", scalar.New("apple"))
				set.Add("fruit", scalar.New("banana"))
				set.Remove("fruit")
			},
			wantContains: map[string]bool{
				"fruit": false,
			},
			wantList:      map[string]Value{},
			testReplicate: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			orsetMap := NewORSetMap()
			tc.operations(orsetMap)
			for key, expected := range tc.wantContains {
				assert.Equal(t, expected, orsetMap.Contains(key), "Check Contains for key "+key)
			}

			gotList := orsetMap.List()
			assert.Equal(t, tc.wantList, gotList, "Check List matches expected")

			// Test replication if required
			if tc.testReplicate {
				anotherORSetMap := NewORSetMap()
				log := orsetMap.ExportLog()
				anotherORSetMap.ImportLog(log)
				for key, expected := range tc.wantContains {
					assert.Equal(t, expected, anotherORSetMap.Contains(key), "Replicated Check Contains for key "+key)
				}
				gotReplicatedList := anotherORSetMap.List()
				assert.Equal(t, tc.wantList, gotReplicatedList, "Replicated Check List matches expected")
			}
		})
	}
}

func TestORSetMapReplication(t *testing.T) {
	tests := []struct {
		name       string
		operations func() []Operation
		expected   map[string]Value
	}{
		{
			name: "Add and remove single item",
			operations: func() []Operation {
				tag1 := Tag{Sequence: 1}
				return []Operation{
					{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag1: true},
					},
					{
						Type: RemoveOperation,
						Key:  "fruit",
						Tags: map[Tag]bool{tag1: true},
					},
				}
			},
			expected: map[string]Value{},
		},
		{
			name: "Add and remove non existent tag",
			operations: func() []Operation {
				tag1 := Tag{Sequence: 1}
				tag2 := Tag{Sequence: 2}
				return []Operation{
					{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag1: true},
					},
					{
						Type: RemoveOperation,
						Key:  "fruit",
						Tags: map[Tag]bool{tag2: true},
					},
				}
			},
			expected: map[string]Value{
				"fruit": scalar.New("apple"),
			},
		},
		{
			name: "Add multiple items",
			operations: func() []Operation {
				return []Operation{
					{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{Tag{Sequence: 1}: true},
					},
					{
						Type:  AddOperation,
						Key:   "vegetable",
						Value: scalar.New("carrot"),
						Tags:  map[Tag]bool{Tag{Sequence: 2}: true},
					},
				}
			},
			expected: map[string]Value{
				"fruit":     scalar.New("apple"),
				"vegetable": scalar.New("carrot"),
			},
		},
		{
			name: "Update existing item",
			operations: func() []Operation {
				tag1 := Tag{Sequence: 1}
				return []Operation{
					{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag1: true},
					},
					{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("banana"),
						Tags:  map[Tag]bool{tag1: true},
					},
				}
			},
			expected: map[string]Value{
				"fruit": scalar.New("banana"),
			},
		},
		{
			name: "Add item twice, remove only one",
			operations: func() []Operation {
				tag1 := Tag{Sequence: 1}
				tag2 := Tag{Sequence: 2}
				return []Operation{
					{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag1: true},
					},
					{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag2: true},
					},
					{
						Type: RemoveOperation,
						Key:  "fruit",
						Tags: map[Tag]bool{tag1: true},
					},
				}
			},
			expected: map[string]Value{
				"fruit": scalar.New("apple"),
			},
		},
		{
			name: "Add item twice, remove both",
			operations: func() []Operation {
				tag1 := Tag{Sequence: 1}
				tag2 := Tag{Sequence: 2}
				return []Operation{
					{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag1: true},
					},
					{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag2: true},
					},
					{
						Type: RemoveOperation,
						Key:  "fruit",
						Tags: map[Tag]bool{tag1: true},
					},
					{
						Type: RemoveOperation,
						Key:  "fruit",
						Tags: map[Tag]bool{tag2: true},
					},
				}
			},
			expected: map[string]Value{},
		},
	}

	for _, tt := range tests {
		// Create a new ORSetMap for each test case
		orsetMap := NewORSetMap()

		// Generate and apply operations
		operations := tt.operations()
		for _, op := range operations {
			orsetMap.applyOperation(op)
		}

		// Verify the results
		result := orsetMap.List()
		assert.Equal(t, tt.expected, result, fmt.Sprintf("Test case: %s", tt.name))
	}
}
