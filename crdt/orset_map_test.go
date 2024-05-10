package crdt

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/df8f7892-ba65-4f95-ba24-7918c2a94a0b/prototype0/scalar"
)

func TestORSetMapmutations(t *testing.T) {
	tests := []struct {
		name          string
		mutations     func(set *ORSetMap)
		wantContains  map[string]bool
		wantList      map[string]Value
		testReplicate bool // Indicates if replication via log should be tested
	}{
		{
			name: "Add single elements",
			mutations: func(set *ORSetMap) {
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
			mutations: func(set *ORSetMap) {
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
			mutations: func(set *ORSetMap) {
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
			mutations: func(set *ORSetMap) {
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
			mutations: func(set *ORSetMap) {
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
			mutations: func(set *ORSetMap) {
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
			tc.mutations(orsetMap)
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
		name      string
		mutations func() []Mutation
		expected  map[string]Value
	}{
		{
			name: "Add and remove single item",
			mutations: func() []Mutation {
				tag1 := Tag{Sequence: 1}
				return []Mutation{
					NewMutation(&Operation{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag1: true},
					}),
					NewMutation(&Operation{
						Type: RemoveOperation,
						Key:  "fruit",
						Tags: map[Tag]bool{tag1: true},
					}),
				}
			},
			expected: map[string]Value{},
		},
		{
			name: "Add and remove non existent tag",
			mutations: func() []Mutation {
				tag1 := Tag{Sequence: 1}
				tag2 := Tag{Sequence: 2}
				return []Mutation{
					NewMutation(&Operation{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag1: true},
					}),
					NewMutation(&Operation{
						Type: RemoveOperation,
						Key:  "fruit",
						Tags: map[Tag]bool{tag2: true},
					}),
				}
			},
			expected: map[string]Value{
				"fruit": scalar.New("apple"),
			},
		},
		{
			name: "Add multiple items",
			mutations: func() []Mutation {
				return []Mutation{
					NewMutation(&Operation{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{{Sequence: 1}: true},
					}),
					NewMutation(&Operation{
						Type:  AddOperation,
						Key:   "vegetable",
						Value: scalar.New("carrot"),
						Tags:  map[Tag]bool{{Sequence: 2}: true},
					}),
				}
			},
			expected: map[string]Value{
				"fruit":     scalar.New("apple"),
				"vegetable": scalar.New("carrot"),
			},
		},
		{
			name: "Update existing item",
			mutations: func() []Mutation {
				tag1 := Tag{Sequence: 1}
				return []Mutation{
					NewMutation(&Operation{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag1: true},
					}),
					NewMutation(&Operation{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("banana"),
						Tags:  map[Tag]bool{tag1: true},
					}),
				}
			},
			expected: map[string]Value{
				"fruit": scalar.New("banana"),
			},
		},
		{
			name: "Add item twice, remove only one",
			mutations: func() []Mutation {
				tag1 := Tag{Sequence: 1}
				tag2 := Tag{Sequence: 2}
				return []Mutation{
					NewMutation(&Operation{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag1: true},
					}),
					NewMutation(&Operation{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag2: true},
					}),
					NewMutation(&Operation{
						Type: RemoveOperation,
						Key:  "fruit",
						Tags: map[Tag]bool{tag1: true},
					}),
				}
			},
			expected: map[string]Value{
				"fruit": scalar.New("apple"),
			},
		},
		{
			name: "Add item twice, remove both",
			mutations: func() []Mutation {
				tag1 := Tag{Sequence: 1}
				tag2 := Tag{Sequence: 2}
				return []Mutation{
					NewMutation(&Operation{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag1: true},
					}),
					NewMutation(&Operation{
						Type:  AddOperation,
						Key:   "fruit",
						Value: scalar.New("apple"),
						Tags:  map[Tag]bool{tag2: true},
					}),
					NewMutation(&Operation{
						Type: RemoveOperation,
						Key:  "fruit",
						Tags: map[Tag]bool{tag1: true},
					}),
					NewMutation(&Operation{
						Type: RemoveOperation,
						Key:  "fruit",
						Tags: map[Tag]bool{tag2: true},
					}),
				}
			},
			expected: map[string]Value{},
		},
	}

	for _, tt := range tests {
		// Create a new ORSetMap for each test case
		orsetMap := NewORSetMap()

		// Generate and apply mutations
		mutations := tt.mutations()
		for _, mu := range mutations {
			orsetMap.applyMutation(mu)
		}

		// Verify the results
		result := orsetMap.List()
		assert.Equal(t, tt.expected, result, fmt.Sprintf("Test case: %s", tt.name))
	}
}

func BenchmarkORSetMap(b *testing.B) {
	const numKeys = 100
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	b.Run("Add", func(b *testing.B) {
		orSetMap := NewORSetMap()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			orSetMap.Add(keys[i%numKeys], scalar.New("v1"))
		}
	})

	b.Run("Get", func(b *testing.B) {
		orSetMap := NewORSetMap()
		// Prepopulate the map with values
		for i := 0; i < numKeys; i++ {
			orSetMap.Add(keys[i], scalar.New("v1"))
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = orSetMap.Get(keys[i%numKeys])
		}
	})

	b.Run("Remove", func(b *testing.B) {
		orSetMap := NewORSetMap()
		// Prepopulate the map with values
		for i := 0; i < numKeys; i++ {
			orSetMap.Add(keys[i], scalar.New("v1"))
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			orSetMap.Remove(keys[i%numKeys])
		}
	})
}
