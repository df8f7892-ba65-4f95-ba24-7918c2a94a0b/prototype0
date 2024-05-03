package crdt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestORSetMapOperations(t *testing.T) {
	tests := []struct {
		name          string
		operations    func(set *ORSetMap)
		wantContains  map[string]bool
		wantList      map[string]interface{}
		testReplicate bool // Indicates if replication via log should be tested
	}{
		{
			name: "Add single elements",
			operations: func(set *ORSetMap) {
				set.Add("fruit", "apple")
				set.Add("computer", "laptop")
				set.Add("car", "Toyota")
			},
			wantContains: map[string]bool{
				"fruit":    true,
				"computer": true,
				"car":      true,
				"bike":     false,
			},
			wantList: map[string]interface{}{
				"fruit":    "apple",
				"computer": "laptop",
				"car":      "Toyota",
			},
			testReplicate: true,
		},
		{
			name: "Update existing element",
			operations: func(set *ORSetMap) {
				set.Add("fruit", "apple")
				set.Add("fruit", "banana")
			},
			wantContains: map[string]bool{
				"fruit": true,
			},
			wantList: map[string]interface{}{
				"fruit": "banana",
			},
			testReplicate: true,
		},
		{
			name: "Remove element",
			operations: func(set *ORSetMap) {
				set.Add("fruit", "apple")
				set.Remove("fruit")
			},
			wantContains: map[string]bool{
				"fruit": false,
			},
			wantList:      map[string]interface{}{},
			testReplicate: true,
		},
		{
			name: "Remove and re-add element",
			operations: func(set *ORSetMap) {
				set.Add("fruit", "apple")
				set.Remove("fruit")
				set.Add("fruit", "cherry")
			},
			wantContains: map[string]bool{
				"fruit": true,
			},
			wantList: map[string]interface{}{
				"fruit": "cherry",
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
			wantList:      map[string]interface{}{},
			testReplicate: true,
		},
		{
			name: "Add, Update, Remove element",
			operations: func(set *ORSetMap) {
				set.Add("fruit", "apple")
				set.Add("fruit", "banana")
				set.Remove("fruit")
			},
			wantContains: map[string]bool{
				"fruit": false,
			},
			wantList:      map[string]interface{}{},
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
