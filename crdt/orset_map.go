package crdt

import (
	"sync"

	"github.com/google/uuid"
)

// KeyValue represents a key with its value in the ORSetMap
type KeyValue struct {
	Value     interface{}
	Tags      map[uuid.UUID]bool
	Tombstone bool
}

// ORSetMap is a map of key-value pairs that supports add and remove operations
type ORSetMap struct {
	elements map[string]*KeyValue
	mu       sync.Mutex
}

// NewORSetMap initializes a new ORSetMap
func NewORSetMap() *ORSetMap {
	return &ORSetMap{
		elements: make(map[string]*KeyValue),
	}
}

// Add inserts or updates a key with a value into the ORSetMap
func (o *ORSetMap) Add(key string, value interface{}) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if elem, ok := o.elements[key]; ok && elem.Tombstone {
		// Reset if tombstoned
		elem.Tombstone = false
		elem.Value = value
		elem.Tags = map[uuid.UUID]bool{uuid.New(): true}
	} else if !ok {
		// Add new KeyValue if not exist
		o.elements[key] = &KeyValue{
			Value:     value,
			Tags:      map[uuid.UUID]bool{uuid.New(): true},
			Tombstone: false,
		}
	} else {
		// Update existing KeyValue
		elem.Value = value
		elem.Tags = map[uuid.UUID]bool{uuid.New(): true}
	}
}

// Remove deletes a key from the ORSetMap
func (o *ORSetMap) Remove(key string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if elem, ok := o.elements[key]; ok {
		// Mark as tombstone and remove all tags
		elem.Tombstone = true
		elem.Tags = make(map[uuid.UUID]bool)
	}
}

// Contains checks if a key is present in the ORSetMap
func (o *ORSetMap) Contains(key string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	elem, exists := o.elements[key]
	return exists && !elem.Tombstone && len(elem.Tags) > 0
}

// List returns all key-value pairs in the ORSetMap
func (o *ORSetMap) List() map[string]interface{} {
	o.mu.Lock()
	defer o.mu.Unlock()

	result := make(map[string]interface{})
	for key, elem := range o.elements {
		if len(elem.Tags) > 0 && !elem.Tombstone {
			result[key] = elem.Value
		}
	}
	return result
}
