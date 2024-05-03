package crdt

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type OperationType int

const (
	AddOperation OperationType = iota
	RemoveOperation
)

// Operation represents an operation in the log
type Operation struct {
	Type  OperationType
	Key   string
	Value interface{} // Value is nil for remove operations
	Time  time.Time
}

// KeyValue represents a key with its value in the ORSetMap
type KeyValue struct {
	Value     interface{}
	Tags      map[uuid.UUID]bool
	Tombstone bool
}

// ORSetMap is a map of key-value pairs that supports add and remove operations
type ORSetMap struct {
	elements map[string]*KeyValue
	log      []Operation
	mu       sync.Mutex
}

// NewORSetMap initializes a new ORSetMap
func NewORSetMap() *ORSetMap {
	return &ORSetMap{
		elements: make(map[string]*KeyValue),
		log:      []Operation{},
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
		elem = &KeyValue{
			Value:     value,
			Tags:      map[uuid.UUID]bool{uuid.New(): true},
			Tombstone: false,
		}
		o.elements[key] = elem
	} else {
		elem.Value = value
		elem.Tags = map[uuid.UUID]bool{uuid.New(): true}
	}

	o.log = append(o.log, Operation{
		Type:  AddOperation,
		Key:   key,
		Value: value,
		Time:  time.Now(),
	})
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

	o.log = append(o.log, Operation{
		Type: RemoveOperation,
		Key:  key,
		Time: time.Now(),
	})
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

// ExportLog exports the operation log
func (o *ORSetMap) ExportLog() []Operation {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]Operation(nil), o.log...)
}

// ImportLog imports an operation log and applies it to the ORSetMap
func (o *ORSetMap) ImportLog(operations []Operation) {
	for _, op := range operations {
		if op.Type == AddOperation {
			o.Add(op.Key, op.Value)
		} else if op.Type == RemoveOperation {
			o.Remove(op.Key)
		}
	}
}
