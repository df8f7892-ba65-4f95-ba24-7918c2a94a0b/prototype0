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
	Value interface{}
	Tags  map[uuid.UUID]bool
	Time  time.Time
}

// KeyValue represents a key with its value in the ORSetMap
type KeyValue struct {
	Value     interface{}
	Tags      map[uuid.UUID]bool
	Tombstone bool
}

// ORSetMap is an enhanced version of the ORSet where each key has one value and operation logs
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

func (o *ORSetMap) applyOperation(op Operation) {
	switch op.Type {
	case AddOperation:
		elem, exists := o.elements[op.Key]
		if !exists || elem.Tombstone {
			// Create a new element or revive a tombstoned element
			o.elements[op.Key] = &KeyValue{
				Value:     op.Value,
				Tags:      op.Tags,
				Tombstone: false,
			}
		} else {
			// Update existing element
			elem.Value = op.Value
			for tag := range op.Tags {
				elem.Tags[tag] = true // Merge tags
			}
		}
	case RemoveOperation:
		if elem, ok := o.elements[op.Key]; ok {
			for tag := range op.Tags {
				delete(elem.Tags, tag) // Remove tags
			}
			if len(elem.Tags) == 0 {
				elem.Tombstone = true
			}
		}
	}
}

// Add creates an operation for adding or updating a key and applies it
func (o *ORSetMap) Add(key string, value interface{}) {
	o.mu.Lock()
	defer o.mu.Unlock()

	newTags := map[uuid.UUID]bool{uuid.New(): true}
	op := Operation{
		Type:  AddOperation,
		Key:   key,
		Value: value,
		Tags:  newTags,
		Time:  time.Now(),
	}
	o.log = append(o.log, op)
	o.applyOperation(op)
}

// Remove creates an operation for removing a key and applies it
func (o *ORSetMap) Remove(key string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	tagsToBeRemoved := make(map[uuid.UUID]bool)
	if elem, exists := o.elements[key]; exists {
		for tag := range elem.Tags {
			tagsToBeRemoved[tag] = true // Capture existing tags for the operation log
		}
	}
	op := Operation{
		Type: RemoveOperation,
		Key:  key,
		Tags: tagsToBeRemoved,
		Time: time.Now(),
	}
	o.log = append(o.log, op)
	o.applyOperation(op)
}

// Contains checks if a key is present in the ORSetMap and not marked as a tombstone
func (o *ORSetMap) Contains(key string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	elem, exists := o.elements[key]
	return exists && !elem.Tombstone
}

// List returns all key-value pairs in the ORSetMap that are not marked as tombstones
func (o *ORSetMap) List() map[string]interface{} {
	o.mu.Lock()
	defer o.mu.Unlock()

	result := make(map[string]interface{})
	for key, elem := range o.elements {
		if !elem.Tombstone {
			result[key] = elem.Value
		}
	}
	return result
}

// ExportLog exports the operation log
func (o *ORSetMap) ExportLog() []Operation {
	o.mu.Lock()
	defer o.mu.Unlock()

	copiedLog := make([]Operation, len(o.log))
	copy(copiedLog, o.log)
	return copiedLog
}

// ImportLog imports an operation log and applies it directly
func (o *ORSetMap) ImportLog(operations []Operation) {
	for _, op := range operations {
		o.applyOperation(op)
	}
}
