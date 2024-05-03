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

type Operation struct {
	Type  OperationType
	Key   string
	Value interface{}
	Tags  map[uuid.UUID]bool
	Time  time.Time
}

type KeyValue struct {
	Value interface{}
	Tags  map[uuid.UUID]bool
}

type ORSetMap struct {
	elements map[string]*KeyValue
	log      []Operation
	mu       sync.Mutex
}

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
		if !exists {
			o.elements[op.Key] = &KeyValue{
				Value: op.Value,
				Tags:  op.Tags,
			}
		} else {
			elem.Value = op.Value
			for tag := range op.Tags {
				elem.Tags[tag] = true
			}
		}
	case RemoveOperation:
		if elem, ok := o.elements[op.Key]; ok {
			for tag := range op.Tags {
				delete(elem.Tags, tag)
			}
		}
	}
}

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

func (o *ORSetMap) Remove(key string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	tagsToBeRemoved := make(map[uuid.UUID]bool)
	if elem, exists := o.elements[key]; exists {
		for tag := range elem.Tags {
			tagsToBeRemoved[tag] = true
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

func (o *ORSetMap) Contains(key string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	elem, exists := o.elements[key]
	return exists && len(elem.Tags) > 0
}

func (o *ORSetMap) List() map[string]interface{} {
	o.mu.Lock()
	defer o.mu.Unlock()

	result := make(map[string]interface{})
	for key, elem := range o.elements {
		if len(elem.Tags) > 0 {
			result[key] = elem.Value
		}
	}
	return result
}

func (o *ORSetMap) ExportLog() []Operation {
	o.mu.Lock()
	defer o.mu.Unlock()

	copiedLog := make([]Operation, len(o.log))
	copy(copiedLog, o.log)
	return copiedLog
}

func (o *ORSetMap) ImportLog(operations []Operation) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for _, op := range operations {
		o.applyOperation(op)
	}
}
