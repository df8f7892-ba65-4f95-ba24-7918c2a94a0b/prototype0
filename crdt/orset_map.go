package crdt

import (
	"sync"
	"time"
)

type OperationType int

const (
	AddOperation OperationType = iota
	RemoveOperation
)

type Tag struct {
	// TODO: Add user id
	Sequence uint64
}

type Operation struct {
	Type  OperationType
	Key   string
	Value interface{}
	Tags  map[Tag]bool
	Time  time.Time
}

type ValueDetail struct {
	Value     interface{}
	Tombstone bool
}

type KeyValue struct {
	Tags map[Tag]*ValueDetail
}

type ORSetMap struct {
	sequence uint64
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
		// TODO: Can there be more than one tags on an add operation?
		elem, exists := o.elements[op.Key]
		if !exists {
			o.elements[op.Key] = &KeyValue{
				Tags: map[Tag]*ValueDetail{},
			}
			elem = o.elements[op.Key]
		}
		for tag := range op.Tags {
			elem.Tags[tag] = &ValueDetail{
				Value: op.Value,
			}
		}
	case RemoveOperation:
		if elem, ok := o.elements[op.Key]; ok {
			for tag := range op.Tags {
				if value, exists := elem.Tags[tag]; exists {
					value.Tombstone = true
				}
			}
		}
	}
}

func (o *ORSetMap) Add(key string, value interface{}) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.sequence++

	newTag := Tag{
		Sequence: o.sequence,
	}

	op := Operation{
		Type:  AddOperation,
		Key:   key,
		Value: value,
		Tags:  map[Tag]bool{newTag: true},
		Time:  time.Now(),
	}
	o.log = append(o.log, op)
	o.applyOperation(op)
}

func (o *ORSetMap) Remove(key string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	tagsToBeRemoved := make(map[Tag]bool)
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
	if !exists {
		return false
	}

	for _, value := range elem.Tags {
		if !value.Tombstone {
			return true
		}
	}

	return false
}

func (o *ORSetMap) List() map[string]interface{} {
	o.mu.Lock()
	defer o.mu.Unlock()

	result := make(map[string]interface{})
	for key, elem := range o.elements {
		var maxPriority int
		var maxValue interface{}
		for tag, value := range elem.Tags {
			if int(tag.Sequence) > maxPriority && !value.Tombstone {
				maxPriority = int(tag.Sequence)
				maxValue = value.Value
			}
		}
		if maxValue != nil {
			result[key] = maxValue
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
