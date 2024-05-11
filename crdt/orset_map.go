package crdt

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dominikbraun/graph"
	"lukechampine.com/blake3"

	"github.com/df8f7892-ba65-4f95-ba24-7918c2a94a0b/prototype0/scalar"
)

type Value interface {
	Type() scalar.Type
	String() (string, bool)
	Int64() (int64, bool)
	Uint64() (uint64, bool)
	Float64() (float64, bool)
	ByteSlice() ([]byte, bool)
	Bool() (bool, bool)
}

type OperationType int

const (
	AddOperation OperationType = iota
	RemoveOperation
)

type (
	Tag struct {
		Sequence uint64
	}
	Tags     map[Tag]bool
	Mutation struct {
		Operations []*Operation
		Parents    []string
		Owner      string
	}
	Operation struct {
		Type  OperationType
		Key   string
		Value Value
		Tags  Tags
		Time  time.Time
	}
	ValueDetail struct {
		Value     Value
		Tombstone bool
	}
	KeyValue struct {
		Tags map[Tag]*ValueDetail
	}
	ORSetMap struct {
		hasher    func(Mutation) string
		sequence  uint64
		mutations map[string]bool // mutations applied
		elements  map[string]*KeyValue
		log       graph.Graph[string, Mutation]
		mu        sync.Mutex
	}
)

// MarshalJSON implements the json.Marshaler interface for the Tags type.
// This is needed for the current dummpy implementation of HashMutation.
// TODO: Remove once a proper hashing function is implemented.
func (t Tags) MarshalJSON() ([]byte, error) {
	m := make(map[string]bool)
	for tag := range t {
		m[fmt.Sprintf("%d", tag.Sequence)] = true
	}
	return json.Marshal(m)
}

func HashMutation(mu Mutation) string {
	// TODO: Implement a proper hashing function
	json, err := json.Marshal(mu)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", blake3.Sum256(json))
}

func (kv *KeyValue) Resolve() Value {
	var maxPriority int
	var maxValue Value
	for tag, value := range kv.Tags {
		if int(tag.Sequence) > maxPriority && !value.Tombstone {
			maxPriority = int(tag.Sequence)
			maxValue = value.Value
		}
	}

	return maxValue
}

func NewORSetMap() *ORSetMap {
	return &ORSetMap{
		hasher:    HashMutation,
		sequence:  0,
		mutations: make(map[string]bool),
		elements:  make(map[string]*KeyValue),
		log: graph.New(
			HashMutation,
			graph.Directed(),
			graph.Acyclic(),
		),
	}
}

func NewMutation(operations ...*Operation) Mutation {
	return Mutation{
		Operations: operations,
	}
}

func (o *ORSetMap) getLeaves() ([]string, error) {
	am, err := o.log.AdjacencyMap()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve adjacency map: %w", err)
	}

	found := map[string]struct{}{}
	for k := range am {
		found[k] = struct{}{}
	}
	for _, v := range am {
		for kk := range v {
			delete(found, kk)
		}
	}

	leaves := []string{}
	for k := range found {
		leaves = append(leaves, k)
	}

	return leaves, nil
}

func (o *ORSetMap) appendMutation(mus ...Mutation) error {
	for _, mu := range mus {
		o.log.AddVertex(mu)
		for _, parent := range mu.Parents {
			o.log.AddEdge(o.hasher(mu), parent)
		}
	}

	// TODO: Rewrite applies. This is very simplistic and inefficient.
	musOrd, err := o.gerOrderedMutations()
	if err != nil {
		return fmt.Errorf("failed to retrieve ordered mutations: %w", err)
	}

	for _, mu := range musOrd {
		muHash := o.hasher(mu)
		mu, err := o.log.Vertex(muHash)
		if err != nil {
			return fmt.Errorf("failed to retrieve mutation with hash %s: %w", muHash, err)
		}
		o.applyMutation(mu)
	}

	return nil
}

func (o *ORSetMap) applyMutation(mu Mutation) {
	if o.mutations[o.hasher(mu)] {
		return
	}

	o.mutations[o.hasher(mu)] = true

	for _, op := range mu.Operations {
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
}

func (o *ORSetMap) gerOrderedMutations() ([]Mutation, error) {
	order, err := graph.TopologicalSort(o.log)
	if err != nil {
		return nil, fmt.Errorf("failed to topologically sort the log: %w", err)
	}

	var mutations []Mutation
	for _, hash := range order {
		mu, err := o.log.Vertex(hash)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve mutation with hash %s: %w", hash, err)
		}
		mutations = append(mutations, mu)
	}

	// reverse mutations
	for i, j := 0, len(mutations)-1; i < j; i, j = i+1, j-1 {
		mutations[i], mutations[j] = mutations[j], mutations[i]
	}

	return mutations, nil
}

func (o *ORSetMap) Add(key string, value Value) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.sequence++

	leaves, err := o.getLeaves()
	if err != nil {
		// TODO: Remove panic
		panic(err)
	}

	op := Operation{
		Type:  AddOperation,
		Key:   key,
		Value: value,
		Tags: map[Tag]bool{
			{Sequence: o.sequence}: true,
		},
		Time: time.Now(),
	}
	mu := Mutation{
		Operations: []*Operation{&op},
		Parents:    leaves,
	}
	o.appendMutation(mu)
}

func (o *ORSetMap) Get(key string) Value {
	o.mu.Lock()
	defer o.mu.Unlock()

	elem, exists := o.elements[key]
	if !exists {
		return nil
	}

	return elem.Resolve()
}

func (o *ORSetMap) Remove(key string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	leaves, err := o.getLeaves()
	if err != nil {
		// TODO: Remove panic
		panic(err)
	}

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
	mu := Mutation{
		Operations: []*Operation{&op},
		Parents:    leaves,
	}

	o.appendMutation(mu)
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

func (o *ORSetMap) List() map[string]Value {
	o.mu.Lock()
	defer o.mu.Unlock()

	result := make(map[string]Value)
	for key, elem := range o.elements {
		value := elem.Resolve()
		if value != nil {
			result[key] = value
		}
	}

	return result
}

func (o *ORSetMap) ExportLog() ([]Mutation, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.gerOrderedMutations()
}

func (o *ORSetMap) ImportLog(mutations []Mutation) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	for _, mu := range mutations {
		o.appendMutation(mu)
	}

	return nil
}
