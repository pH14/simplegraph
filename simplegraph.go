package simplegraph

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Query struct {
	subject, predicate, object []byte
	subjectVariable, predicateVariable, objectVariable string
}
//
//type Edge interface {
//	Subject() []byte
//	Predicate() []byte
//	Object() []byte
//}

type EdgeResult struct {
	edge *Edge
	variables map[DataField]*VariableResult
}

func (e *EdgeResult) Edge() *Edge {
	return e.edge
}

type Edge struct {
	subject, predicate, object []byte
}

func (e Edge) String() string {
	return fmt.Sprintf("Edge[subject: %v, predicate: %v, object: %v]", string(e.subject), string(e.predicate), string(e.object))
}

func (e Edge) toBytes() []byte {
	return tuple.Tuple{ e.subject, e.predicate, e.object }.Pack()
}

func (e Edge) toComparisonBytes(comparisonOrdering []DataField) []byte {
	comparisonTuple := make(tuple.Tuple, 2)

	for i, dataField := range comparisonOrdering {
		switch dataField {
		case SUBJECT:
			comparisonTuple[i] = e.subject
		case PREDICATE:
			comparisonTuple[i] = e.predicate
		case OBJECT:
			comparisonTuple[i] = e.object
		}

	}

	return comparisonTuple.Pack()
}

func fromBytes(bytes []byte) *Edge {
	tuples, e := tuple.Unpack(bytes)

	if e != nil {
		panic(e)
	}

	return &Edge {
		subject: tuples[0].([]byte),
		predicate: tuples[1].([]byte),
		object: tuples[2].([]byte),
	}
}

type SimpleGraph struct {
	kvstore KVStore
}

func NewSimpleGraph(kvstore KVStore) *SimpleGraph {
	return &SimpleGraph {
		kvstore: kvstore,
	}
}

func (graph *SimpleGraph) AddEdges(edges []Edge) error {
	kvKeys := make([][]byte, len(edges)*len(Indices))

	i := 0
	for _, edge := range edges {
		for _, index := range Indices {
			kvKeys[i] = index.toBytes(&edge)
			i++
		}
	}

	return graph.kvstore.Put(kvKeys ...)
}

func (graph *SimpleGraph) GetEdges(query Query) (<-chan *Edge, error){
	parsedQuery := transformQuery(query)
	idx := findIndices(parsedQuery)
	return graph._getRangeStreaming(query, idx[0])
}

func (graph *SimpleGraph) _getRangeStreaming(query Query, idx *hexastoreIndex) (<-chan *Edge, error){
	queryRange := idx.toRangeFromQuery(transformQuery(query))

	kvs := make(chan []byte)
	edges := make(chan *Edge)

	go func(rawKVStream chan<- []byte) {
		e := graph.kvstore.Get(queryRange, rawKVStream)
		if e != nil {
			panic(e)
		}
	}(kvs)

	go func(rawKVStream <-chan []byte, edgeOutput chan<- *Edge) {
		defer close(edgeOutput)

		for rawKey := range rawKVStream {
			edge, e := idx.fromBytes(rawKey)

			if e != nil {
				panic(e)
			}

			edgeOutput <- edge
		}
	}(kvs, edges)

	return edges, nil
}

func (graph *SimpleGraph) GetRangeStreamingAnd(query1 Query, query2 Query) (<-chan *Edge, error){
	idx1, idx2 := findIndexPair(transformQuery(query1), transformQuery(query2))

	// EZ: Can answer with the most specific index, which has already been selected
	if idx1 == idx2 {
		fmt.Printf("Same index: %v\n", idx1)
		return graph._getRangeStreaming(query1, idx1)
	}

	fmt.Printf("Choosing index: %v then %v\n", idx1, idx2)

	stream1, e := graph._getRangeStreaming(query1, idx1)

	if e != nil {
		return nil, e
	}

	stream2, e := graph._getRangeStreaming(query2, idx2)

	if e != nil {
		return nil, e
	}

	join := OrderedStreamJoin{
		tripleOrder: TripleOrder{ dataFieldOrder: []DataField{idx1.ordering[1], idx1.ordering[2] } },
	}

	return join.join(stream1, stream2), nil
}

func (graph *SimpleGraph) Search(query Query) (<-chan *SearchResults, error){
	edges, e := graph.GetEdges(query)

	if e != nil {
		return nil, e
	}

	stream := VariableStream{
		variables: query.toVariableMap(),
	}

	return stream.join(edges), nil
}

func (query *Query) toVariableMap() map[DataField]string {
	variables := make(map[DataField]string)

	if s := query.subjectVariable; s != "" {
		variables[SUBJECT] = s
	}

	if p := query.predicateVariable; p != "" {
		variables[PREDICATE] = p
	}

	if o := query.objectVariable; o != "" {
		variables[OBJECT] = o
	}

	return variables
}

type DataField int

const (
	SUBJECT   DataField = 1
	PREDICATE DataField = 2
	OBJECT    DataField = 3
)

type hexastoreIndex struct {
	ss       subspace.Subspace
	ordering []DataField
}

var Indices = map[string]*hexastoreIndex {
	"spo": { subspace.Sub("spo"), []DataField{SUBJECT, PREDICATE, OBJECT }, },
	"sop": { subspace.Sub("sop"), []DataField{SUBJECT, OBJECT, PREDICATE }, },
	"pos": { subspace.Sub("pos"), []DataField{PREDICATE, OBJECT, SUBJECT }, },
	"pso": { subspace.Sub("pso"), []DataField{PREDICATE, SUBJECT, OBJECT }, },
	"osp": { subspace.Sub("osp"), []DataField{OBJECT, SUBJECT, PREDICATE }, },
	"ops": { subspace.Sub("ops"), []DataField{OBJECT, PREDICATE, SUBJECT }, },
}

func (idx hexastoreIndex) toBytes(edge *Edge) []byte {
	indexTuple := make(tuple.Tuple, 3)

	for i, dataField := range idx.ordering {
		switch et := dataField; et {
		case SUBJECT:
			indexTuple[i] = edge.subject
		case PREDICATE:
			indexTuple[i] = edge.predicate
		case OBJECT:
			indexTuple[i] = edge.object
		default:
			panic(fmt.Sprintf("Unknown element type: %v", et))
		}
	}

	return idx.ss.Pack(indexTuple)
}

func (idx hexastoreIndex) fromBytes(kvBytes []byte) (*Edge, error) {
	unpacked, e := tuple.Unpack(kvBytes)

	if e != nil {
		return nil, e
	}

	edge := Edge{}

	for i, dataField := range idx.ordering {
		switch dataField {
		// add 1 to tuple index to account for index subspace entry
		case SUBJECT:
			edge.subject = unpacked[i + 1].([]byte)
		case PREDICATE:
			edge.predicate = unpacked[i + 1].([]byte)
		case OBJECT:
			edge.object = unpacked[i + 1].([]byte)
		}
	}

	return &edge, nil
}

func (idx hexastoreIndex) toRangeFromQuery(query map[DataField][]byte) []byte {
	indexTuple := make(tuple.Tuple, 0)

	for _, dataField := range idx.ordering {
		if v := query[dataField]; v == nil {
			break
		} else {
			indexTuple = append(indexTuple, v)
		}
	}

	return idx.ss.Pack(indexTuple)
}

func (idx hexastoreIndex) matchDepth(query map[DataField][]byte) int {
	depthOfMatch := 0

	for _, dataField := range idx.ordering {
		if len(query[dataField]) > 0 {
			depthOfMatch++
		} else {
			break
		}
	}

	return depthOfMatch
}

func (idx hexastoreIndex) locationOf()
