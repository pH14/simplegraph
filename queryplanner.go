package simplegraph

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

/**
1. paul | friend | x           object ordering, either spo or pso

2. x | friend | y              so | os ordering, either pso or pos

3. y | friend | jess           subject ordering, either pos or ops


If 1 is SPO, could emit tuples "{ x: o }" -- VariableStream (spo --> {"x"})

Then 2: consumes 1 && another stream with pso order (object ordering in 1 becomes subject order in 2)
	emits { x: s, y: o }

	-- AndStream(pso|pos, VariableStream)

Then 3:  consumes 2 and another stream either pos or ops, doesn't matter




If 3 is POS could emit tuples "_ | _| y"

2 consumes both... needs to maintain

1. paul | friend | x           object ordering, but want to sort on object only
2. paul | abc | y              object ordering, but want to sort on object only





 */

func transformQuery(query Query) map[DataField][]byte {
	dataFieldsInQuery := make(map[DataField][]byte)

	if query.subject != nil {
		dataFieldsInQuery[SUBJECT] = query.subject
	}

	if query.predicate != nil {
		dataFieldsInQuery[PREDICATE] = query.predicate
	}

	if query.object != nil {
		dataFieldsInQuery[OBJECT] = query.object
	}

	return dataFieldsInQuery
}

func findIndices(query map[DataField][]byte) (indices []*hexastoreIndex, matchDepth int) {
	maxDepthFound := 0
	var candidateIndices []*hexastoreIndex

	for _, index := range Indices {
		matchDepth := index.matchDepth(query)

		if matchDepth > maxDepthFound {
			maxDepthFound = matchDepth
			candidateIndices = candidateIndices[:0]
			candidateIndices = append(candidateIndices, index)
		} else if matchDepth > 0 && matchDepth == maxDepthFound {
			candidateIndices = append(candidateIndices, index)
		}
	}

	if maxDepthFound == 0 {
		panic("no index found for query. were any fields set?")
	}

	return candidateIndices, maxDepthFound
}

type queryPlan struct {
	source tripleSource
}

type tripleSource interface {
	getTripleOrder() *TripleOrder
}

type indexScanSource struct {
	query *Query
	idx *hexastoreIndex
	tripleOrder *TripleOrder
}

func (iss *indexScanSource) getTripleOrder() *TripleOrder {
	return iss.tripleOrder
}

type bufferSortedSource struct {
	tripleSource
	tripleOrder *TripleOrder
}

func (bss *bufferSortedSource) getTripleOrder() *TripleOrder {
	return bss.tripleOrder
}

type mergeJoin struct {
	joins []tripleSource
	tripleOrder *TripleOrder
}

func (lmj *mergeJoin) getTripleOrder() *TripleOrder {
	return lmj.tripleOrder
}

// ("paul", a, b) | ("paul", b, a) -- want spo x sop. AB ordering output, S fixed, PO order. next stage choose object, predicate to get same AB ordering
// ("paul", _, _)

func generateQueryPlan(queries ... Query) *queryPlan {


	OUTER:
	for i := 0; i < len(queries) - 1; i++ {
		thisQuery := queries[i]
		nextQuery := queries[i+1]

		possibleIndicesThisQuery, matchDepthThisQuery := findIndices(transformQuery(thisQuery))
		possibleIndicesNextQuery, matchDepthNextQuery := findIndices(transformQuery(nextQuery))

		variableMapThisQuery := thisQuery.toVariableMap()
		variableMapNextQuery := nextQuery.toVariableMap()

		var commonVariables []string

		for _, v1 := range variableMapThisQuery {
			for _, v2 := range variableMapNextQuery {
				if v1 == v2 {
					commonVariables = append(commonVariables, v1)
				}
			}
		}

		for _, idxThisQuery := range possibleIndicesThisQuery {
			var variableOrderingThisQuery []string
			var variableOrderingNextQuery []string

			for _, indexOrderedDataField := range idxThisQuery.ordering {
				v1 := variableMapThisQuery[indexOrderedDataField]

				if contains(v1, commonVariables) {
					variableOrderingThisQuery = append(variableMapThisQuery, v1)
				}
			}

			hasOrderingMatch := false
			for _, idxNextQuery := range possibleIndicesNextQuery {
				for _, indexOrderedDataField := range idxNextQuery.ordering {
					v2 := variableMapNextQuery[indexOrderedDataField]

					if contains(v2, commonVariables) {
						variableOrderingNextQuery = append(variableOrderingNextQuery, v2)
					}
				}

				// now we have orderings of common variables + possible indices.
				// if the variable orderings are the same


				for i := range variableOrderingThisQuery {
					if i > len(variableOrderingNextQuery) {
						continue
					}

					if variableOrderingThisQuery[i] != variableOrderingNextQuery[i] {

					}
				}

				continue OUTER
			}
		}
	}
	
	

}

func contains(variable string, commonVariables []string) bool {
	for _, cv := range commonVariables {
		if cv == variable {
			return true
		}
	}

	return false
}

func findIndexPairV2(query1, query2 map[DataField][]byte) (idx1 *hexastoreIndex, idx2 *hexastoreIndex) {
	indices1, depth1 := findIndices(query1)
	indices2, depth2 := findIndices(query2)

	matchDepth1 := indices1[0].matchDepth(query1)
	matchDepth2 := indices2[0].matchDepth(query2)

	source := &indexScanSource{}

	plan := queryPlan{
		source: &mergeJoin{
			joins: []tripleSource{ &bufferSortedSource{source: source}, source, source  },
		},
	}

	fmt.Printf("%v", plan)

	/*
	use depth to find constrained fields and unconstrained fields (variables)

	of the constrained fields, we know these are our fixed range prefixes
		if two constrained fields: index we choose doesn't matter

	if one unconstrained field:
		order s.t. the VARIABLE ORDERING is the same in the next stream

	e.g. in (paul, friend, x) piped to (x, friend, y)
		for first stage: we know S and P are fixed. can use either SPO or PSO
		this yields results in X ordering

		for the second stage, we know P is fixed, so now we choose between PSO and POS
		but we know the input is in X ordering, so we want to match that.
		here, X is the Subject. So therefore we choose PSO

		then say (x, friend, y) | (y, friend, jess)

		we know P and O are fixed, so we have POS and OPS to choose from.
		we know that the input is in XY ordering, which is SO

		we don't have an available ordering for this, so we must collect the results into memory
		and sort them to Y ordering to match plan for next constraint


		output:
			stage 1: SPO | PSO, with object x variable
			stage 2: PSO, with object x y variables, sort stream by Y (OS or OP depending on next index)
			stage 3: POS | OPS, with variable y
	 */



	dataField1 := indices1[0].ordering[0]
	dataField2 := indices2[0].ordering[0]

	// can answer both by scanning one keyrange
	// can join these streams together using the most selective one.
	// separate db streams from logic joinsd
	if dataField1 == dataField2 {
		if matchDepth1 >= matchDepth2 {
			// this could also be randomized since we want to spread load across both indices potentially
			return indices1[0], indices1[0]
		} else {
			return indices2[1], indices2[1]
		}
	} else {
		// we want the same sorting order between the two, in so much as is possible
		// easiest way to think of this is as a palindrome: if we choose `spo` for 1, we want `pos` for 2
		// since `po` will be in sorted order relative to s in each case, and we'll only be scanning over one `s`

		for _, idx1Candidate := range indices1 {
			for _, idx2Candidate := range indices2 {
				if idx1Candidate.ordering[1] == idx2Candidate.ordering[0] &&
					idx1Candidate.ordering[2] == idx2Candidate.ordering[1] {
					return idx1Candidate, idx2Candidate
				}
			}
		}
	}

	panic(fmt.Sprintf("couldnt find indices for %v, %v", query1, query2))
}


func findIndexPair(query1, query2 map[DataField][]byte) (idx1 *hexastoreIndex, idx2 *hexastoreIndex) {
	indices1 := findIndices(query1)
	indices2 := findIndices(query2)

	matchDepth1 := indices1[0].matchDepth(query1)
	matchDepth2 := indices2[0].matchDepth(query2)

	dataField1 := indices1[0].ordering[0]
	dataField2 := indices2[0].ordering[0]

	// can answer both by scanning one keyrange
	// can join these streams together using the most selective one.
	// separate db streams from logic joinsd
	if dataField1 == dataField2 {
		if matchDepth1 >= matchDepth2 {
			// this could also be randomized since we want to spread load across both indices potentially
			return indices1[0], indices1[0]
		} else {
			return indices2[1], indices2[1]
		}
	} else {
		// we want the same sorting order between the two, in so much as is possible
		// easiest way to think of this is as a palindrome: if we choose `spo` for 1, we want `pos` for 2
		// since `po` will be in sorted order relative to s in each case, and we'll only be scanning over one `s`

		for _, idx1Candidate := range indices1 {
			for _, idx2Candidate := range indices2 {
				if idx1Candidate.ordering[1] == idx2Candidate.ordering[0] &&
					idx1Candidate.ordering[2] == idx2Candidate.ordering[1] {
					return idx1Candidate, idx2Candidate
				}
			}
		}
	}

	panic(fmt.Sprintf("couldnt find indices for %v, %v", query1, query2))
}

type VariableResult struct {
	name string
	value []byte
}

type SearchResults struct {
	edge      *Edge
	variables map[DataField]*VariableResult
}

type TripleOrder struct {
	dataFieldOrder []DataField
}

func (to TripleOrder) fromEdge(edge *Edge) []byte {
	comparisonTuple := make(tuple.Tuple, 2)

	for i, dataField := range to.dataFieldOrder {
		switch dataField {
		case SUBJECT:
			comparisonTuple[i] = edge.subject
		case PREDICATE:
			comparisonTuple[i] = edge.predicate
		case OBJECT:
			comparisonTuple[i] = edge.object
		}
	}

	return comparisonTuple.Pack()
}

func (to TripleOrder) fromVariables(variables map[DataField]*VariableResult) []byte {
	comparisonTuple := make(tuple.Tuple, len(to.dataFieldOrder))

	for i, dataField := range to.dataFieldOrder {
		if variables[dataField].value == nil {
			panic(fmt.Sprintf("bad argument, variables asked to sort on %v without field specified. was %v, sort %v\n",
				dataField,
				variables,
				to.dataFieldOrder))
		}

		comparisonTuple[i] = variables[dataField].value
	}

	return comparisonTuple.Pack()
}

