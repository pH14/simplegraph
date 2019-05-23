package simplegraph

type VariableStream struct {
	variables map[DataField]string
}

func (ss *VariableStream) join(input <-chan *Edge) <-chan *SearchResults {
	output := make(chan *SearchResults)

	go func(output chan<- *SearchResults) {
		defer close(output)
		for edge := range input {
			output <- ss.toSearchResult(edge)
		}
	}(output)

	return output
}

func (ss *VariableStream) toSearchResult(edge *Edge) *SearchResults {
	results := make(map[DataField]*VariableResult)

	for elementType, variableName := range ss.variables {
		switch elementType {
		case SUBJECT:
			results[SUBJECT] = &VariableResult{
				name: variableName,
				value: edge.subject,
			}
		case PREDICATE:
			results[PREDICATE] = &VariableResult{
				name:  variableName,
				value: edge.predicate,
			}
		case OBJECT:
			results[OBJECT] = &VariableResult{
				name: variableName,
				value: edge.object,
			}
		}
	}

	return &SearchResults{ edge: edge, variables: results }
}
