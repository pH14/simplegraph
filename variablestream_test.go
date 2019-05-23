package simplegraph

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSearchStream_join(t *testing.T) {
	edge := Edge{
		subject:   []byte("S"),
		predicate: []byte("P"),
		object:    []byte("O"),
	}

	edgeChannel := make(chan *Edge)

	searchStream := VariableStream{
		variables: map[DataField]string{
			SUBJECT: "x",
		},
	}

	go func() {
		edgeChannel <- &edge
		close(edgeChannel)
	}()

	output := searchStream.join(edgeChannel)

	count := 0
	for variables := range output {
		count++
		fmt.Printf("got %v\n", *variables.variables[SUBJECT])
		if !reflect.DeepEqual(*variables, SearchResults{
			edge: &edge,
			variables: map[DataField]*VariableResult{
				SUBJECT: {
					name:  "x",
					value: []byte("S"),
				},
			},
		}) {
			t.Fatalf("got %v", *variables)
		}
	}

	if count != 1 {
		t.Fatalf("got %v rows", count)
	}
}
