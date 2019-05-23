package simplegraph

import (
	"bytes"
	"fmt"
)

type OrderedStreamJoin struct {
	tripleOrder TripleOrder
}

func (sj *OrderedStreamJoin) join(inputStreamOne, inputStreamTwo <-chan *Edge) <-chan *Edge {
	orderedOutputStream := make(chan *Edge)

	go func(inputStreamOne, inputStreamTwo <-chan *Edge, outputStream chan<- *Edge) {
		defer close(outputStream)

		var currentStreamOneEdge *Edge = nil
		var currentStreamTwoEdge *Edge = nil
		var streamOneComparisonBytes []byte = nil
		var streamTwoComparisonBytes []byte = nil

		for {
			if currentStreamOneEdge == nil {
				edge, more := <-inputStreamOne
				fmt.Printf("Match: %v\n", edge)

				if !more {
					return
				}

				currentStreamOneEdge = edge
				streamOneComparisonBytes = sj.tripleOrder.fromEdge(edge)
			}

			if currentStreamTwoEdge == nil {
				edge, more := <-inputStreamTwo
				fmt.Printf("Candidate: %v\n", edge)

				if !more {
					return
				}

				currentStreamTwoEdge = edge
				streamTwoComparisonBytes = sj.tripleOrder.fromEdge(edge)
			}

			comparison := bytes.Compare(streamTwoComparisonBytes, streamOneComparisonBytes)

			if comparison == 0 {
				// key is in both streams, this counts as a match. advance both streams 1
				outputStream <- currentStreamOneEdge
				currentStreamTwoEdge = nil
				currentStreamOneEdge = nil
			} else if comparison == -1 {
				// candidate is less than key. we need to seek candidate forward until it matches or is greater
				//fmt.Printf("Candidate seek: %v\n", edge)
				currentStreamTwoEdge = nil
			} else if comparison == 1 {
				// key is less than candidate. we need to seek key forward until it matches or is greater
				//fmt.Printf("Match seek: %v\n", currentStreamOneEdge)
				currentStreamOneEdge = nil
			}
		}
	}(inputStreamOne, inputStreamTwo, orderedOutputStream)

	return orderedOutputStream
}

