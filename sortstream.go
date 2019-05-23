package simplegraph

import (
	"bytes"
	"log"
	"sort"
)

type SortStream struct {
	variables map[DataField]string
	tripleOrder TripleOrder
}

func (ss *SortStream) join(input <-chan *SearchResults) <-chan *SearchResults {
	output := make(chan *SearchResults)

	go func(output chan<- *SearchResults) {
		defer close(output)
		buf := make([]*SearchResults, 0)

		for edge := range input {
			buf = append(buf, edge)
		}

		sort.Sort(sortResults(buf))

		for _, result := range buf {
			output <- result
		}
	}(output)

	return output
}

type sortResults []*SearchResults

func (b sortResults) Len() int {
	return len(b)
}

func (b sortResults) Less(i, j int) bool {
	// todo: this is inefficient to re-pack the byte arrays each time
	switch bytes.Compare(b[i].edge.toBytes(), b[j].edge.toBytes()) {
	case -1:
		return true
	case 0, 1:
		return false
	default:
		log.Panic("not fail-able with `bytes.Comparable` bounded [-1, 1].")
		return false
	}
}

func (b sortResults) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}
