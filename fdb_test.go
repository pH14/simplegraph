package simplegraph

import (
	"encoding/binary"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestALotOfKeys(t *testing.T) {
	_ = fdb.APIVersion(600)
	database := fdb.MustOpenDefault()

	graph := FdbGraph{&database}

	simpleGraph := NewSimpleGraph(&graph)

	start := time.Now()

	const numEntries = 100000
	const batch = 500

	var waitGroup sync.WaitGroup

	for i := 0; i < numEntries; i += batch {
		waitGroup.Add(1)
		go (func(currentIndex int) {
			defer waitGroup.Done()

			edges := make([]Edge, batch)

			for j := currentIndex; j < currentIndex + batch; j++  {
				b := make([]byte, 8)
				binary.LittleEndian.PutUint64(b, uint64(j))

				edges = append(edges, Edge{
					subject: []byte("subject3"),
					predicate: []byte("predicate"),
					object: b,
				})
			}

			e := simpleGraph.AddEdges(edges)

			if e != nil {
				panic(e)
			}
		})(i)
	}

	waitGroup.Wait()

	fmt.Printf("Inserting 100k keys took: %v seconds \n", time.Since(start).Seconds())

	start = time.Now()
	//getRange, e := simpleGraph.GetEdges([]byte("subject2"), []byte("predicate"))
	//
	//if e != nil {
	//	t.Fatalf("ded %v", e)
	//	t.Fail()
	//}
	//
	//numberOfRows := 0
	//for range getRange {
	//	numberOfRows++
	//	//fmt.Println(string(key))
	//}
	//fmt.Printf("Reading %v keys took: %v seconds \n", numberOfRows, time.Since(start).Seconds())
}

func TestStreamingAnd(t *testing.T) {
	_ = fdb.APIVersion(600)
	database := fdb.MustOpenDefault()
	graph := FdbGraph{&database}
	simpleGraph := NewSimpleGraph(&graph)

	_, _ = database.Transact(func(tx fdb.Transaction) (i interface{}, e error) {
		tx.ClearRange(subspace.AllKeys())
		return nil, nil
	})

	start := time.Now()

	_ = simpleGraph.AddEdges([]Edge{
		{subject: []byte("Pierce"), predicate: []byte("played for"), object: []byte("Celtics"),},
		{subject: []byte("Al Jefferson"), predicate: []byte("played for"), object: []byte("Celtics"),},
		{subject: []byte("Al Jefferson"), predicate: []byte("played for"), object: []byte("Timberwolves"),},
		{subject: []byte("Garnett"), predicate: []byte("played for"), object: []byte("Celtics"),},
		{subject: []byte("Garnett"), predicate: []byte("played for"), object: []byte("Timberwolves"),},
		{subject: []byte("Kyrie "), predicate: []byte("played for"), object: []byte("Cleveland"),},
		{subject: []byte("Kyrie"), predicate: []byte("plays for"), object: []byte("Celtics"),},
	})

	/*
	sort order is different at the row level...
	pos : p o s
	p : p o s | p s o

	s p : s p o | p s o
	o : o p s | o s p

	p : p o s | p s o           played for | Timberwolves | Garnett or played for | Garnett | Timberwolves
	o : o p s | o s p           Timberwolves | played for | Garnett or Timberwolves | Garnett | played for
	*/
	edges, _ := simpleGraph.GetRangeStreamingAnd(
		Query{
			predicate: []byte("played for"),
			object: []byte("Timberwolves"),
		},
		Query{
			//subject: []byte("Kyrie"),
			predicate: []byte("played for"),
			//object: []byte("Timberwolves"),
		})

	var edgeResults []*Edge
	k := 0
	for edge := range edges {
		edgeResults = append(edgeResults, edge)
		k++
	}

	expected := []*Edge{
		{subject: []byte("Al Jefferson"), predicate: []byte("played for"), object: []byte("Timberwolves"),},
		{subject: []byte("Garnett"), predicate: []byte("played for"), object: []byte("Timberwolves"),},
	}

	if !reflect.DeepEqual(edgeResults, expected) {
		t.Fatalf("expected %v to equal %v", edgeResults, expected)
		t.Fail()
	}

	fmt.Printf("Reading %v keys took: %v seconds \n", k, time.Since(start).Seconds())
}

func TestSimpleGraph_GetRangeStreamingAnd(t *testing.T) {
	_ = fdb.APIVersion(600)
	database := fdb.MustOpenDefault()
	graph := FdbGraph{&database}
	simpleGraph := NewSimpleGraph(&graph)

	_ = simpleGraph.AddEdges([]Edge{
		{subject: []byte("Paul Pierce"), predicate: []byte("played for"), object: []byte("Celtics"),},
		{subject: []byte("Al Jefferson"), predicate: []byte("played for"), object: []byte("Celtics"),},
		{subject: []byte("Al Jefferson"), predicate: []byte("played for"), object: []byte("Timberwolves"),},
		{subject: []byte("Kevin Garnett"), predicate: []byte("played for"), object: []byte("Celtics"),},
		{subject: []byte("Kevin Garnett"), predicate: []byte("played for"), object: []byte("Timberwolves"),},
		{subject: []byte("Kyrie Irving"), predicate: []byte("played for"), object: []byte("Cleveland"),},
		{subject: []byte("Kyrie Irving"), predicate: []byte("plays for"), object: []byte("Celtics"),},
		{subject: []byte("Smush Parker"), predicate: []byte("played for"), object: []byte("Lakers"),},
		{subject: []byte("Sasha Vujačić"), predicate: []byte("played for"), object: []byte("Lakers"),},
	})

	type args struct {
		queries []Query
	}

	tests := []struct {
		name string
		args args
		want []*Edge
	}{
		{ "filters a single subject",
			args{ []Query { { subject: []byte("Kyrie Irving") }, { subject: []byte("Kyrie Irving") } } },
			[]*Edge {
				{subject: []byte("Kyrie Irving"), predicate: []byte("played for"), object: []byte("Cleveland"),},
				{subject: []byte("Kyrie Irving"), predicate: []byte("plays for"), object: []byte("Celtics"),},
			},
		},
		{ "filters two apiece",
			args{ []Query {
				{ subject: []byte("Kyrie Irving"), predicate: []byte("plays for")},
				{ subject: []byte("Kyrie Irving"), object: []byte("Celtics") } } },
			[]*Edge {
				{subject: []byte("Kyrie Irving"), predicate: []byte("plays for"), object: []byte("Celtics"),},
			},
		},
		{ "filters player and team in separate queries",
			args{ []Query { { subject: []byte("Kyrie Irving") }, { object: []byte("Celtics") } } },
			[]*Edge {
				{subject: []byte("Kyrie Irving"), predicate: []byte("plays for"), object: []byte("Celtics"),},
			},
		},
		//{ "filters",
		//	// TODO: Mark this as an invalid query
		//	args{ []Query { { object: []byte("Lakers") }, { object: []byte("Celtics") } } },
		//	[]*Edge {},
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := simpleGraph.GetRangeStreamingAnd(tt.args.queries[0], tt.args.queries[1])

			var edgeResults []*Edge
			for edge := range got {
				edgeResults = append(edgeResults, edge)
			}

			if !reflect.DeepEqual(edgeResults, tt.want) {
				t.Errorf("simpleGraph.GetRangeStreamingAnd() = %v, want %v", edgeResults, tt.want)
			}
		})
	}
}

