package simplegraph

import (
	"reflect"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func TestNewSimpleGraph(t *testing.T) {
	type args struct {
		kvstore KVStore
	}
	tests := []struct {
		name string
		args args
		want *SimpleGraph
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSimpleGraph(tt.args.kvstore); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSimpleGraph() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSimpleGraph_AddEdges(t *testing.T) {
	type fields struct {
		kvstore KVStore
	}
	type args struct {
		edges []Edge
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph := &SimpleGraph{
				kvstore: tt.fields.kvstore,
			}
			if err := graph.AddEdges(tt.args.edges); (err != nil) != tt.wantErr {
				t.Errorf("SimpleGraph.AddEdges() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
//
//func TestSimpleGraph_GetRange(t *testing.T) {
//	type args struct {
//		query Query
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    [][]byte
//		wantErr bool
//	}{
//		{
//			name: "it gets a single predicate range",
//			args: args{ query: Query{ subject: []byte("S") } },
//			want: [][]byte { []byte("S") },
//			wantErr: false,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			graph := &SimpleGraph{
//				kvstore: kvstore,
//			}
//			got, err := graph.GetEdges(Query{ subject: tt.args.subject, predicate: tt.args.predicate})
//			if (err != nil) != tt.wantErr {
//				t.Errorf("SimpleGraph.GetEdges() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("SimpleGraph.GetEdges() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func TestIndex_ToBytes(t *testing.T) {
	type fields struct {
		ss       subspace.Subspace
		ordering []DataField
	}
	type args struct {
		edge *Edge
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		{
			name:   "it orders hexastoreIndex correctly",
			fields: fields{ss: subspace.Sub("test-ss"), ordering: []DataField{OBJECT, SUBJECT, PREDICATE}},
			args:   args{&Edge{subject: []byte("S"), predicate: []byte("P"), object: []byte("O")}},
			want:   tuple.Tuple{"test-ss", []byte("O"), []byte("S"), []byte("P")}.Pack(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := hexastoreIndex{
				ss:       tt.fields.ss,
				ordering: tt.fields.ordering,
			}
			if got := index.toBytes(tt.args.edge); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Index.toBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_findIndex(t *testing.T) {
	type args struct {
		query Query
	}
	tests := []struct {
		name string
		args args
		want []*hexastoreIndex
	}{
		{
			name: "it selects the deepest index with single entry: spo | sop",
			args: args{Query{subject: []byte("S")}},
			want: []*hexastoreIndex{Indices["spo"], Indices["sop"]},
		},
		{
			name: "it selects the deepest index with a different single entry: ops | osp",
			args: args{Query{object: []byte("O")}},
			want: []*hexastoreIndex{Indices["ops"], Indices["osp"]},
		},
		{
			name: "it selects the deepest index with two entries: spo | pso",
			args: args{Query{subject: []byte("S"), predicate: []byte("P")}},
			want: []*hexastoreIndex{Indices["spo"], Indices["pso"]},
		},
		{
			name: "it selects the deepest index with two other entries: pos | ops",
			args: args{Query{predicate: []byte("P"), object: []byte("O")}},
			want: []*hexastoreIndex{Indices["pos"], Indices["ops"]},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findIndices(transformQuery(tt.args.query))

			anySuccess := false

			for _, validIndex := range tt.want {
				if reflect.DeepEqual(got, validIndex) {
					anySuccess = true
					break
				}
			}

			if !anySuccess {
				t.Errorf("findIndices() = %v, want %v", got, tt.want)
			}
		})
	}
}

//func Test_conjunctiveStreamJoin(t *testing.T) {
//	mustMatch := make(chan *Edge)
//
//	go func() {
//		defer close(mustMatch)
//		for i := 0; i <= 12; i += 1 {
//			b := make([]byte, 8)
//			binary.LittleEndian.PutUint64(b, uint64(i))
//			mustMatch <- b
//		}
//	}()
//
//	candidates := make(chan []byte)
//
//	go func() {
//		defer close(candidates)
//		for i := 0; i <= 12; i += 3 {
//			b := make([]byte, 8)
//			binary.LittleEndian.PutUint64(b, uint64(i))
//			candidates <- b
//		}
//	}()
//
//	out := make(chan []byte)
//
//	streamJoin := OrderedStreamJoin{
//		inputStreamOne: mustMatch,
//		inputStreamTwo: candidates,
//		outStream:       out,
//	}
//
//	go streamJoin.join()
//
//	for x := range out {
//		fmt.Println(x)
//	}
//}

func Test_hexastoreIndex_fromBytes(t *testing.T) {
	for _, idx := range Indices {
		edge := &Edge{
			subject:   []byte("Subject"),
			predicate: []byte("Predicate"),
			object:    []byte("Object"),
		}

		edgeV2, e := idx.fromBytes(idx.toBytes(edge))

		if e != nil {
			t.Errorf("failed to deserialize. %v", e)
		}

		if !reflect.DeepEqual(edge, edgeV2) {
			t.Errorf("edge did not survive serialization. was: %v, but got %v", edge, *edgeV2)
		}
	}
}
