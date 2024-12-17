package kflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Kidsunbo/kie_toolbox_go/kflow"
)

type Node1 struct {
	Field1 string `k:"name:field_2; type:auto"`
	Field2 string `k:"name:field_2; type:input"`
	Field3 string `k:"name:field_3; type:output"`
	Field4 *string `k:"name:Node2; type:delay"`
	Field5 string `k:"type:auto"`
}
func (n *Node1) Run() error {
	return nil
}

type Node2 struct {
	Field1 string `k:"name:TheNode.Field3; type:auto"`
	Field2 string `k:"name:field_2; type:input"`
	Field3 string `k:"type:output"`
	Field4 string `k:"name:field_4; type:delay"`
	Field5 string `k:"type:auto"`
	Field6 string `k:"type:output; name: fault"`
}
func (n *Node2) Run() error {
	return nil
}


func New[T any](ctx context.Context) *T {
	return new(T)
}



func ToJSON(obj any) string {
	bs, err := json.Marshal(obj)
	if err != nil {
		return err.Error()
	}
	return string(bs)
}

func TestFlowBuilder(t *testing.T) {
	f := kflow.NewFlowBuilder("test")
	kflow.Add(f, New[Node1])
	kflow.Add(f, New[Node2])
	f.Prepare()
	fmt.Println(ToJSON(f))
}
