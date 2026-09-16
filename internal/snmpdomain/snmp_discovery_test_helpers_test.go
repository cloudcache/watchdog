package snmpdomain

import "context"

type fakeQueryEngine struct {
	walks map[string]QueryResponse
	gets  map[string]QueryResponse
}

func (f fakeQueryEngine) Get(_ context.Context, request GetRequest) (QueryResponse, error) {
	return f.gets[request.Context], nil
}

func (f fakeQueryEngine) Walk(_ context.Context, request WalkRequest) (QueryResponse, error) {
	return f.walks[request.BaseOID], nil
}
