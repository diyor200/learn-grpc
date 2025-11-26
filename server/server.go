package server

import (
	"context"
	hellov1 "github.com/diyor200/learn-grpc/proto"
)

type Server struct {
	conn hellov1.UnimplementedHelloServer
}

func (s *Server) SayHello(ctx context.Context, in *hellov1.HelloRequest) (*hellov1.HelloResponse, error) {
	return &hellov1.HelloResponse{HelloText: "Hello " + in.Name}, nil
}
