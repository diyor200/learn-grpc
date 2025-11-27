package main

import (
	progressv1 "github.com/diyor200/learn-grpc/proto"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

type server struct {
	progressv1.UnimplementedProgressServiceServer
}

func (s *server) GetProgress(req *progressv1.ProgressRequest, stream progressv1.ProgressService_GetProgressServer) error {
	//progressList := []int32{0, 25, 50, 75, 100}

	for i := int32(1); i <= 100; i++ {
		err := stream.Send(&progressv1.ProgressResponse{
			Percent: i,
			Message: "Fetching progress ...",
		})
		if err != nil {
			log.Println(err)
			return err
		}

		time.Sleep(time.Millisecond * 50)
	}

	return nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	progressv1.RegisterProgressServiceServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
