package main

import (
	"context"
	progressv1 "github.com/diyor200/learn-grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"time"
)

func main() {
	conn, err := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := progressv1.NewProgressServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	stream, err := client.GetProgress(ctx, &progressv1.ProgressRequest{TaskId: "123"})
	if err != nil {
		log.Fatalf("failed to get progress: %v", err)
	}

	for {
		update, err := stream.Recv()
		if err == io.EOF {
			log.Println("end of stream")
			break
		}

		if err != nil {
			log.Fatalf("failed to recv: %v", err)
		}

		log.Printf("Progress %d%% - %s", update.Percent, update.Message)
	}
}
