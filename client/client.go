package main

import (
	"context"
	fileuploadv1 "github.com/diyor200/learn-grpc/proto"
	"google.golang.org/grpc"
	"io"
	"log"
	"os"
)

func main() {
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := fileuploadv1.NewFileServiceClient(conn)

	filePath := "img.HEIC"
	stream, err := client.FileUpload(context.Background())
	if err != nil {
		log.Fatalf("FileUpload err: %v", err)
	}

	err = stream.Send(&fileuploadv1.FileChunk{Name: filePath})
	if err != nil {
		log.Fatalf("FileUpload err: %v", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("os.Open err: %v", err)
	}
	defer file.Close()

	buf := make([]byte, 1024*64)

	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			err = stream.CloseSend()
			if err != nil {
				log.Fatalf("stream.CloseSend err: %v", err)
			}
			break
		}

		if err != nil {
			log.Fatalf("file.Read err: %v", err)
		}

		err = stream.Send(&fileuploadv1.FileChunk{Data: buf[:n]})
		if err != nil {
			log.Fatalf("stream.Send err: %v", err)
		}
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("FileUpload err: %v", err)
	}

	log.Printf("Upload status: %s\n", res.Message)
	log.Printf("Upload size: %d\n", res.Size)
	log.Println("File SHA256:", res.Sha256)

}
