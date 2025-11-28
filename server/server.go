package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"net"
	"os"

	fileuploadv1 "github.com/diyor200/learn-grpc/proto"
	"google.golang.org/grpc"
)

type server struct {
	fileuploadv1.UnimplementedFileServiceServer
}

func (s *server) CheckFile(ctx context.Context, in *fileuploadv1.CheckRequest) (*fileuploadv1.CheckResponse, error) {
	log.Println("received check file request")

	info, err := os.Stat(in.Filename)
	if err != nil {
		log.Println(err)
		if os.IsNotExist(err) {
			return &fileuploadv1.CheckResponse{UploadedSize: 0}, nil
		}
		return nil, err
	}

	return &fileuploadv1.CheckResponse{UploadedSize: info.Size()}, nil
}

func (s *server) FileUpload(stream fileuploadv1.FileService_FileUploadServer) error {
	log.Println("received file upload request")

	var file *os.File
	var totalSize int64 = 0
	var fileName string

	hasher := sha256.New()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			hashSum := hasher.Sum(nil)
			hashHex := hex.EncodeToString(hashSum)
			err = stream.SendAndClose(&fileuploadv1.UploadStatus{
				Message: "file uploaded",
				Size:    totalSize,
				Sha256:  hashHex,
			})
			if err != nil {
				return err
			}
			break
		}
		if err != nil {
			log.Println(err)
			return err
		}

		if file == nil {
			fileName = chunk.Name
			file, err = os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Println(err)
				return err
			}

			_, err = file.Seek(chunk.Offset, 0)
			if err != nil {
				log.Println(err)
				return err
			}
		}

		n, err := file.Write(chunk.Data)
		if err != nil {
			log.Println(err)
			return err
		}

		totalSize += int64(n)
		hasher.Write(chunk.Data)
	}

	return nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	fileuploadv1.RegisterFileServiceServer(s, &server{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
