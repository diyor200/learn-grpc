package main

import (
	"crypto/sha256"
	"encoding/hex"
	fileuploadv1 "github.com/diyor200/learn-grpc/proto"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
)

type server struct {
	fileuploadv1.UnimplementedFileServiceServer
}

func (s *server) FileUpload(stream fileuploadv1.FileService_FileUploadServer) error {
	var file *os.File
	var totalSize int64 = 0

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
			fileName := filepath.Base(chunk.Name)
			file, err = os.Create(fileName)
			if err != nil {
				return err
			}

			log.Println("Creating file:", fileName)
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
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
