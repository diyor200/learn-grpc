package main

import (
	"context"
	"io"
	"log"
	"math"
	"os"
	"path"

	fileuploadv1 "github.com/diyor200/learn-grpc/proto"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := fileuploadv1.NewFileServiceClient(conn)

	filePath := "swift.tar.gz"
	stream, err := client.FileUpload(context.Background())
	if err != nil {
		log.Fatalf("FileUpload err: %v", err)
	}

	// check file
	check, err := client.CheckFile(context.Background(), &fileuploadv1.CheckRequest{Filename: filePath})
	if err != nil {
		log.Fatalf("could not check file: %v", err)
	}

	if check.UploadedSize == 0 {
		err = stream.Send(&fileuploadv1.FileChunk{Name: filePath})
		if err != nil {
			log.Fatalf("FileUpload err: %v", err)
		}
	}

	filePath = path.Base(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("os.Open err: %v", err)
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	if check.UploadedSize == fileInfo.Size() {
		_, err = stream.CloseAndRecv()
		if err != nil {
			log.Fatalf("FileUpload err: %v", err)
		}

		log.Println("file already uploaded")
		return
	}

	buf := make([]byte, 1024*64)
	var sentBytes int64

	_, err = file.Seek(check.UploadedSize, 0)
	if err != nil {
		log.Fatalf("FileUpload err: %v", err)
	}
	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("file.Read err: %v", err)
		}

		err = stream.Send(&fileuploadv1.FileChunk{Name: filePath, Data: buf[:n], Offset: check.UploadedSize + sentBytes})
		if err != nil {
			log.Fatalf("stream.Send err: %v", err)
		}

		sentBytes += int64(n)
		percent := float64(sentBytes+check.UploadedSize) / float64(fileInfo.Size()) * 100
		log.Printf("Uploaded: %d%%", int(math.Round(percent)))
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("FileUpload err: %v", err)
	}

	log.Printf("Upload status: %s\n", res.Message)
	log.Printf("Upload size: %d\n", res.Size)
	log.Println("File SHA256:", res.Sha256)

}
