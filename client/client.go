package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/diyor200/learn-grpc/proto"
	"google.golang.org/grpc"
)

const (
	chunkSize   = 1024 * 1024 * 3 // 3 MB
	concurrency = 4
	maxRetries  = 3
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("dial err: %v", err)
	}
	defer conn.Close()
	client := pb.NewFileServiceClient(conn)

	filePath := "swift.gz"
	finfo, err := os.Stat(filePath)
	if err != nil {
		log.Fatalf("stat err: %v", err)
	}
	totalSize := finfo.Size()
	totalChunks := int((totalSize + chunkSize - 1) / chunkSize)

	type job struct {
		id     int
		offset int64
		size   int
		data   []byte
	}

	// prepare jobs (read file parts into memory one by one to avoid reading concurrently from disk)
	jobs := make([]job, 0, totalChunks)
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("open err: %v", err)
	}
	defer f.Close()

	for i := 0; i < totalChunks; i++ {
		offset := int64(i) * chunkSize
		remain := totalSize - offset
		sz := int(chunkSize)
		if int64(sz) > remain {
			sz = int(remain)
		}
		buf := make([]byte, sz)
		_, err := f.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			log.Fatalf("readAt err: %v", err)
		}
		jobs = append(jobs, job{id: i, offset: offset, size: sz, data: buf})
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	var uploaded int64 // atomic

	for _, jb := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j job) {
			defer wg.Done()
			defer func() { <-sem }()

			var attempt int
			for {
				attempt++
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				_, err := client.UploadChunk(ctx, &pb.UploadChunkRequest{
					Filename: filePath,
					ChunkId:  int64(j.id),
					Offset:   j.offset,
					Data:     j.data,
				})
				cancel()
				if err == nil {
					atomic.AddInt64(&uploaded, int64(j.size))
					percent := float64(atomic.LoadInt64(&uploaded)) / float64(totalSize) * 100
					log.Printf("Chunk %d uploaded (attempt %d). Progress: %.2f%%", j.id, attempt, percent)
					break
				}
				log.Printf("Chunk %d upload failed (attempt %d): %v", j.id, attempt, err)
				if attempt >= maxRetries {
					log.Fatalf("Chunk %d: exceeded retries", j.id)
				}
				time.Sleep(time.Second * time.Duration(attempt)) // backoff
			}
		}(jb)
	}
	wg.Wait()

	// call CompleteFile
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	res, err := client.CompleteFile(ctx, &pb.CompleteFileRequest{
		Filename:    filePath,
		TotalSize:   totalSize,
		TotalChunks: int32(totalChunks),
	})
	if err != nil {
		log.Fatalf("CompleteFile err: %v", err)
	}
	log.Printf("Complete: message=%s size=%d sha256=%s", res.Message, res.Size, res.Sha256)

	// local optional verify
	verifyLocalSHA(filePath, res.Sha256)
}

func verifyLocalSHA(path, want string) {
	f, err := os.Open(path)
	if err != nil {
		log.Printf("verify open err: %v", err)
		return
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Printf("verify copy err: %v", err)
		return
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if sum != want {
		log.Printf("SHA mismatch: local=%s server=%s", sum, want)
	} else {
		log.Printf("SHA verified")
	}
}
