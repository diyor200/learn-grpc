package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"net"

	pb "github.com/diyor200/learn-grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const uploadDir = "uploads"

type server struct {
	pb.UnimplementedFileServiceServer
}

func (s *server) UploadChunk(ctx context.Context, req *pb.UploadChunkRequest) (*pb.UploadChunkResponse, error) {
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return nil, err
	}

	// Temp chunk filename
	partName := filepath.Join(uploadDir, req.Filename+".part."+strconv.FormatInt(req.ChunkId, 10))
	// Write data (overwrite if exists — idempotent behavior for same chunk)
	if err := os.WriteFile(partName, req.Data, 0644); err != nil {
		return nil, err
	}
	return &pb.UploadChunkResponse{Received: int64(len(req.Data))}, nil
}

func (s *server) CompleteFile(ctx context.Context, req *pb.CompleteFileRequest) (*pb.CompleteFileResponse, error) {
	// find parts
	pattern := filepath.Join(uploadDir, req.Filename+".part.*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, os.ErrNotExist
	}
	// sort by chunk id (extract id from suffix)
	sort.Slice(matches, func(i, j int) bool {
		// parse suffix
		pi := matches[i][len(matches[i])-1]
		pj := matches[j][len(matches[j])-1]
		// naive fallback: parse by splitting
		return pi < pj
	})

	outPath := filepath.Join(uploadDir, req.Filename)
	outFile, err := os.Create(outPath + ".tmp")
	if err != nil {
		return nil, err
	}
	defer outFile.Close()

	// concatenate parts in numeric order of chunk id
	// safer to parse chunk id by splitting suffix
	type part struct {
		path string
		id   int
	}
	parts := make([]part, 0, len(matches))
	for _, p := range matches {
		// filename.part.<id>
		base := filepath.Base(p)
		// find last dot
		// base format: "<filename>.part.<id>"
		// get id after last '.'
		segments := filepath.Ext(p) // gives like ".<id>" not reliable for multi dots -> fallback parsing:
		_ = segments
		// simpler parse: find ".part."
		idx := -1
		if i := len(base); i > 0 {
			if pos := findPartIndex(base); pos >= 0 {
				idx = pos + len(".part.")
				if idx < len(base) {
					idStr := base[idx:]
					id, err := strconv.Atoi(idStr)
					if err != nil {
						return nil, err
					}
					parts = append(parts, part{path: p, id: id})
				}
			}
		}
	}
	// sort parts by id
	sort.Slice(parts, func(i, j int) bool { return parts[i].id < parts[j].id })

	h := sha256.New()
	var total int64
	for _, p := range parts {
		f, err := os.Open(p.path)
		if err != nil {
			return nil, err
		}
		n, err := io.Copy(outFile, io.TeeReader(f, h))
		f.Close()
		if err != nil {
			return nil, err
		}
		total += n
	}
	// finalize sha256
	sum := hex.EncodeToString(h.Sum(nil))

	// rename temp to final atomically
	if err := os.Rename(outPath+".tmp", outPath); err != nil {
		return nil, err
	}
	// optionally cleanup parts
	for _, p := range parts {
		os.Remove(p.path)
	}

	return &pb.CompleteFileResponse{
		Message: "ok",
		Size:    total,
		Sha256:  sum,
	}, nil
}

// helper: find ".part." index in base filename
func findPartIndex(name string) int {
	return indexOf(name, ".part.")
}
func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func (s *server) CheckFile(ctx context.Context, req *pb.CheckRequest) (*pb.CheckResponse, error) {
	outPath := filepath.Join(uploadDir, req.Filename)
	info, err := os.Stat(outPath)
	if err == nil {
		return &pb.CheckResponse{UploadedSize: info.Size()}, nil
	}
	if os.IsNotExist(err) {
		return &pb.CheckResponse{UploadedSize: 0}, nil
	}
	return nil, err
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen err: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterFileServiceServer(grpcServer, &server{})
	reflection.Register(grpcServer)
	log.Println("server listening :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve err: %v", err)
	}
}
