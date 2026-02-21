package chunktransfer

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pb.UnimplementedChunkTransferServiceServer
	storageDir string
}

func NewServer(storageDir string) *Server {
	return &Server{storageDir: storageDir}
}

func (s *Server) CreateChunk(ctx context.Context, req *pb.CreateChunkRequest) (*emptypb.Empty, error) {
	chunkDir := filepath.Join(s.storageDir, fmt.Sprintf("%d", req.ChunkId))
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create chunk directory: %v", err)
	}
	log.Printf("Created chunk %d at %s", req.ChunkId, chunkDir)
	return &emptypb.Empty{}, nil
}

func (s *Server) WriteBlock(stream pb.ChunkTransferService_WriteBlockServer) error {
	blocksReceived := 0
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.Printf("WriteBlock stream closed, received %d blocks", blocksReceived)
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive block: %v", err)
		}

		// Verify checksum
		checksum := crc32.ChecksumIEEE(req.Data)
		if checksum != req.Checksum {
			return status.Errorf(codes.DataLoss, "checksum mismatch for block %d: got %d, expected %d",
				req.BlockIndex, checksum, req.Checksum)
		}

		// Write block to disk
		blockPath := filepath.Join(s.storageDir, fmt.Sprintf("%d", req.ChunkId),
			fmt.Sprintf("%d.block", req.BlockIndex))
		if err := os.WriteFile(blockPath, req.Data, 0644); err != nil {
			return status.Errorf(codes.Internal, "failed to write block %d: %v", req.BlockIndex, err)
		}

		log.Printf("Stored block %d of chunk %d (%d bytes)", req.BlockIndex, req.ChunkId, len(req.Data))
		blocksReceived++
	}
}

func (s *Server) ReadFromChunk(req *pb.ReadFromChunkRequest, stream pb.ChunkTransferService_ReadFromChunkServer) error {
	// TODO: implement
	return nil
}
