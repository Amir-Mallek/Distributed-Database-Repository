package chunktransfer

import (
	"context"
	"fmt"
	"hash/crc32"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const blockSize = 1024 * 1024 // 1 MB per block

type Client struct {
	conn   *grpc.ClientConn
	client pb.ChunkTransferServiceClient
}

func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", serverAddr, err)
	}
	return &Client{
		conn:   conn,
		client: pb.NewChunkTransferServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// SendChunk sends one metadata message followed by block messages.
func (c *Client) SendChunk(ctx context.Context, chunkId uint32, clientId, datasetId string, data []byte) error {
	// Open a client-streaming WriteChunk call.
	stream, err := c.client.WriteChunk(ctx)
	if err != nil {
		return fmt.Errorf("WriteChunk stream open failed: %w", err)
	}

	if err := stream.Send(&pb.WriteChunkRequest{
		Msg: &pb.WriteChunkRequest_Meta{
			Meta: &pb.ChunkMetadata{
				ChunkId:   chunkId,
				ClientId:  clientId,
				DatasetId: datasetId,
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send chunk metadata: %w", err)
	}

	// Split data into fixed-size blocks and stream them.
	blockIndex := uint32(0)
	for offset := 0; offset < len(data); offset += blockSize {
		end := offset + blockSize
		if end > len(data) {
			end = len(data)
		}
		block := data[offset:end]

		err := stream.Send(&pb.WriteChunkRequest{
			Msg: &pb.WriteChunkRequest_Block{
				Block: &pb.ChunkBlock{
					Data:     block,
					Checksum: crc32.ChecksumIEEE(block),
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to send block %d: %w", blockIndex, err)
		}
		blockIndex++
	}

	// Close the stream and wait for the server acknowledgement.
	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("CloseAndRecv failed: %w", err)
	}
	return nil
}
