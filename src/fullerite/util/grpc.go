package util

import (
	"context"
	grpcMetrics "fullerite/collector/metrics"
	"time"

	"google.golang.org/grpc"
)

// GRPCGetter provides the interface for gRPC clients.
type GRPCGetter interface {
	Get() ([]byte, string, error)
}

type grpcGetterImpl struct {
	contentType string
	client      grpcMetrics.MetricsClient
	conn        *grpc.ClientConn
}

// NewGRPCGetter constructs a new GRPCGetter instance.
func NewGRPCGetter(url string, timeout int) (GRPCGetter, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, url, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := grpcMetrics.NewMetricsClient(conn)
	return &grpcGetterImpl{
		client:      client,
		contentType: "text/plain; version=0.0.4",
		conn:        conn,
	}, nil
}

// Get retrieves content from the metrics gRPC endpoint.
func (g *grpcGetterImpl) Get() ([]byte, string, error) {
	defer g.conn.Close()
	res, err := g.client.Metrics(context.Background(), &grpcMetrics.MetricsRequest{})
	if err != nil {
		return nil, "", err
	}
	return []byte(res.Data), g.contentType, nil
}
