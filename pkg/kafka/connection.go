package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// pingTimeout bounds the connectivity check performed when a Connection is
// constructed.
const pingTimeout = 10 * time.Second

// Connection connects to Kafka for working with topics. Topic admin methods are
// added by the admin change; this change provides connect and close.
type Connection struct {
	client *kgo.Client
}

// openConnection builds a franz-go client from the config and verifies
// connectivity (eager connect). It fails if the cluster is unreachable.
func openConnection(cfg ConnectionConfig) (*Connection, error) {
	opts, err := clientOptions([]string{cfg.Address}, cfg.SASL, cfg.TLS)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Kafka client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("connecting to %s: %w", cfg.Address, err)
	}

	return &Connection{client: client}, nil
}

// Close closes the underlying client and releases its connections.
func (c *Connection) Close() {
	if c.client != nil {
		c.client.Close()
		c.client = nil
	}
}
