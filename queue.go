package queue

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/scottjbarr/redis"
)

// Queue is the complete interface for a read/write, relaible QuUeue.
type Queue interface {
	Receive(chan<- Message) error
	Enqueuer
	BatchEnqueuer
	Acker
}

type Writer interface {
	Enqueuer
	BatchEnqueuer
}

// Enqueuer is an interface for queueing messages.
type Enqueuer interface {
	Enqueue(context.Context, *Message) error
}

// BatchEnqueuer is an interface for queueing batches of messages.
type BatchEnqueuer interface {
	BatchEnqueue(context.Context, []Message) error
}

type Acker interface {
	Ack(Message) error
}

// Message is carries message data to and from queue implementations.
type Message struct {
	ID      string
	Handle  string
	Payload string
	Trace   string

	// GroupID is used by FIFO queues to ensure FIFO delivery for groups of messages.
	GroupID string `json:"group_id,omitempty"`
}

func New(uri string) (Queue, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	if strings.HasPrefix(uri, "https://sqs.") {
		if strings.HasSuffix(uri, ".fifo") {
			// FIFO queue always ends with ".fifo"
			q := NewSQSFIFOQueue(uri)
			return &q, nil

		} else {
			q := NewSQSQueue(uri)
			return &q, nil
		}
	} else if u.Scheme == "redis" {
		pool, err := redis.NewPool(uri)
		if err != nil {
			return nil, err
		}

		conn := pool.Get()
		defer conn.Close()

		if _, err := conn.Do("PING"); err != nil {
			return nil, err
		}

		// strip the leading / from the path
		key := u.Path[1:]

		q := NewRedisPoolQueue(pool, key)

		return q, nil
	}

	return nil, errors.New("Unsupported scheme")
}
