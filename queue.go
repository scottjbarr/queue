package queue

import (
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
	Enqueue(*Message) error
}

// BatchEnqueuer is an interface for queueing batches of messages.
type BatchEnqueuer interface {
	BatchEnqueue([]Message) error
}

type Acker interface {
	Ack(*Message) error
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

	if strings.Contains(uri, ".amazonaws.com/") {
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

// BatchMessages splits a []Message slice into batches of a given size.
//
// Some queue providers such as SQS have limits on batch enqueueing.
func BatchMessages(messages []Message, chunkSize int) [][]Message {
	var batches [][]Message

	for i := 0; i < len(messages); i += chunkSize {
		end := i + chunkSize

		// necessary check to avoid slicing beyond slice capacity
		if end > len(messages) {
			end = len(messages)
		}

		batches = append(batches, messages[i:end])
	}

	return batches
}
