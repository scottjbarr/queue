package queue

import "context"

// Queue is the complete interface for a read/write, relaible QuUeue.
type Queue interface {
	Receive(chan<- Message) error
	Send(Message) error
	SendBatch([]Message) error
	Acker
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
