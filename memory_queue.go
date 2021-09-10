package queue

import "context"

type MemoryQueue struct {
	Items []Message
}

func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		Items: []Message{},
	}
}

func (q *MemoryQueue) Enqueue(ctx context.Context, m *Message) error {
	q.Items = append(q.Items, *m)

	return nil
}

func (q *MemoryQueue) BatchEnqueue(ctx context.Context, msg []Message) error {
	for _, m := range msg {
		// ignoring errors from mock
		_ = q.Enqueue(ctx, &m)
	}

	return nil
}
