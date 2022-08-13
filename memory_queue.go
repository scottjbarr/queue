package queue

import "errors"

type MemoryQueue struct {
	Items []Message
	// Input chan Message
}

func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		Items: []Message{},
		// Input: make(chan Message, 20),
	}
}

func (q *MemoryQueue) Enqueue(m *Message) error {
	q.Items = append(q.Items, *m)

	// q.Input <- *m

	return nil
}

func (q *MemoryQueue) BatchEnqueue(msg []Message) error {
	for _, m := range msg {
		// ignoring errors from mock
		_ = q.Enqueue(&m)
	}

	return nil
}

func (q *MemoryQueue) Ack(m *Message) error {
	return nil
}

func (q *MemoryQueue) Receive(ch chan Message) error {
	// for {
	// 	m := <-q.Input
	// 	ch <- m
	// }

	// return nil
	return errors.New("not implemented")
}
