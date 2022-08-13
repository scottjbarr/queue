package queue

import (
	"log"
)

type ChannelQueue struct {
	Input chan Message
}

func NewChannelQueue(input chan Message) *ChannelQueue {
	return &ChannelQueue{
		Input: input,
	}
}

func (q *ChannelQueue) Enqueue(m *Message) error {
	q.Input <- *m

	return nil
}

func (q *ChannelQueue) BatchEnqueue(msg []Message) error {
	for _, m := range msg {
		// ignoring errors for this implementation
		_ = q.Enqueue(&m)
	}

	return nil
}

func (q *ChannelQueue) Ack(m *Message) error {
	log.Printf("INFO ChannelQueue Ack")
	return nil
}

func (q *ChannelQueue) Receive(ch chan Message, done chan bool) error {
	for {
		select {
		case m := <-q.Input:
			ch <- m
		case <-done:
			log.Printf("INFO ChannelQueue done")
			break
		}
	}

	return nil
}
