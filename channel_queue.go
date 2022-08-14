package queue

// ChannelQueue is a queue that manages messages over a channel.
//
// Ensure the channel size is sufficient.
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
		// ignoring errors for this implementation as Enqueue cannot return an error.
		_ = q.Enqueue(&m)
	}

	return nil
}

func (q *ChannelQueue) Ack(m *Message) error {
	// there is no ack for this implementation
	return nil
}

func (q *ChannelQueue) Receive(ch chan Message, done chan bool) error {
	for {
		select {
		case m := <-q.Input:
			ch <- m
		case <-done:
			break
		}
	}

	return nil
}
