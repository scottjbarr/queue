package queue

type NoopQueue struct {
}

func NewNoopQueue() *NoopQueue {
	return &NoopQueue{}
}

func (q *NoopQueue) Enqueue(m *Message) error {
	return nil
}

func (q *NoopQueue) BatchEnqueue(msg []Message) error {
	return nil
}

func (q *NoopQueue) Ack(m *Message) error {
	return nil
}

func (q *NoopQueue) Receive(ch chan Message) error {
	return nil
}
