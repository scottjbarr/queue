package queue

type Queue interface {
	Receive(chan<- Message) error
	Send(Message) error
	SendBatch([]Message) error
	Ack(Message) error
}

type Message struct {
	ID      string
	Handle  string
	Payload string
	Trace string
}
