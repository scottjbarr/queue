package queue

import (
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
)

const (
	// SQSQueueAttributeTrace is the optional SQS Message attribute that holds a trace ID.
	SQSQueueAttributeTrace = "trace"
)

type SQSQueue struct {
	URL string
}

func NewSQSQueue(url string) SQSQueue {
	return SQSQueue{
		URL: url,
	}
}

func (s SQSQueue) Receive(messages chan<- Message) error {
	maxMessages := int64(10)
	waitTime := int64(20)

	rmin := &sqs.ReceiveMessageInput{
		QueueUrl:            &s.URL,
		MaxNumberOfMessages: &maxMessages,
		WaitTimeSeconds:     &waitTime,
	}

	// loop over all queue messages.
	for {
		resp, err := s.client().ReceiveMessage(rmin)

		if err != nil {
			return err
		}

		if len(resp.Messages) == 0 {
			log.Printf("No messages")
			return nil
		}

		log.Printf("received %v messages...", len(resp.Messages))

		for _, m := range resp.Messages {
			message := Message{}
			message.ID = *m.MessageId
			message.Handle = *m.ReceiptHandle
			message.Payload = *m.Body

			messages <- message
		}
	}

	return nil
}

func (s SQSQueue) Send(m Message) error {
	return fmt.Errorf("Send not implemented")
}

func (s SQSQueue) SendBatch(messages []Message) error {
	entries := []*sqs.SendMessageBatchRequestEntry{}

	for i := 0; i < len(messages); i++ {
		m := messages[i]

		entry := sqs.SendMessageBatchRequestEntry{
			Id:          &m.ID,
			MessageBody: &m.Payload,
		}
		entries = append(entries, &entry)
	}

	smbi := sqs.SendMessageBatchInput{
		QueueUrl: &s.URL,
		Entries:  entries,
	}

	req, output := s.client().SendMessageBatchRequest(&smbi)

	if err := req.Send(); err != nil {
		return err
	}

	if len(output.Successful) != len(messages) {
		return fmt.Errorf("Messages fail count : %v", len(output.Failed))
	}

	return nil
}

func (s SQSQueue) Ack(m Message) error {
	dmi := &sqs.DeleteMessageInput{
		QueueUrl:      &s.URL,
		ReceiptHandle: &m.Handle,
	}

	_, err := s.client().DeleteMessage(dmi)

	return err
}

func (s SQSQueue) client() *sqs.SQS {
	awsConfig := aws.Config{}

	return sqs.New(session.New(), &awsConfig)
}

// BuildMessageFromSQSEventsMessage converts an events.SQSMessage into a Message.
func BuildMessageFromSQSMessage(r *events.SQSMessage) *Message {
	m := Message{
		ID:      r.MessageId,
		Handle:  r.ReceiptHandle,
		Payload: r.Body,
	}

	if r.MessageAttributes == nil {
		// create a new trace
		m.Trace = uuid.New().String()

		return &m
	}

	// get the "trace" if we can
	trace, ok := r.MessageAttributes[SQSQueueAttributeTrace]
	if !ok {
		return &m
	}

	m.Trace = *trace.StringValue

	return &m
}
