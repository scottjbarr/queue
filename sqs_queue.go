package queue

import (
	"context"
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

	// SQSTypeString is used to state the data type of an SQS Message attribute.
	SQSTypeString = "String"
)

type SQSQueue struct {
	URL  string
	FIFO bool
}

func NewSQSQueue(url string) SQSQueue {
	return SQSQueue{
		URL: url,
	}
}

func NewSQSFIFOQueue(url string) SQSQueue {
	return SQSQueue{
		URL:  url,
		FIFO: true,
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

func (s SQSQueue) Enqueue(ctx context.Context, m *Message) error {
	return s.Send(*m)
}

func (s SQSQueue) Send(m Message) error {
	smi := s.buildSendMessageInput(&m)

	_, err := s.client().SendMessage(smi)

	return err
}

func (s SQSQueue) SendBatch(messages []Message) error {
	entries := []*sqs.SendMessageBatchRequestEntry{}

	for i := 0; i < len(messages); i++ {
		m := messages[i]

		entry := sqs.SendMessageBatchRequestEntry{
			Id:          &m.ID,
			MessageBody: &m.Payload,
		}

		if s.FIFO {
			if len(m.ID) > 0 {
				entry.MessageDeduplicationId = &m.ID
			}

			// FIFO queues need the MessageGroupId, populate it if specified.
			if len(m.GroupID) > 0 {
				entry.MessageGroupId = &m.GroupID
			}
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

// buildSendMessageInput builds a message that can queued on an SQS from a Message.
func (q *SQSQueue) buildSendMessageInput(m *Message) *sqs.SendMessageInput {
	smi := &sqs.SendMessageInput{
		MessageBody: &m.Payload,
		QueueUrl:    &q.URL,
	}

	// Check the optional Trace value
	if len(m.Trace) > 0 {
		q.addAttribute(smi, SQSQueueAttributeTrace, m.Trace)
	}

	if q.FIFO {
		if len(m.ID) > 0 {
			smi.MessageDeduplicationId = &m.ID
		}

		// FIFO queues need the MessageGroupId, populate it if specified.
		if len(m.GroupID) > 0 {
			smi.MessageGroupId = &m.GroupID
		}
	}

	return smi
}

// addAttribute safely adds a single string attribute to the SQS Message attributes.
func (q *SQSQueue) addAttribute(smi *sqs.SendMessageInput, name, value string) {
	if smi.MessageAttributes == nil {
		smi.MessageAttributes = map[string]*sqs.MessageAttributeValue{}
	}

	v := sqs.MessageAttributeValue{
		DataType:    aws.String(SQSTypeString),
		StringValue: aws.String(value),
	}

	smi.MessageAttributes[name] = &v
}
