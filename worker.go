package queue

import (
	"log"
	"time"
)

type Runner interface {
	Start() error
	Stop()
}

type HandlerFunc func(Message) error
type BackoffFunc func(count int)

func DefaultWorkerBackoffFunc(count int) {
	time.Sleep(time.Second * time.Duration(count*30))
}

type Worker struct {
	Name         string
	Queue        ReceivingAcker
	ChannelWrite chan Message
	HandleFunc   HandlerFunc
	BackoffFunc  BackoffFunc
	done         chan bool
}

func NewWorker(name string, r ReceivingAcker, h HandlerFunc) *Worker {
	return &Worker{
		Name:         name,
		Queue:        r,
		ChannelWrite: make(chan Message),
		HandleFunc:   h,
		BackoffFunc:  DefaultWorkerBackoffFunc,
		done:         make(chan bool),
	}
}

func (w *Worker) Start() error {
	log.Printf("INFO %s starting", w.Name)

	failures := int(0)

	// receive messages from the queue
	go func() {
		for {
			if err := w.Queue.Receive(w.ChannelWrite); err != nil {
				log.Printf("ERROR %s err = %v", w.Name, err)

				// increment consecutive failures
				failures++

				// backoff
				w.BackoffFunc(failures)

				continue
			}

			// reset failure count
			failures = 0
		}
	}()

	// read from the message and done channels
	for {
		select {
		case msg := <-w.ChannelWrite:
			// The queue wrote a message to the channel.
			//
			// Pass the message to the handler.
			if err := w.HandleFunc(msg); err != nil {
				log.Printf("ERROR %s processing message %+v : err = %v", w.Name, msg.Payload, err)
				continue
			}

			if err := w.Queue.Ack(&msg); err != nil {
				log.Printf("ERROR %s could not ack message %v", w.Name, msg.Handle)
			}

		case <-w.done:
			// we've received the "done" message. Stop the Receiver.
			log.Printf("INFO %s stopping", w.Name)

			break
		}
	}

	return nil
}

func (w *Worker) Stop() {
	w.done <- true
}
