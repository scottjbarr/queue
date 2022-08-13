package queue

import (
	"log"
)

type HandlerFunc func(Message) error

type Worker struct {
	Name         string
	Queue        ReceivingAcker
	ChannelWrite chan Message
	HandleFunc   HandlerFunc
	done         chan bool
}

func NewWorker(name string, r ReceivingAcker, h HandlerFunc) *Worker {
	return &Worker{
		Name:         name,
		Queue:        r,
		ChannelWrite: make(chan Message),
		HandleFunc:   h,
		done:         make(chan bool, 2),
	}
}

func (w *Worker) Start() error {
	log.Printf("INFO %s starting", w.Name)
	// channel on which we can tell the receiver to stop running
	receiverDone := make(chan bool)

	go func() {
		for {
			select {
			case msg := <-w.ChannelWrite:
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
				receiverDone <- true

				return
			}
		}
	}()

	return w.Queue.Receive(w.ChannelWrite, receiverDone)
}

func (w *Worker) Stop() {
	w.done <- true
}
