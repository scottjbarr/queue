package queue

import (
	"github.com/gomodule/redigo/redis"
)

const (
	// redisCmdBlpop is the BLPOP command
	redisCmdBlpop = "BLPOP"

	// redisCmdRpush is the RPUSH command
	redisCmdRpush = "RPUSH"
)

// RedisQueue works with a Redis instance to satisfy most of the Queue unterface, except
type RedisQueue struct {
	Topic string
	conn  redis.Conn
}

func NewRedisQueue(conn redis.Conn, topic string) *RedisQueue {
	return &RedisQueue{
		conn:  conn,
		Topic: topic,
	}
}

// Receive reads messages from the topic and pushes them to the channel.
func (q *RedisQueue) Receive(ch chan<- Message) error {
	for {
		resp, err := redis.ByteSlices(q.conn.Do(redisCmdBlpop, q.Topic, 0))
		if err != nil {
			return err
		}

		// data a in the response is...
		//
		// [0] topic we were listening on.
		// [1] data
		msg := Message{
			Payload: string(resp[1]),
		}

		// The channel will block if it fills.
		//
		// It is recommended to use a buffered channel.
		ch <- msg
	}

	return nil
}

func (q *RedisQueue) SendBatch(messages []Message) error {
	for _, m := range messages {

		if err := q.Send(m); err != nil {
			return err
		}
	}

	return nil
}

// Enqueue adds a message to the queue.
func (q *RedisQueue) Send(m Message) error {
	b := []byte(m.Payload)

	if _, err := q.conn.Do(redisCmdRpush, q.Topic, b); err != nil {
		return err
	}

	q.conn.Flush()

	return nil
}

// Ack is not support by Redis, so provide a NOOP implementation.
func (q *RedisQueue) Ack(_ Message) error {
	return nil
}
