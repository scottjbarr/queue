package queue

import (
	"github.com/gomodule/redigo/redis"
	redigo "github.com/gomodule/redigo/redis"
)

// RedisQueue works with a Redis instance to satisfy most of the Queue unterface, except
type RedisPoolQueue struct {
	Topic string
	pool  *redis.Pool
}

func NewRedisPoolQueue(pool *redigo.Pool, topic string) *RedisPoolQueue {
	return &RedisPoolQueue{
		pool:  pool,
		Topic: topic,
	}
}

// Receive reads messages from the topic and pushes them to the channel.
func (q *RedisPoolQueue) Receive(ch chan Message) error {
	conn := q.pool.Get()
	defer conn.Close()

	for {
		resp, err := redis.ByteSlices(conn.Do(redisCmdBlpop, q.Topic, 0))
		if err != nil {
			conn.Close()
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

func (q *RedisPoolQueue) SendBatch(messages []Message) error {
	for _, m := range messages {
		if err := q.Send(m); err != nil {
			return err
		}
	}

	return nil
}

func (q *RedisPoolQueue) Enqueue(m *Message) error {
	return q.Send(*m)
}

func (q *RedisPoolQueue) BatchEnqueue(msgs []Message) error {
	for _, m := range msgs {
		if err := q.Enqueue(&m); err != nil {
			return err
		}
	}

	return nil
}

// Enqueue adds a message to the queue.
func (q *RedisPoolQueue) Send(m Message) error {
	b := []byte(m.Payload)

	conn := q.pool.Get()
	defer conn.Close()

	if _, err := conn.Do(redisCmdRpush, q.Topic, b); err != nil {
		return err
	}

	conn.Flush()

	return nil
}

// Ack is not support by Redis, so provide a NOOP implementation.
func (q *RedisPoolQueue) Ack(_ *Message) error {
	return nil
}
