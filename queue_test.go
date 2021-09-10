package queue

import (
	"reflect"
	"testing"
)

func TestNew_Redis(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "redis",
			url:  "redis://localhost:5555/foo",
			want: "foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.url)
			if err != nil {
				t.Fatal(err)
			}

			q, ok := got.(*RedisQueue)
			if !ok {
				t.Fatalf("got %#+v", got)
			}

			// if !reflect.DeepEqual(got, tt.want) {
			// 	t.Fatalf("got\n%#+v\nwant\n%#+v", got, tt.want)
			// }

			if q.Topic != tt.want {
				t.Fatalf("got %v want %v", q.Topic, tt.want)
			}
		})
	}
}

func TestNew_SQS(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want *SQSQueue
	}{
		{
			name: "sqs",
			url:  "sqs://example.com/queuename",
			want: &SQSQueue{
				URL:         "sqs://example.com/queuename",
				MaxMessages: 10,
				WaitTime:    20,
			},
		},
		{
			name: "sqs fifo",
			url:  "sqs://example.com/queuename.fifo",
			want: &SQSQueue{
				URL:         "sqs://example.com/queuename.fifo",
				FIFO:        true,
				MaxMessages: 10,
				WaitTime:    20,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.url)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got\n%#+v\nwant\n%#+v", got, tt.want)
			}
		})
	}
}
