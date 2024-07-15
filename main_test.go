package main

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type mockKafkaClient struct {
	onProduce func(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
}

func (m *mockKafkaClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	m.onProduce(ctx, r, promise)
}

func TestRun(t *testing.T) {
	t.Run("the event has a key and data", func(t *testing.T) {
		expected := EventType{
			Key:  "test",
			Data: []byte("test"),
		}
		var wasCalled bool
		mockKafkaClient := &mockKafkaClient{
			onProduce: func(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
				// Check if the key and data are the same as the expected values.
				if string(r.Key) != expected.Key {
					t.Errorf("expected key %s, got %s", expected.Key, string(r.Key))
				}
				if string(r.Value) != string(expected.Data) {
					t.Errorf("expected data %s, got %s", string(expected.Data), string(r.Value))
				}
				wasCalled = true
			},
		}
		// Create the concrete KafkaPublisher type, but use the mockKafkaClient,
		// instead of the kgo.Client.
		kp := NewKafkaPublisher(mockKafkaClient)
		kp.Publish(context.Background(), expected)

		if !wasCalled {
			t.Error("expected mockKafkaClient.Produce to be called")
		}
	})
}
